"""Graph node factories for the V1.2 tool-using agent (PDF §14-§22, §38-§39).

Every node opens its own DB session (MVP pattern, nothing shared across
awaits) and returns a state *update* dict. Nodes never raise into the graph:
tool failures become observations (§37), LLM failures become an AGENT_LLM_ERROR
error dict that routing short-circuits to build_reply.

Deps (registry / llm / policy / limits) arrive via the runner + per-run
ToolPolicy, closed over by build_nodes() - the graph itself stays stateless.
"""

import logging
import time
from typing import Any

from sqlalchemy import select

from app.agent.core.context import AgentContext
from app.agent.core.prompts import decide_messages
from app.agent.llm.errors import LLMError
from app.agent.llm.schemas import AgentDecision
from app.agent.tools.base import ToolErrorCodes
from app.db.database import SessionLocal
from app.db.models import Device

logger = logging.getLogger(__name__)

# Agent-run failure codes (PDF §126). Tool-level codes live in ToolErrorCodes.
AGENT_LLM_ERROR = "AGENT_LLM_ERROR"
AGENT_MAX_TOOL_CALLS = "AGENT_MAX_TOOL_CALLS"
AGENT_TIMEOUT = "AGENT_TIMEOUT"

_FACT_LIMIT = 400


def _agent_error(code: str, message: str = "") -> dict:
    return {"error_code": code, "message": message}


def build_nodes(runner: Any, policy: Any) -> dict[str, Any]:
    """Close over the runner (config/llm/registry) and the per-run policy."""

    async def understand(state: dict) -> dict:
        """PDF §16: base understanding. V1.2 keeps this deterministic - the
        request IS the goal; semantics are decided inside the tool loop."""
        return {"current_goal": (state.get("user_request") or "").strip()}

    async def load_context(state: dict) -> dict:
        """PDF §17: cheap deterministic context - pointer to the device the
        user named, if any. Read-only; never consumes tool budget. Failures
        degrade to an empty context instead of killing the run."""
        ctx = AgentContext.from_dict(state.get("context"))
        ctx.user_id = state.get("sender_id") or ctx.user_id
        ctx.channel = state.get("channel", ctx.channel)
        ctx.conversation_id = state.get("conversation_id", ctx.conversation_id)
        try:
            text = (state.get("user_request") or "").lower()
            with SessionLocal() as db:
                names = [n for (n,) in db.execute(select(Device.name).where(Device.revoked_at.is_(None))).all()]
            best = max((n for n in names if n and n.lower() in text), key=len, default=None)
            if best:
                with SessionLocal() as db:
                    device = db.scalars(select(Device).where(Device.name == best)).first()
                if device is not None:
                    ctx.current_device_id = device.device_id
                    ctx.current_device_name = device.name
        except Exception:  # noqa: BLE001 - context is best-effort (§37 spirit)
            logger.warning("load_context could not resolve a device pointer", exc_info=True)
        return {"context": ctx.to_dict()}

    async def plan(state: dict) -> dict:
        """PDF §18-§19: the planner proposes, never executes. V1.2 ships a
        static scaffold; the LLM-generated plan is a later upgrade."""
        return {
            "plan": [
                "收集与请求相关的事实",
                "需要时通过工具执行对应操作",
                "汇总事实给出最终答复",
            ]
        }

    async def llm_decide(state: dict) -> dict:
        """PDF §20-§21: the decision node. Output is always an AgentDecision
        via the function-calling channel; LLMError after bounded retries
        short-circuits the run (§127-§128)."""
        messages = decide_messages(state, runner.registry.enabled_descriptions())
        try:
            decision: AgentDecision = await runner.llm.decide(messages)
        except LLMError as exc:
            logger.warning("llm_decide failed: %s", exc)
            return {"error": _agent_error(AGENT_LLM_ERROR, str(exc))}
        return {
            "decision": decision.action,
            "tool_name": decision.tool_name,
            "tool_args": dict(decision.tool_args or {}),
            "final_answer": decision.answer,
            "reason": decision.reason,
        }

    async def execute_tool(state: dict) -> dict:
        """PDF §22/§85: the ONLY path from an LLM decision to a tool. The
        policy chain (exists/args/permission/confirmation/limits) never
        raises - rejections come back as failed ToolResults (§37)."""
        with SessionLocal() as db:
            result = await policy.execute(
                db,
                tool_name=state.get("tool_name") or "",
                args=state.get("tool_args") or {},
                confirmed=state.get("confirmed", False),
            )
        update = {
            "tool_result": result.model_dump(),
            "tool_call_count": state.get("tool_call_count", 0) + 1,
        }
        # PDF §57: the user must answer before a confirmation-gated tool runs;
        # park the pending call in the context so resume can re-execute it.
        if result.error_code == ToolErrorCodes.CONFIRMATION_REQUIRED:
            ctx = AgentContext.from_dict(state.get("context"))
            ctx.pending_confirmation = {
                "tool_name": state.get("tool_name"),
                "tool_args": state.get("tool_args") or {},
            }
            update["context"] = ctx.to_dict()
        return update

    async def observe(state: dict) -> dict:
        """PDF §38: turn the raw ToolResult into a compact fact the LLM can
        reason on; the (truncated) raw result rides along for reference."""
        result = state.get("tool_result") or {}
        name = result.get("tool_name") or state.get("tool_name") or "unknown_tool"
        if result.get("success"):
            fact = f"工具 {name} 成功：{_compact(result.get('data'))}"
        else:
            code = result.get("error_code") or "UNKNOWN"
            fact = f"工具 {name} 失败 {code}: {result.get('message') or ''}"
        observations = [*(state.get("observations") or []), {"source": name, "fact": fact}]
        return {"observations": observations}

    async def evaluate(state: dict) -> dict:
        """PDF §39/§41-§42: programmatic loop guards only - semantic goal
        completion is the LLM's call (its next decision may be finish).
        §41 trips on count EXCEEDING the limit: the Nth call runs, then the
        policy rejects the (N+1)th as an observation the LLM can still
        finish on; this node is the backstop, not the first line."""
        if state.get("tool_call_count", 0) > runner.max_tool_calls:
            return {
                "decision": "finish",
                "error": _agent_error(
                    AGENT_MAX_TOOL_CALLS,
                    f"reached the tool call limit ({runner.max_tool_calls})",
                ),
            }
        elapsed = time.monotonic() - state.get("started_at", time.monotonic())
        if elapsed >= runner.max_runtime:
            return {
                "decision": "finish",
                "error": _agent_error(AGENT_TIMEOUT, f"run exceeded {runner.max_runtime}s"),
            }
        return {"decision": "continue"}

    async def ask_user(state: dict) -> dict:
        """PDF §46-§47: asking is a first-class outcome, not an error. Two
        flavors: an LLM ask_user decision (final_answer) and the PDF §57
        confirmation parking after a CONFIRMATION_REQUIRED rejection."""
        if not state.get("final_answer"):
            pending = AgentContext.from_dict(state.get("context")).pending_confirmation
            if pending:
                args = _compact(pending.get("tool_args") or {})
                return {
                    "reply": (
                        f"即将执行操作 {pending.get('tool_name')}（参数：{args}），"
                        "是否确认？回复“是”确认，其他回复视为取消。"
                    ),
                    "paused": True,
                }
        return {"reply": state.get("final_answer") or "请补充更多信息。", "paused": True}

    async def build_reply(state: dict) -> dict:
        """PDF §138-§139: facts for the user, never internals."""
        error = state.get("error")
        if error:
            code = error.get("error_code")
            if code == AGENT_LLM_ERROR:
                return {"reply": "抱歉，智能助手暂时无法处理该请求，请稍后再试。"}
            if code == AGENT_MAX_TOOL_CALLS:
                return {
                    "reply": "这个问题需要的处理步骤较多，已达到单次处理上限。"
                    "请把需求拆小一点再试，或直接在控制台查看设备与任务状态。"
                }
            if code == AGENT_TIMEOUT:
                return {"reply": "处理超时，请稍后再试或把需求拆小一点。"}
            return {"reply": f"处理未完成：{error.get('message') or code or '未知错误'}"}

        answer = state.get("final_answer")
        if answer:
            return {"reply": answer}
        facts = [o.get("fact", "") for o in (state.get("observations") or [])[-3:]]
        facts = [f for f in facts if f]
        if facts:
            return {"reply": "已完成查询：" + "；".join(facts)}
        return {"reply": "没有获取到足够的信息，请换个说法再试。"}

    return {
        "understand": understand,
        "load_context": load_context,
        "plan": plan,
        "llm_decide": llm_decide,
        "execute_tool": execute_tool,
        "observe": observe,
        "evaluate": evaluate,
        "ask_user": ask_user,
        "build_reply": build_reply,
    }


def _compact(value: Any, limit: int = _FACT_LIMIT) -> str:
    import json

    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001
        text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"
