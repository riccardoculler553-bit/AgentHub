"""AgentRunner: per-run lifecycle for the V1.2 graph (PDF §93).

Creates the state, the per-run ToolPolicy (call ledger) and the compiled
graph, then drives one run to completion. Persistence of AgentRun rows and
resume/cancel arrive with the Phase 6/8 work; run() is deliberately the only
public method today.
"""

import logging
import time
import uuid
from typing import Any

from app.agent.core.context import AgentContext
from app.agent.core.policies import ToolPolicy
from app.agent.graph.graph import build_graph
from app.core.config import settings

logger = logging.getLogger(__name__)

# Affirmative answers for the PDF §57 confirmation flow. Exact-match only and
# deliberately narrow: anything ambiguous counts as NOT confirmed (safe
# default - a declined confirmation must never widen into an execution).
_AFFIRMATIVE = {
    "是", "是的", "是的。", "对", "行", "好", "好的", "可以", "确认", "确定",
    "同意", "继续", "执行", "执行吧", "跑吧", "yes", "y", "ok", "okay", "sure",
}


def _is_affirmative(text: str) -> bool:
    normalized = (text or "").strip().lower().strip("。！？.!?\u3002\uff01\uff1f ")
    return normalized in _AFFIRMATIVE


class AgentRunner:
    def __init__(
        self,
        *,
        registry: Any,
        llm: Any,
        hub: Any = None,
        max_tool_calls: int | None = None,
        max_runtime: float | None = None,
    ) -> None:
        self.registry = registry
        self.llm = llm
        self.hub = hub
        self.max_tool_calls = (
            settings.agent_max_tool_calls if max_tool_calls is None else max_tool_calls
        )
        self.max_runtime = settings.agent_max_runtime if max_runtime is None else max_runtime

    async def run(
        self,
        user_request: str,
        *,
        channel: str = "api",
        user_id: str = "",
        conversation_id: str = "",
        sender_id: str = "",
        run_id: str | None = None,
    ) -> dict:
        """Drive one user request through the graph; returns the final state.

        `run_id` lets a caller that pre-created the AgentRun row (AgentService)
        keep the row, the audit trail and the state on one identity."""
        run_id = run_id or uuid.uuid4().hex
        # run_id enables the agent_tool_calls audit trail (PDF §122)
        policy = ToolPolicy(self.registry, max_total_calls=self.max_tool_calls, run_id=run_id)
        graph = build_graph(self, policy)
        state: dict = {
            "run_id": run_id,
            "user_request": user_request,
            "channel": channel,
            "conversation_id": conversation_id,
            "sender_id": sender_id,
            "resumed": False,
            "user_reply": "",
            "context": AgentContext(
                user_id=user_id or sender_id,
                conversation_id=conversation_id,
                channel=channel,
            ).to_dict(),
            "plan": [],
            "current_goal": "",
            "tool_name": None,
            "tool_args": {},
            "tool_result": None,
            "observations": [],
            "decision": "",
            "tool_call_count": 0,
            "llm_retry_count": 0,
            "started_at": time.monotonic(),
            "error": None,
            "final_answer": None,
            "reply": "",
            "paused": False,
        }
        final = await graph.ainvoke(state)
        logger.info(
            "agent run %s finished: decision=%s tool_calls=%s error=%s paused=%s",
            final.get("run_id"),
            final.get("decision"),
            final.get("tool_call_count"),
            (final.get("error") or {}).get("error_code"),
            final.get("paused", False),
        )
        return final

    async def resume(self, previous_state: dict, new_user_message: str) -> dict:
        """Continue a WAITING_USER run (PDF §48/§133).

        Restores context + plan + observations + tool-call budget from the
        persisted state, folds in the user's answer, and re-enters the graph
        from the top (understand rebuilds the goal with the supplement).
        The runtime budget restarts; the tool-call budget does not (§41 is
        per AgentRun, and a resumed run is the same run).

        PDF §57: when the run was parked on a tool confirmation, an
        affirmative answer re-executes the parked call with confirmed=True
        straight through routing - the LLM is never consulted again, so a
        prompt-injected model cannot alter the confirmed arguments. Any
        non-affirmative reply cancels the pending call and flows back into
        the normal loop as an observation.
        """
        state = dict(previous_state)
        ctx = AgentContext.from_dict(state.get("context"))
        supplement = (new_user_message or "").strip()
        pending = ctx.pending_confirmation
        state.update(
            {
                "resumed": True,
                "user_reply": supplement,
                "user_request": f"{state.get('user_request', '')}\n（用户补充：{supplement}）",
                "decision": "",
                "tool_name": None,
                "tool_args": {},
                "tool_result": None,
                "final_answer": None,
                "error": None,
                "reply": "",
                "paused": False,
                "confirmed": False,
                "started_at": time.monotonic(),
            }
        )
        if pending:
            ctx.pending_confirmation = None
            if _is_affirmative(supplement):
                state.update(
                    {
                        "confirmed": True,
                        "decision": "tool_call",
                        "tool_name": pending.get("tool_name"),
                        "tool_args": dict(pending.get("tool_args") or {}),
                    }
                )
            else:
                state["observations"] = [
                    *(state.get("observations") or []),
                    {"source": "user", "fact": f"用户未确认执行，已取消该操作（回复：{supplement}）"},
                ]
        state["context"] = ctx.to_dict()
        policy = ToolPolicy(
            self.registry,
            max_total_calls=self.max_tool_calls,
            used_total=int(state.get("tool_call_count") or 0),
            run_id=state.get("run_id"),
        )
        graph = build_graph(self, policy)
        final = await graph.ainvoke(state)
        logger.info(
            "agent run %s resumed and finished: decision=%s tool_calls=%s error=%s paused=%s",
            final.get("run_id"),
            final.get("decision"),
            final.get("tool_call_count"),
            (final.get("error") or {}).get("error_code"),
            final.get("paused", False),
        )
        return final
