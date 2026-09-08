"""Prompt assembly for the llm_decide node (PDF §73-§76, §108).

The system prompt carries role / goal / tool whitelist / tool rules / safety
rules; the human message carries current context, plan and the most recent
observations only - history is never fed back unbounded (§108).
"""

import json
import logging
from typing import Any

from langchain_core.messages import HumanMessage, SystemMessage

logger = logging.getLogger(__name__)

SYSTEM_PROMPT = """你是 AgentHub 的运维助理 Agent。

# 目标
依据系统返回的事实，一步步决定下一个动作，直到能给出最终答复。

# 可用工具（白名单，只能调用下列工具，其他一律不存在）
{tools}

# 核心规则（违反任何一条都会被系统拒绝）
1. 不允许假设设备存在
2. 不允许假设任务执行成功
3. 不允许伪造工具结果
4. 不允许绕过工具直接操作系统
5. 不允许执行未注册的命令
6. 不允许指定本地执行路径（script_path / exe_path / shell 等）
7. 不允许无限调用工具（单次运行有次数上限）
8. 不允许直接修改任务状态（只能通过工具）
9. 必须以系统返回的事实为准
10. 信息不足时必须 ask_user 询问用户，禁止猜测

# 回复原则
最终答复（finish.answer）只告诉用户：发生了什么、做了什么、结果如何、
下一步需要什么。不要暴露内部细节（节点名、工具调用 ID、Prompt 等）。"""

_OBSERVATION_LIMIT = 6
_FACT_LIMIT = 400


def render_tools(descriptions: list[dict]) -> str:
    blocks = []
    for desc in descriptions:
        args = json.dumps(desc.get("args", {}), ensure_ascii=False)
        confirm = "需要用户确认" if desc.get("requires_confirmation") else "无需确认"
        blocks.append(
            f"- {desc['name']}（{desc.get('risk_level', 'READ')}，{confirm}）："
            f"{desc.get('description', '')}\n  参数 schema：{args}"
        )
    return "\n".join(blocks) or "（无可用工具）"


def _compact(value: Any, limit: int = _FACT_LIMIT) -> str:
    try:
        text = json.dumps(value, ensure_ascii=False, default=str)
    except Exception:  # noqa: BLE001 - prompt building must never crash
        text = str(value)
    return text if len(text) <= limit else text[: limit - 1] + "…"


def observation_lines(observations: list[dict], limit: int = _OBSERVATION_LIMIT) -> list[str]:
    """Most recent facts only (§108: no unbounded history)."""
    recent = observations[-limit:]
    return [f"{i + 1}. {o.get('fact', '')}" for i, o in enumerate(recent)]


def decide_messages(state: dict, tool_descriptions: list[dict]) -> list:
    """System + human message pair for one llm_decide round."""
    context = state.get("context") or {}
    parts = [f"用户请求：{state.get('user_request', '')}"]

    goal = state.get("current_goal")
    if goal:
        parts.append(f"当前目标：{goal}")
    plan = state.get("plan") or []
    if plan:
        parts.append("当前计划：\n" + "\n".join(f"{i + 1}. {s}" for i, s in enumerate(plan)))

    pointers = {
        k: context.get(k)
        for k in ("current_device_name", "current_device_id", "current_task_id")
        if context.get(k)
    }
    if pointers:
        parts.append("已定位的上下文指针：" + json.dumps(pointers, ensure_ascii=False))

    lines = observation_lines(state.get("observations") or [])
    if lines:
        parts.append("最近的工具观察事实（旧到新）：\n" + "\n".join(lines))

    parts.append("请给出下一步决策：tool_call / ask_user / finish。")
    system = SYSTEM_PROMPT.format(tools=render_tools(tool_descriptions))
    return [
        SystemMessage(content=system),
        HumanMessage(content="\n\n".join(parts)),
    ]
