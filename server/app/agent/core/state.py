"""V1.2 AgentState: the Agent's in-graph reasoning scratchpad (PDF §11-§12).

Strictly separated from business truth (PDF §10):
- AgentState        = temporary reasoning context (what the agent knows/thinks)
- Task/Attempt/Event = persisted business facts, only readable via Tools

Everything here must stay JSON-serializable (dict/list/str/int/float/bool)
so the whole state can be persisted for resume without a LangGraph
checkpointer (PDF §48/§134).
"""

from typing import TypedDict


class AgentState(TypedDict, total=False):
    # --- run identity ---
    run_id: str
    user_request: str          # original request, or resume reply + original
    channel: str               # dingtalk | api
    conversation_id: str
    sender_id: str
    resumed: bool              # True when continuing a WAITING_USER run
    user_reply: str            # the user's follow-up message on resume

    # --- context & plan (PDF §9/§18) ---
    context: dict              # AgentContext.to_dict()
    plan: list[str]            # ordered steps the planner proposed
    current_goal: str

    # --- tool loop (PDF §20-§22) ---
    tool_name: str             # tool about to be / just executed
    tool_args: dict
    tool_result: dict          # ToolResult.model_dump() (+ tool_call_id)
    observations: list[dict]   # [{"source": tool_name, "fact": str}, ...]
    decision: str              # llm action | evaluate outcome

    # --- limits (PDF §41/§127) ---
    tool_call_count: int
    llm_retry_count: int
    started_at: float          # monotonic seconds; runtime guard
    confirmed: bool            # resume-after-confirmation: skip llm_decide (§57)

    # --- errors & output (PDF §125) ---
    error: dict | None         # {"error_code": ..., "message": ...}
    final_answer: str
    reply: str                 # user-facing reply (fact-based, PDF §138)
    paused: bool               # True on ask_user: run parked in WAITING_USER (§46)
