"""Per-tool-call run context (Phase 3/4).

The policy chain is the only place that knows the AgentRun id; handlers keep
the (db, args) signature. A ContextVar bridges the two without threading a
new parameter through every handler: ToolPolicy.execute sets it around the
handler call, tools that create long-running work (run_capability) read it to
link Task -> AgentRun so the terminal notification can find the conversation.
"""

from contextvars import ContextVar

current_run_id: ContextVar[str] = ContextVar("agent_current_run_id", default="")
