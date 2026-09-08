"""Routing policy: pure state->node-name functions (PDF §83-§84, §106).

The LLM chooses an action; routing decides which node realizes it. Edges
(§106):

    plan         -> llm_decide | execute_tool   (confirmed resume, §57)
    llm_decide   -> execute_tool | ask_user | build_reply
    execute_tool -> observe | ask_user          (confirmation parking, §57)
    evaluate     -> llm_decide | build_reply    (finish / guard tripped)
"""

from app.agent.tools.base import ToolErrorCodes


def route_after_plan(state: dict) -> str:
    """A resumed run with an affirmed confirmation goes straight to the tool
    - deliberately NOT through llm_decide, so no LLM round can flip the
    user's yes into different arguments (PDF §57/§121)."""
    if state.get("confirmed") and state.get("tool_name"):
        return "execute_tool"
    return "llm_decide"


def route_after_decide(state: dict) -> str:
    """Where an llm_decide outcome goes. Errors (LLM layer) short-circuit."""
    if state.get("error"):
        return "build_reply"
    action = state.get("decision")
    if action == "tool_call":
        return "execute_tool"
    if action == "ask_user":
        return "ask_user"
    return "build_reply"  # finish (or anything unexpected) ends the loop


def route_after_execute(state: dict) -> str:
    """A confirmation-gated tool parks the run on ask_user (§57) instead of
    observing a rejection the LLM might talk its way around."""
    result = state.get("tool_result") or {}
    if not result.get("success") and result.get("error_code") == ToolErrorCodes.CONFIRMATION_REQUIRED:
        return "ask_user"
    return "observe"


def route_after_evaluate(state: dict) -> str:
    """Continue the loop, or end it when a guard forced a finish."""
    if state.get("error") or state.get("decision") == "finish":
        return "build_reply"
    return "llm_decide"
