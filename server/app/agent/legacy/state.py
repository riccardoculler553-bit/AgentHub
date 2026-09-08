"""AgentState: LangGraph reasoning context (NOT the task state).

DB task state is the authoritative business fact; this dict only carries the
in-flight reasoning between graph nodes (PDF §131-133).
"""

from typing import TypedDict


class AgentState(TypedDict, total=False):
    user_request: str
    # snapshot of the world used for planning
    context: dict
    # structured plan: {"device_hint": str|None, "steps": [{"command","params"}], "rationale": str}
    execution_plan: dict | None
    target_device_id: str | None
    task_id: str | None
    task_status: str | None
    task_result: dict | None
    error: dict | None
    decision: str | None  # finish | retry | replan | human
    message: str
    retry_count: int
    replan_count: int
