"""WorkflowContext: the run's data plane (V1.3 §34/§94-§97).

Pure JSON - never a code execution environment (§40). Persisted on
workflow_runs.context_json after every step completion so a server restart
never loses step results (§88/§94).
"""

from typing import Any


def build_context(variables: dict | None) -> dict:
    return {"variables": dict(variables or {}), "steps": {}}


def record_step_result(
    context: dict, step_name: str, task_id: str, status: str, result: dict | None
) -> dict:
    """Persist one step's outcome into context.steps (§35/§97)."""
    steps = context.setdefault("steps", {})
    steps[step_name] = {
        "status": status,
        "task_id": task_id,
        "result": dict(result or {}),
    }
    return context


def append_artifacts(context: dict, step_name: str, artifacts: list[dict]) -> dict:
    """V1.4 §27: run-level artifact index ({"artifact_id", "name", "type",
    "size", "step"}) so later steps / the Agent reference products without
    touching worker filesystems (§28)."""
    if not artifacts:
        return context
    index = context.setdefault("artifacts", [])
    for artifact in artifacts:
        entry = dict(artifact)
        entry.setdefault("step", step_name)
        index.append(entry)
    return context


def step_entry(context: dict, step_name: str) -> dict[str, Any] | None:
    return context.get("steps", {}).get(step_name)
