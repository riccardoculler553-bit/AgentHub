"""Workflow state machines (V1.3 §127/§128).

Single source of truth for legal Workflow/StepRun transitions. Every status
mutation in WorkflowEngine/WorkflowService must go through `can_transition`
plus a DB-level conditional UPDATE (CAS) for concurrency.

Core rules:
- Workflow: PENDING -> RUNNING -> {SUCCESS, FAILED, CANCELLED}. Terminal
  states are irreversible (§52): no reopen, ever.
- StepRun: PENDING -> READY -> RUNNING -> {SUCCESS, FAILED, CANCELLED}
  (§18/§127). SKIPPED closes steps that will never run (failure-stop §130);
  cancel prefers CANCELLED as the unified terminal for remaining steps (§131).
- Retry is NOT a transition: a failed step retry is a Task Engine retry
  (TaskService.request_retry) while the StepRun stays RUNNING (§45/§132).
"""

WORKFLOW_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED"}
STEP_RUN_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED", "SKIPPED"}

# workflow_runs.status
WORKFLOW_TRANSITIONS: dict[str, set[str]] = {
    "PENDING": {"RUNNING", "CANCELLED"},
    "RUNNING": {"SUCCESS", "FAILED", "CANCELLED"},
    # terminal states: no outgoing transitions
    "SUCCESS": set(),
    "FAILED": set(),
    "CANCELLED": set(),
}

# workflow_step_runs.status
STEP_RUN_TRANSITIONS: dict[str, set[str]] = {
    "PENDING": {"READY", "CANCELLED", "SKIPPED"},
    "READY": {"RUNNING", "CANCELLED", "SKIPPED"},
    "RUNNING": {"SUCCESS", "FAILED", "CANCELLED"},
    # terminal states: no outgoing transitions
    "SUCCESS": set(),
    "FAILED": set(),
    "CANCELLED": set(),
    "SKIPPED": set(),
}


def can_workflow_transition(current: str, target: str) -> bool:
    return target in WORKFLOW_TRANSITIONS.get(current, set())


def can_step_run_transition(current: str, target: str) -> bool:
    return target in STEP_RUN_TRANSITIONS.get(current, set())


def is_workflow_terminal(status: str) -> bool:
    return status in WORKFLOW_TERMINAL_STATES


def is_step_run_terminal(status: str) -> bool:
    return status in STEP_RUN_TERMINAL_STATES
