"""Task/Attempt state machines (V1.1 §5/§6/§9/§58).

Single source of truth for legal status transitions. Every status mutation in
TaskService / TaskDispatcher / TaskMonitor must go through `can_transition`
(plus a DB-level conditional UPDATE for concurrency, see dispatcher/monitor).

Core rules:
- Terminal states are irreversible: SUCCESS/FAILED/CANCELLED/TIMEOUT never
  transition anywhere (a late result may NOT reopen a finished task).
- Retry is NOT a transition: it creates a NEW attempt (task goes back to
  PENDING through the explicit retry path, never by mutating the old one).
- STALE marks an attempt that is no longer the current execution context
  (its result arrived late; recorded for audit, never mutates the task).
"""

TASK_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"}
ATTEMPT_TERMINAL_STATES = {"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT", "STALE"}

# task.status: allowed current -> target pairs. Absence = illegal.
TASK_TRANSITIONS: dict[str, set[str]] = {
    "PENDING": {"DISPATCHING", "CANCELLED", "TIMEOUT"},
    "DISPATCHING": {"SENT", "PENDING", "CANCELLED", "FAILED"},
    # A worker may report a terminal result from any live state (fast executors
    # can succeed without a visible running event, or race one in).
    "SENT": {"ACCEPTED", "RUNNING", "SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"},
    "ACCEPTED": {"RUNNING", "SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"},
    "RUNNING": {"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"},
    # terminal states: no outgoing transitions
    "SUCCESS": set(),
    "FAILED": set(),
    "CANCELLED": set(),
    "TIMEOUT": set(),
}

# attempt.status: DISPATCHING -> SENT -> ACCEPTED -> RUNNING -> terminal;
# any live state may become STALE when a newer attempt takes over the step.
_ATTEMPT_LIVE = {"DISPATCHING", "SENT", "ACCEPTED", "RUNNING"}
ATTEMPT_TRANSITIONS: dict[str, set[str]] = {
    "DISPATCHING": {"SENT", "PENDING"} | ATTEMPT_TERMINAL_STATES,
    "SENT": {"ACCEPTED", "RUNNING"} | ATTEMPT_TERMINAL_STATES,
    "ACCEPTED": {"RUNNING"} | ATTEMPT_TERMINAL_STATES,
    "RUNNING": set(ATTEMPT_TERMINAL_STATES),
    "SUCCESS": set(),
    "FAILED": set(),
    "CANCELLED": set(),
    "TIMEOUT": set(),
    "STALE": set(),
}


def can_transition(kind: str, current: str, target: str) -> bool:
    """kind: "task" | "attempt". Unknown states never transition."""
    table = TASK_TRANSITIONS if kind == "task" else ATTEMPT_TRANSITIONS
    return target in table.get(current, set())


def is_task_terminal(status: str) -> bool:
    return status in TASK_TERMINAL_STATES


def is_attempt_terminal(status: str) -> bool:
    return status in ATTEMPT_TERMINAL_STATES
