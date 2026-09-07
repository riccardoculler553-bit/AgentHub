"""Task/Attempt state machine unit tests (V1.1 §5/§6/§9)."""

from app.task.state import (
    ATTEMPT_TERMINAL_STATES,
    TASK_TERMINAL_STATES,
    can_transition,
    is_attempt_terminal,
    is_task_terminal,
)


def test_task_happy_path_transitions():
    path = ["PENDING", "DISPATCHING", "SENT", "ACCEPTED", "RUNNING", "SUCCESS"]
    for current, target in zip(path, path[1:]):
        assert can_transition("task", current, target), f"{current} -> {target}"


def test_task_timeout_and_cancel_paths():
    for live in ("PENDING", "DISPATCHING", "SENT", "ACCEPTED", "RUNNING"):
        if live != "DISPATCHING":  # DISPATCHING cancels go through FAILED/PENDING
            assert can_transition("task", live, "CANCELLED"), live
    for live in ("PENDING", "SENT", "ACCEPTED", "RUNNING"):
        assert can_transition("task", live, "TIMEOUT"), live


def test_task_terminal_states_are_absorbing():
    for terminal in TASK_TERMINAL_STATES:
        for target in ("PENDING", "DISPATCHING", "SENT", "RUNNING", "SUCCESS", "FAILED"):
            assert not can_transition("task", terminal, target), f"{terminal} -> {target}"


def test_task_cannot_reopen_via_success_after_timeout():
    """V1.1 §55.3: late SUCCESS must never flip a TIMEOUT task."""
    assert not can_transition("task", "TIMEOUT", "SUCCESS")


def test_attempt_transitions_cover_retry_and_stale():
    assert can_transition("attempt", "DISPATCHING", "SENT")
    assert can_transition("attempt", "SENT", "ACCEPTED")
    assert can_transition("attempt", "ACCEPTED", "RUNNING")
    for terminal in ATTEMPT_TERMINAL_STATES:
        assert can_transition("attempt", "RUNNING", terminal), terminal
        assert can_transition("attempt", "SENT", terminal), terminal
    # failed dispatch rolls the attempt back to a clean PENDING bookkeeping
    assert can_transition("attempt", "DISPATCHING", "PENDING")


def test_attempt_terminal_states_are_absorbing():
    for terminal in ATTEMPT_TERMINAL_STATES:
        for target in ("RUNNING", "SUCCESS", "TIMEOUT"):
            assert not can_transition("attempt", terminal, target), f"{terminal} -> {target}"


def test_attempt_success_never_overwrites_timeout():
    """V1.1 §9: attempt 1 TIMEOUT + late SUCCESS -> attempt stays TIMEOUT."""
    assert not can_transition("attempt", "TIMEOUT", "SUCCESS")


def test_predicates():
    assert is_task_terminal("CANCELLED") and not is_task_terminal("RUNNING")
    assert is_attempt_terminal("STALE") and not is_attempt_terminal("SENT")
    assert not can_transition("task", "UNKNOWN", "PENDING")
    assert not can_transition("attempt", "UNKNOWN", "RUNNING")
