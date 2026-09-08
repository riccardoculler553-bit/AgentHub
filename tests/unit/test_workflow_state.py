"""Workflow state machine unit tests (V1.3 §127/§166/§167).

Legal transitions apply; terminal states never reopen (§52/§53).
"""

from app.workflow.state import (
    STEP_RUN_TERMINAL_STATES,
    WORKFLOW_TRANSITIONS,
    can_step_run_transition,
    can_workflow_transition,
    is_step_run_terminal,
    is_workflow_terminal,
)


def test_workflow_legal_transitions():
    assert can_workflow_transition("PENDING", "RUNNING")
    assert can_workflow_transition("RUNNING", "SUCCESS")
    assert can_workflow_transition("RUNNING", "FAILED")
    assert can_workflow_transition("RUNNING", "CANCELLED")
    assert can_workflow_transition("PENDING", "CANCELLED")  # cancel before start


def test_workflow_terminal_never_reopens():
    for terminal in ("SUCCESS", "FAILED", "CANCELLED"):
        assert WORKFLOW_TRANSITIONS[terminal] == set()
        assert not can_workflow_transition(terminal, "RUNNING")
        assert is_workflow_terminal(terminal)


def test_step_run_legal_transitions():
    assert can_step_run_transition("PENDING", "READY")
    assert can_step_run_transition("READY", "RUNNING")
    assert can_step_run_transition("RUNNING", "SUCCESS")
    assert can_step_run_transition("RUNNING", "FAILED")
    assert can_step_run_transition("RUNNING", "CANCELLED")
    # failure-stop skips the remaining steps (§130); cancel closes them (§131)
    assert can_step_run_transition("PENDING", "SKIPPED")
    assert can_step_run_transition("READY", "CANCELLED")
    assert can_step_run_transition("PENDING", "CANCELLED")


def test_step_run_terminal_never_reopens():
    for terminal in STEP_RUN_TERMINAL_STATES:
        assert not can_step_run_transition(terminal, "READY")
        assert not can_step_run_transition(terminal, "RUNNING")
        assert is_step_run_terminal(terminal)


def test_retry_is_not_a_step_run_transition():
    """§45/§132: a step retry is a Task Engine retry - the StepRun never goes
    FAILED -> RUNNING; it stays RUNNING while the task re-attempts."""
    assert not can_step_run_transition("FAILED", "RUNNING")
    assert not can_step_run_transition("FAILED", "READY")


def test_unknown_states_never_transition():
    assert not can_workflow_transition("WHATEVER", "RUNNING")
    assert not can_step_run_transition("WHATEVER", "READY")
