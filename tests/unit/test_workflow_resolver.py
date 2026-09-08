"""Context + resolver unit tests (V1.3 §34-§40/§168/§169)."""

import pytest

from app.workflow.context import build_context, record_step_result
from app.workflow.errors import WorkflowParamResolutionFailed
from app.workflow.resolver import resolve_params, resolve_template


def _ctx() -> dict:
    context = build_context({"date": "2026-09-08", "shop": "Amazon-US"})
    record_step_result(
        context,
        "download_orders",
        "task_1",
        "SUCCESS",
        {"file_path": "D:/Data/orders.xlsx", "rows": 1532, "nested": {"a": {"b": "deep"}}},
    )
    return context


STEPS = {"download_orders", "process_orders"}


def test_build_context_shape():
    context = build_context({"date": "2026-09-08"})
    assert context == {"variables": {"date": "2026-09-08"}, "steps": {}}


def test_record_step_result_persists_into_steps():
    context = _ctx()
    assert context["steps"]["download_orders"] == {
        "status": "SUCCESS",
        "task_id": "task_1",
        "result": {"file_path": "D:/Data/orders.xlsx", "rows": 1532, "nested": {"a": {"b": "deep"}}},
    }


def test_variable_reference_whole_value():
    assert resolve_template("{{ variables.date }}", _ctx(), STEPS) == "2026-09-08"


def test_step_result_reference_whole_value_keeps_type():
    # whole-string template preserves the JSON type (§38)
    assert resolve_template("{{ steps.download_orders.result.rows }}", _ctx(), STEPS) == 1532
    assert resolve_template("{{ steps.download_orders.result.file_path }}", _ctx(), STEPS) == "D:/Data/orders.xlsx"


def test_nested_path_resolution():
    assert resolve_template("{{ steps.download_orders.result.nested.a.b }}", _ctx(), STEPS) == "deep"


def test_partial_substitution_stringifies():
    resolved = resolve_template("orders-{{ variables.date }}.xlsx", _ctx(), STEPS)
    assert resolved == "orders-2026-09-08.xlsx"


def test_params_resolved_recursively():
    params = resolve_params(
        {
            "date": "{{ variables.date }}",
            "file_path": "{{ steps.download_orders.result.file_path }}",
            "flags": ["{{ variables.shop }}"],
            "inner": {"rows": "{{ steps.download_orders.result.rows }}"},
            "literal": 7,
        },
        _ctx(),
        STEPS,
    )
    assert params == {
        "date": "2026-09-08",
        "file_path": "D:/Data/orders.xlsx",
        "flags": ["Amazon-US"],
        "inner": {"rows": 1532},
        "literal": 7,
    }


def test_missing_variable_fails():
    with pytest.raises(WorkflowParamResolutionFailed):
        resolve_template("{{ variables.nope }}", _ctx(), STEPS)


def test_unknown_step_reference_fails_never_guesses():
    with pytest.raises(WorkflowParamResolutionFailed):
        resolve_template("{{ steps.ghost.result.file_path }}", _ctx(), STEPS)


def test_path_segment_missing_fails():
    with pytest.raises(WorkflowParamResolutionFailed):
        resolve_template("{{ steps.download_orders.result.ghost }}", _ctx(), STEPS)


def test_future_step_reference_fails():
    """A step that has not completed yet cannot be referenced (§80)."""
    context = build_context({})
    with pytest.raises(WorkflowParamResolutionFailed):
        resolve_template("{{ steps.process_orders.result.x }}", context, STEPS)


def test_failed_step_reference_fails():
    context = build_context({})
    record_step_result(context, "download_orders", "task_1", "FAILED", {})
    with pytest.raises(WorkflowParamResolutionFailed):
        resolve_template("{{ steps.download_orders.result.x }}", context, STEPS)


def test_object_ref_inside_partial_string_fails():
    with pytest.raises(WorkflowParamResolutionFailed):
        resolve_template("prefix-{{ steps.download_orders.result.nested }}", _ctx(), STEPS)


def test_invalid_template_syntax_is_left_alone():
    """Only the documented syntax is interpreted - everything else is a literal."""
    assert resolve_template("{{ os.system('x') }}", _ctx(), STEPS) == "{{ os.system('x') }}"
