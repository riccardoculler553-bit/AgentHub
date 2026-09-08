"""WorkflowRegistry unit tests (V1.3 §21-§23/§108/§192)."""

import pytest
from pydantic import ValidationError
from sqlalchemy import select

from app.command.db_models import Command
from app.db.database import SessionLocal
from app.workflow.errors import (
    WorkflowAlreadyExists,
    WorkflowInvalid,
    WorkflowNotFound,
    WorkflowVersionNotFound,
)
from app.workflow.registry import WorkflowRegistry
from app.workflow.schemas import WorkflowDefinitionIn, WorkflowStepDefinition


def _definition(name="amazon_daily_report", version="1.0.0", command="echo", **kwargs) -> WorkflowDefinitionIn:
    return WorkflowDefinitionIn(
        name=name,
        version=version,
        description="日报流程",
        **kwargs,
        steps=[
            WorkflowStepDefinition(name="download_orders", command=command),
            WorkflowStepDefinition(
                name="process_orders",
                command=command,
                params={"file_path": "{{ steps.download_orders.result.file_path }}"},
                on_failure="retry",
                retry_policy={"max_attempts": 2, "retry_on": ["EXECUTOR_FAILED"]},
            ),
        ],
    )


@pytest.fixture()
def registry():
    with SessionLocal() as db:
        db.add(Command(command_name="echo", version="1.0", executor_type="echo", enabled=True))
        db.commit()
        yield WorkflowRegistry(db)


def test_create_persists_definition_and_steps(registry):
    workflow = registry.create(_definition())
    assert workflow.status == "DRAFT"  # §108: created drafts cannot run
    steps = registry.get_steps(workflow.workflow_id)
    assert [s.name for s in steps] == ["download_orders", "process_orders"]
    assert [s.order_no for s in steps] == [1, 2]
    assert steps[1].retry_policy == {"max_attempts": 2, "retry_on": ["EXECUTOR_FAILED"]}
    assert steps[1].params == {"file_path": "{{ steps.download_orders.result.file_path }}"}


def test_validate_rejects_unknown_command(registry):
    problems = registry.validate(_definition(command="amazon.order.download"))
    assert any("unknown command" in p for p in problems)


def test_validate_rejects_disabled_command():
    with SessionLocal() as db:
        db.add(Command(command_name="disabled_cmd", version="1.0", executor_type="echo", enabled=False))
        db.commit()
        problems = WorkflowRegistry(db).validate(_definition(command="disabled_cmd"))
    assert any("disabled" in p for p in problems)


def test_contract_rejects_bad_name_and_version():
    with pytest.raises(ValidationError):
        _definition(name="Bad Name")
    with pytest.raises(ValidationError):
        _definition(version="1.0")


def test_validate_rejects_duplicate_step_names(registry):
    definition = WorkflowDefinitionIn(
        name="ok_name",
        version="1.0.0",
        steps=[
            WorkflowStepDefinition(name="step_one", command="echo"),
            WorkflowStepDefinition(name="step_one", command="echo"),
        ],
    )
    problems = registry.validate(definition)
    assert any("unique" in p for p in problems)


def test_create_duplicate_name_version_rejected(registry):
    registry.create(_definition())
    with pytest.raises(WorkflowAlreadyExists):
        registry.create(_definition())


def test_find_active_version_returns_enabled_newest(registry):
    registry.create(_definition(version="1.0.0"))
    workflow_v2 = registry.create(_definition(version="1.1.0"))
    with pytest.raises(WorkflowNotFound):
        registry.find("amazon_daily_report")  # nothing ENABLED yet
    registry.enable(workflow_v2.workflow_id)
    assert registry.find("amazon_daily_report").version == "1.1.0"
    with pytest.raises(WorkflowVersionNotFound):
        registry.find("amazon_daily_report", "9.9.9")


def test_enable_disables_other_versions_of_same_name(registry):
    v1 = registry.create(_definition(version="1.0.0"))
    v2 = registry.create(_definition(version="1.1.0"))
    registry.enable(v1.workflow_id)
    assert registry.get(v1.workflow_id).status == "ENABLED"
    registry.enable(v2.workflow_id)
    assert registry.get(v2.workflow_id).status == "ENABLED"
    assert registry.get(v1.workflow_id).status == "DISABLED"  # §67: one active version


def test_enable_revalidates_commands(registry):
    workflow = registry.create(_definition())
    registry.disable(workflow.workflow_id)
    assert registry.get(workflow.workflow_id).status == "DISABLED"
    registry.enable(workflow.workflow_id)
    assert registry.get(workflow.workflow_id).status == "ENABLED"
    # commands are re-validated on enable: killing the command breaks it
    with SessionLocal() as db:
        command = db.scalars(select(Command).where(Command.command_name == "echo")).first()
        command.enabled = False
        db.commit()
    with pytest.raises(WorkflowInvalid):
        registry.enable(workflow.workflow_id)
