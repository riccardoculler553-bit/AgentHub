"""WorkflowRegistry: definition catalogue (V1.3 §21-§23/§192).

Like the Command Registry: register/get/version/enable/disable/validate.
Definitions are immutable per (name, version) - fixes ship as a new version
(§63-§66); enabling one version disables the other versions of the same
name (§67: one active version).
"""

import re
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.command.service import CommandDisabled, CommandError, CommandNotFound, CommandService
from app.workflow.db_models import Workflow, WorkflowStep
from app.workflow.errors import (
    WorkflowAlreadyExists,
    WorkflowInvalid,
    WorkflowNotFound,
    WorkflowVersionNotFound,
)
from app.workflow.schemas import WorkflowDefinitionIn, WorkflowStepDefinition

_NAME_RE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class WorkflowRegistry:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ---------------------------------------------------------------- create

    def create(self, definition: WorkflowDefinitionIn) -> Workflow:
        """Validate against the Command Registry, then persist the immutable
        definition (§22/§23: every step command must exist and be enabled)."""
        problems = self.validate(definition)
        if problems:
            raise WorkflowInvalid(problems)
        dup = self.db.scalars(
            select(Workflow).where(
                Workflow.name == definition.name, Workflow.version == definition.version
            )
        ).first()
        if dup is not None:
            raise WorkflowAlreadyExists(definition.name, definition.version)

        workflow = Workflow(
            workflow_id=_new_id("wf"),
            name=definition.name,
            version=definition.version,
            description=definition.description,
            status="ENABLED" if definition.enabled else "DRAFT",
            active_singleton=definition.active_singleton,
            risk_level=definition.risk_level,
            requires_confirmation=definition.requires_confirmation,
            definition_json=definition.model_dump(),
        )
        self.db.add(workflow)
        for order_no, step in enumerate(definition.steps, start=1):
            self.db.add(
                WorkflowStep(
                    step_id=_new_id("wfstep"),
                    workflow_id=workflow.workflow_id,
                    name=step.name,
                    order_no=order_no,
                    command=step.command,
                    capability_version=step.capability_version,
                    params=step.params,
                    device_id=step.device_id,
                    on_failure=step.on_failure,
                    retry_policy=step.retry_policy.model_dump(),
                    enabled=step.enabled,
                )
            )
        self.db.commit()
        return self.get(workflow.workflow_id)

    # --------------------------------------------------------------- validate

    def validate(self, definition: WorkflowDefinitionIn) -> list[str]:
        problems: list[str] = []
        if not _NAME_RE.match(definition.name):
            problems.append(f"invalid workflow name: {definition.name}")
        if not _VERSION_RE.match(definition.version):
            problems.append(f"invalid version: {definition.version} (expect semver x.y.z)")
        names = [s.name for s in definition.steps]
        if not names:
            problems.append("workflow needs at least one step")
        if len(set(names)) != len(names):
            problems.append("step names must be unique")
        command_service = CommandService(self.db)
        for index, step in enumerate(definition.steps, start=1):
            if not _NAME_RE.match(step.name):
                problems.append(f"step {index} ({step.name}): invalid step name: {step.name}")
            if step.capability_version is not None:
                # V1.4 §24: capability step - command doubles as the capability
                # name and must exist with the pinned version PUBLISHED.
                problems.extend(self._validate_capability_step(index, step.name, step))
                continue
            try:
                command_service.require_executable(step.command)
            except CommandNotFound:
                problems.append(f"step {index} ({step.name}): unknown command: {step.command}")
            except CommandDisabled:
                problems.append(f"step {index} ({step.name}): command is disabled: {step.command}")
            except CommandError as exc:  # defensive: registry layer error
                problems.append(f"step {index} ({step.name}): {exc}")
        return problems

    def _validate_capability_step(self, index: int, step_name: str, step: WorkflowStepDefinition) -> list[str]:
        from app.capability_runtime.errors import CapabilityError
        from app.capability_runtime.service import CapabilityService

        try:
            CapabilityService(self.db).get_published_version(step.command, step.capability_version)
        except CapabilityError as exc:
            return [f"step {index} ({step_name}): {exc}"]
        return []

    # ----------------------------------------------------------------- query

    def get(self, workflow_id: str) -> Workflow:
        row = self.db.scalars(select(Workflow).where(Workflow.workflow_id == workflow_id)).first()
        if row is None:
            raise WorkflowNotFound(workflow_id)
        return row

    def get_steps(self, workflow_id: str) -> list[WorkflowStep]:
        return list(
            self.db.scalars(
                select(WorkflowStep)
                .where(WorkflowStep.workflow_id == workflow_id)
                .order_by(WorkflowStep.order_no)
            )
        )

    def find(self, name: str, version: str | None = None) -> Workflow:
        """Resolve by name (+version); without a version return the active
        (ENABLED, newest) version (§67)."""
        stmt = select(Workflow).where(Workflow.name == name)
        if version is not None:
            stmt = stmt.where(Workflow.version == version)
            row = self.db.scalars(stmt).first()
            if row is None:
                raise WorkflowVersionNotFound(name, version)
            return row
        row = self.db.scalars(
            stmt.where(Workflow.status == "ENABLED").order_by(Workflow.created_at.desc(), Workflow.id.desc())
        ).first()
        if row is None:
            raise WorkflowNotFound(name)
        return row

    def list(self, status: str | None = None) -> list[Workflow]:
        stmt = select(Workflow).order_by(Workflow.name, Workflow.id)
        if status:
            stmt = stmt.where(Workflow.status == status)
        return list(self.db.scalars(stmt))

    # --------------------------------------------------------------- lifecycle

    def enable(self, workflow_id: str) -> Workflow:
        workflow = self.get(workflow_id)
        definition = WorkflowDefinitionIn.model_validate(workflow.definition_json)
        problems = self.validate(definition)
        if problems:
            raise WorkflowInvalid(problems)
        # one active version per name (§67/§109)
        for other in self.db.scalars(
            select(Workflow).where(Workflow.name == workflow.name, Workflow.workflow_id != workflow_id)
        ):
            other.status = "DISABLED"
        workflow.status = "ENABLED"
        self.db.commit()
        return workflow

    def disable(self, workflow_id: str) -> Workflow:
        workflow = self.get(workflow_id)
        workflow.status = "DISABLED"
        self.db.commit()
        return workflow
