"""WorkflowService: the single entrypoint for workflows (V1.3 §122/§156).

Agent Tools and the Admin API both land here - neither writes workflow SQL
directly (§72/§157). Owns definition CRUD, run creation/startup and cancel;
advancement lives in WorkflowEngine, notified via the task-terminal observer
and repaired by the WorkflowMonitor.
"""

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.workflow import schemas
from app.workflow.db_models import Workflow, WorkflowRun, WorkflowStepRun
from app.workflow.engine import WorkflowEngine
from app.workflow.errors import (
    WorkflowDisabled,
    WorkflowError,
    WorkflowNotFound,
    WorkflowRunNotFound,
)
from app.workflow.registry import WorkflowRegistry


def _iso(value) -> str | None:
    return value.isoformat() if value else None


class WorkflowService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ------------------------------------------------------------- definitions

    def create_workflow(self, definition: schemas.WorkflowDefinitionIn) -> Workflow:
        return WorkflowRegistry(self.db).create(definition)

    def list_workflows(self, status: str | None = None) -> list[Workflow]:
        return WorkflowRegistry(self.db).list(status)

    def get_workflow(self, workflow_id: str) -> Workflow:
        return WorkflowRegistry(self.db).get(workflow_id)

    def find_workflow(self, name: str, version: str | None = None) -> Workflow:
        return WorkflowRegistry(self.db).find(name, version)

    def get_steps(self, workflow_id: str) -> list:
        return WorkflowRegistry(self.db).get_steps(workflow_id)

    def enable_workflow(self, workflow_id: str) -> Workflow:
        return WorkflowRegistry(self.db).enable(workflow_id)

    def disable_workflow(self, workflow_id: str) -> Workflow:
        return WorkflowRegistry(self.db).disable(workflow_id)

    # -------------------------------------------------------------------- runs

    def create_run(
        self,
        name: str,
        version: str | None = None,
        variables: dict | None = None,
        trigger_type: str = "api",
        created_by: str = "admin",
    ) -> tuple[WorkflowRun, list[str]]:
        """Resolve the definition, create the run, start it into the first
        step. Returns (run, task_ids_to_dispatch)."""
        workflow = WorkflowRegistry(self.db).find(name, version)
        if workflow.status != "ENABLED":
            raise WorkflowDisabled(workflow.name, workflow.version)
        engine = WorkflowEngine(self.db)
        run = engine.create_run(workflow, variables, trigger_type, created_by)
        dispatch_ids = engine.start_run(run.run_id)
        return self.get_run(run.run_id), dispatch_ids

    def get_run(self, run_id: str) -> WorkflowRun:
        row = self.db.scalars(
            select(WorkflowRun).where(WorkflowRun.run_id == run_id)
        ).first()
        if row is None:
            raise WorkflowRunNotFound(run_id)
        return row

    def get_step_runs(self, run_id: str) -> list[WorkflowStepRun]:
        return WorkflowEngine(self.db).get_step_runs(run_id)

    def cancel_run(self, run_id: str) -> dict:
        return WorkflowEngine(self.db).cancel_run(run_id)

    # ------------------------------------------------------------- serialization

    def workflow_out(self, workflow: Workflow) -> schemas.WorkflowOut:
        steps = self.get_steps(workflow.workflow_id)
        return schemas.WorkflowOut(
            workflow_id=workflow.workflow_id,
            name=workflow.name,
            version=workflow.version,
            description=workflow.description,
            status=workflow.status,
            active_singleton=workflow.active_singleton,
            risk_level=workflow.risk_level,
            requires_confirmation=workflow.requires_confirmation,
            created_at=_iso(workflow.created_at) or "",
            steps=[
                schemas.WorkflowStepOut(
                    step_id=s.step_id,
                    name=s.name,
                    order_no=s.order_no,
                    command=s.command,
                    params=s.params or {},
                    device_id=s.device_id,
                    capability_version=s.capability_version,
                    on_failure=s.on_failure,
                    retry_policy=s.retry_policy or {},
                    enabled=s.enabled,
                )
                for s in steps
            ],
        )

    def run_out(self, run: WorkflowRun) -> schemas.WorkflowRunOut:
        current = None
        for step_run in self.get_step_runs(run.run_id):
            if step_run.status in ("READY", "RUNNING"):
                current = step_run.name
                break
        return schemas.WorkflowRunOut(
            run_id=run.run_id,
            workflow_id=run.workflow_id,
            workflow_name=run.workflow_name,
            workflow_version=run.workflow_version,
            status=run.status,
            trigger_type=run.trigger_type,
            created_by=run.created_by,
            current_step=current,
            error_code=run.error_code,
            error_message=run.error_message,
            created_at=_iso(run.created_at) or "",
            started_at=_iso(run.started_at),
            finished_at=_iso(run.finished_at),
            context=run.context_json or {},
            steps=[
                schemas.WorkflowStepRunOut(
                    step_run_id=s.step_run_id,
                    name=s.name,
                    order_no=s.order_no,
                    command=s.command,
                    capability_version=s.capability_version,
                    status=s.status,
                    task_id=s.task_id,
                    retry_count=s.retry_count,
                    result=s.result,
                    error_code=s.error_code,
                    error_message=s.error_message,
                    started_at=_iso(s.started_at),
                    finished_at=_iso(s.finished_at),
                )
                for s in self.get_step_runs(run.run_id)
            ],
        )


__all__ = ["WorkflowService", "WorkflowError"]
