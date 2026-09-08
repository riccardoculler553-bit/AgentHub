"""Workflow Admin APIs (V1.3 §101-§107/§156).

All endpoints require the admin token (§106/§160). Everything goes through
WorkflowService - the API never writes workflow state directly (§157).
"""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.auth.admin import require_admin
from app.db.database import get_db
from app.task.dispatcher import TaskDispatcher
from app.workflow import schemas
from app.workflow.errors import WorkflowError
from app.workflow.service import WorkflowService

router = APIRouter(prefix="/api", tags=["workflows"], dependencies=[Depends(require_admin)])


def _workflow_error(exc: WorkflowError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)})


# ---------------------------------------------------------------- definitions


@router.post("/workflows", response_model=schemas.WorkflowOut, status_code=status.HTTP_201_CREATED)
def create_workflow(payload: schemas.WorkflowDefinitionIn, db: Session = Depends(get_db)):
    try:
        workflow = WorkflowService(db).create_workflow(payload)
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc
    return WorkflowService(db).workflow_out(workflow)


@router.get("/workflows", response_model=list[schemas.WorkflowOut])
def list_workflows(status_filter: str | None = None, db: Session = Depends(get_db)):
    service = WorkflowService(db)
    return [service.workflow_out(w) for w in service.list_workflows(status_filter)]


@router.get("/workflows/{workflow_id}", response_model=schemas.WorkflowOut)
def get_workflow(workflow_id: str, db: Session = Depends(get_db)):
    service = WorkflowService(db)
    try:
        return service.workflow_out(service.get_workflow(workflow_id))
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc


@router.post("/workflows/{workflow_id}/enable", response_model=schemas.WorkflowOut)
def enable_workflow(workflow_id: str, db: Session = Depends(get_db)):
    service = WorkflowService(db)
    try:
        workflow = service.enable_workflow(workflow_id)
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc
    return service.workflow_out(workflow)


@router.post("/workflows/{workflow_id}/disable", response_model=schemas.WorkflowOut)
def disable_workflow(workflow_id: str, db: Session = Depends(get_db)):
    service = WorkflowService(db)
    try:
        workflow = service.disable_workflow(workflow_id)
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc
    return service.workflow_out(workflow)


# ----------------------------------------------------------------------- runs


@router.post("/workflows/{workflow_id}/runs", response_model=schemas.WorkflowRunOut, status_code=status.HTTP_201_CREATED)
async def create_run(
    workflow_id: str,
    payload: schemas.RunWorkflowIn,
    request: Request,
    db: Session = Depends(get_db),
):
    from app.workflow.errors import WorkflowDisabled

    service = WorkflowService(db)
    try:
        workflow = service.get_workflow(workflow_id)
        if workflow.status != "ENABLED":
            raise WorkflowDisabled(workflow.name, workflow.version)
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc
    try:
        run, dispatch_ids = service.create_run(
            workflow.name,
            version=payload.version,
            variables=payload.variables,
            trigger_type="api",
            created_by="admin",
        )
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc
    for task_id in dispatch_ids:
        asyncio.create_task(TaskDispatcher(request.app.state.hub).dispatch_task(task_id))
    return service.run_out(run)


@router.get("/workflow-runs", response_model=list[schemas.WorkflowRunOut])
def list_runs(status_filter: str | None = None, limit: int = 50, db: Session = Depends(get_db)):
    from sqlalchemy import select

    from app.workflow.db_models import WorkflowRun
    from app.workflow.state import WORKFLOW_TERMINAL_STATES

    stmt = select(WorkflowRun).order_by(WorkflowRun.id.desc()).limit(max(1, min(limit, 200)))
    if status_filter:
        stmt = stmt.where(WorkflowRun.status == status_filter)
    service = WorkflowService(db)
    return [service.run_out(run) for run in db.scalars(stmt)]


@router.get("/workflow-runs/{run_id}", response_model=schemas.WorkflowRunOut)
def get_run(run_id: str, db: Session = Depends(get_db)):
    service = WorkflowService(db)
    try:
        return service.run_out(service.get_run(run_id))
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc


@router.post("/workflow-runs/{run_id}/cancel", response_model=schemas.WorkflowRunOut)
async def cancel_run(run_id: str, request: Request, db: Session = Depends(get_db)):
    service = WorkflowService(db)
    try:
        outcome = service.cancel_run(run_id)
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc
    task_id = outcome.get("notify_task_id")
    if task_id:
        from app.task.monitor import TaskMonitor

        asyncio.create_task(TaskMonitor(request.app.state.hub).notify_cancel(task_id))
    return service.run_out(service.get_run(run_id))


@router.get("/workflow-runs/{run_id}/events")
def get_run_events(run_id: str, db: Session = Depends(get_db)):
    from sqlalchemy import select

    from app.workflow.db_models import WorkflowEvent

    service = WorkflowService(db)
    try:
        service.get_run(run_id)
    except WorkflowError as exc:
        raise _workflow_error(exc) from exc
    events = db.scalars(
        select(WorkflowEvent).where(WorkflowEvent.run_id == run_id).order_by(WorkflowEvent.id)
    ).all()
    return [
        {
            "id": e.id,
            "run_id": e.run_id,
            "step_run_id": e.step_run_id,
            "event_type": e.event_type,
            "payload": e.payload or {},
            "created_at": e.created_at.isoformat() if e.created_at else None,
        }
        for e in events
    ]
