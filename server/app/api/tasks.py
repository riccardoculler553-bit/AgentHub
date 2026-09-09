"""AgentHub APIs: tasks CRUD/lifecycle + command registry + capabilities.

All management endpoints require the admin token (X-Admin-Token).
"""

import asyncio

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.auth.admin import require_admin
from app.capability.service import CapabilityService
from app.command.service import CommandError, CommandService
from app.core.exceptions import DeviceLinkError
from app.db.database import get_db
from app.task import models as schemas
from app.task.dispatcher import TaskDispatcher
from app.task.errors import TaskError
from app.task.monitor import TaskMonitor
from app.task.service import TaskService

router = APIRouter(prefix="/api", tags=["agenthub"], dependencies=[Depends(require_admin)])


def _task_error(exc: TaskError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)})


def _command_error(exc: CommandError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)})


def _task_detail(db: Session, task_id: str) -> schemas.TaskDetailOut:
    service = TaskService(db)
    task = service.get(task_id)
    return schemas.TaskDetailOut(
        task_id=task.task_id,
        name=task.name,
        target_device_id=task.target_device_id,
        status=task.status,
        priority=task.priority,
        max_attempts=task.max_attempts,
        created_at=task.created_at,
        started_at=task.started_at,
        finished_at=task.finished_at,
        timeout_at=task.timeout_at,
        steps=[
            schemas.StepOut(
                step_id=s.step_id, order_no=s.order_no, command=s.command, params=s.params,
                status=s.status, started_at=s.started_at, finished_at=s.finished_at,
            )
            for s in service.get_steps(task_id)
        ],
        attempts=[
            schemas.AttemptOut(
                attempt_id=a.attempt_id, step_id=a.step_id, device_id=a.device_id,
                attempt_no=a.attempt_no, status=a.status, error_code=a.error_code,
                error_message=a.error_message, created_at=a.created_at, finished_at=a.finished_at,
            )
            for a in service.get_attempts(task_id)
        ],
        events=[
            schemas.EventOut(
                id=e.id, task_id=e.task_id, step_id=e.step_id, attempt_id=e.attempt_id,
                event_type=e.event_type, payload=e.payload, created_at=e.created_at,
            )
            for e in service.get_events(task_id)
        ],
    )


# ---------------------------------------------------------------------- tasks


@router.post("/tasks", response_model=schemas.TaskDetailOut, status_code=status.HTTP_201_CREATED)
async def create_task(payload: schemas.TaskCreateIn, request: Request, db: Session = Depends(get_db)):
    """Validate the ExecutionPlan -> create Task(PENDING) -> dispatch if online."""
    try:
        task = TaskService(db).create(payload)
    except TaskError as exc:
        raise _task_error(exc) from exc
    dispatcher = TaskDispatcher(request.app.state.hub)
    asyncio.create_task(dispatcher.dispatch_task(task.task_id))
    return _task_detail(db, task.task_id)


@router.get("/tasks", response_model=list[schemas.TaskOut])
def list_tasks(
    status_filter: str | None = None,
    device_id: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    service = TaskService(db)
    tasks = service.list_tasks(status=status_filter, device_id=device_id, limit=limit)
    return [
        schemas.TaskOut(
            task_id=t.task_id, name=t.name, target_device_id=t.target_device_id, status=t.status,
            priority=t.priority, max_attempts=t.max_attempts, created_at=t.created_at,
            started_at=t.started_at, finished_at=t.finished_at, timeout_at=t.timeout_at,
        )
        for t in tasks
    ]


@router.get("/tasks/{task_id}", response_model=schemas.TaskDetailOut)
def get_task(task_id: str, db: Session = Depends(get_db)):
    try:
        return _task_detail(db, task_id)
    except TaskError as exc:
        raise _task_error(exc) from exc


@router.get("/tasks/{task_id}/events", response_model=list[schemas.EventOut])
def get_task_events(task_id: str, db: Session = Depends(get_db)):
    try:
        return [
            schemas.EventOut(
                id=e.id, task_id=e.task_id, step_id=e.step_id, attempt_id=e.attempt_id,
                event_type=e.event_type, payload=e.payload, created_at=e.created_at,
            )
            for e in TaskService(db).get_events(task_id)
        ]
    except TaskError as exc:
        raise _task_error(exc) from exc


@router.post("/tasks/{task_id}/cancel", response_model=schemas.TaskDetailOut)
async def cancel_task(task_id: str, request: Request, db: Session = Depends(get_db)):
    try:
        result = TaskService(db).request_cancel(task_id)
    except TaskError as exc:
        raise _task_error(exc) from exc
    if result["notify_device"]:
        monitor = TaskMonitor(request.app.state.hub)
        asyncio.create_task(monitor.notify_cancel(result["task_id"]))
    return _task_detail(db, task_id)


@router.post("/tasks/{task_id}/retry", response_model=schemas.TaskDetailOut)
async def retry_task(task_id: str, request: Request, db: Session = Depends(get_db)):
    try:
        TaskService(db).request_retry(task_id)
    except TaskError as exc:
        raise _task_error(exc) from exc
    dispatcher = TaskDispatcher(request.app.state.hub)
    asyncio.create_task(dispatcher.dispatch_task(task_id))
    return _task_detail(db, task_id)


# ------------------------------------------------------------------- commands


@router.get("/commands", response_model=list[schemas.CommandOut])
def list_commands(db: Session = Depends(get_db)):
    return [
        schemas.CommandOut(
            command_name=c.command_name, version=c.version, description=c.description,
            executor_type=c.executor_type, executor_config=c.executor_config,
            params_schema=c.params_schema, timeout=c.timeout, enabled=c.enabled,
        )
        for c in CommandService(db).list_commands()
    ]


@router.get("/commands/{command_name}", response_model=schemas.CommandOut)
def get_command(command_name: str, db: Session = Depends(get_db)):
    try:
        c = CommandService(db).get_command(command_name)
    except CommandError as exc:
        raise _command_error(exc) from exc
    return schemas.CommandOut(
        command_name=c.command_name, version=c.version, description=c.description,
        executor_type=c.executor_type, executor_config=c.executor_config,
        params_schema=c.params_schema, timeout=c.timeout, enabled=c.enabled,
    )


# --------------------------------------------------------------- capabilities
# V1.4: /api/capabilities now serves the AUTOMATION capability registry
# (app.api.capability). Device command capabilities moved here.


@router.get("/device-capabilities", response_model=list[schemas.CapabilityOut])
def get_all_device_capabilities(db: Session = Depends(get_db)):
    return CapabilityService(db).get_all_grouped()


@router.get("/devices/{device_id}/capabilities", response_model=schemas.CapabilityOut)
def get_device_capabilities(device_id: str, db: Session = Depends(get_db)):
    caps = CapabilityService(db).get_device_capabilities(device_id)
    return schemas.CapabilityOut(
        device_id=device_id,
        capabilities=[{"name": c.command_name, "version": c.version, "enabled": c.enabled} for c in caps],
    )
