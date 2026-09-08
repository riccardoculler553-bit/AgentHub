"""Task tools (PDF §30-§34): read task facts, retry/cancel through services.

retry_task / cancel_task never write SQL themselves - they call
TaskService.request_retry/request_cancel, so V1.1 state-machine and
idempotency rules stay the single source of truth (PDF §54-§55).
"""

import asyncio
import logging
import time
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agent.tools.base import AgentTool, RiskLevel, ToolErrorCodes, ToolResult
from app.agent.tools.device import _resolve_device
from app.agent.tools.registry import ToolRegistry
from app.core.config import settings
from app.db.database import SessionLocal
from app.task.db_models import Task
from app.task.dispatcher import TaskDispatcher
from app.task.errors import TaskError, TaskNotFound
from app.task.monitor import TaskMonitor
from app.task.service import TERMINAL_TASK_STATES, RETRYABLE_TASK_STATES, TaskService
from app.task.waiters import task_waiters

logger = logging.getLogger(__name__)


class RecentTasksArgs(BaseModel):
    model_config = {"extra": "forbid"}

    device_name: str | None = Field(default=None, description="按设备名过滤")
    command: str | None = Field(default=None, description="按命令过滤，如 yingdao.audit")
    status: str | None = Field(default=None, description="按状态过滤，如 FAILED")
    limit: int = Field(default=5, ge=1, le=20)


class TaskIdArgs(BaseModel):
    model_config = {"extra": "forbid"}

    task_id: str = Field(min_length=1)


class TaskEventsArgs(BaseModel):
    model_config = {"extra": "forbid"}

    task_id: str = Field(min_length=1)
    limit: int = Field(default=20, ge=1, le=100)


class RetryTaskArgs(BaseModel):
    model_config = {"extra": "forbid"}

    task_id: str = Field(min_length=1)


# ------------------------------------------------------------------ helpers

def _final_result(service: TaskService, task_id: str) -> dict | None:
    for event in reversed(service.get_events(task_id)):
        if event.event_type in ("task.success", "task.failed", "task.timeout", "task.cancelled"):
            payload = dict(event.payload or {})
            return {"event": event.event_type, "payload": payload}
    return None


def _task_summary(service: TaskService, task: Task, steps=None) -> dict:
    if steps is None:
        steps = service.get_steps(task.task_id)
    return {
        "task_id": task.task_id,
        "name": task.name,
        "status": task.status,
        "target_device_id": task.target_device_id,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "finished_at": task.finished_at.isoformat() if task.finished_at else None,
        "commands": [s.command for s in steps],
    }


def _latest_error_code(service: TaskService, task_id: str) -> str | None:
    attempts = service.get_attempts(task_id)
    for attempt in reversed(attempts):
        if attempt.error_code:
            return attempt.error_code
    return None


async def _wait_terminal(task_id: str, timeout: float) -> tuple[str | None, dict | None]:
    """Wait for a task to reach a terminal state (PDF §44: internal wait, the
    LLM never polls). task_waiters event wake-up + DB polling fallback,
    mirroring the MVP wait_result node."""
    event = task_waiters.register(task_id)
    deadline = time.monotonic() + timeout
    try:
        while True:
            with SessionLocal() as db:
                status = db.scalars(select(Task.status).where(Task.task_id == task_id)).first()
            if status in TERMINAL_TASK_STATES:
                with SessionLocal() as db:
                    service = TaskService(db)
                    return status, _final_result(service, task_id)
            if time.monotonic() > deadline:
                return status, None  # still RUNNING/PENDING: report, don't lie
            try:
                await asyncio.wait_for(
                    asyncio.shield(event.wait()), timeout=settings.agent_poll_interval * 5
                )
            except asyncio.TimeoutError:
                continue
    finally:
        task_waiters.unregister(task_id, event)


def _dispatch(hub: Any, task_id: str):
    return TaskDispatcher(hub).dispatch_task(task_id)


# ------------------------------------------------------------------- register

def register_task_tools(registry: ToolRegistry, hub: Any = None) -> None:
    async def get_recent_tasks(db: Session, args: dict) -> ToolResult:
        service = TaskService(db)
        device_id = None
        if args.get("device_name"):
            device, err = _resolve_device(db, args["device_name"])
            if err:
                return err
            device_id = device.device_id
        tasks = service.list_tasks(
            status=args.get("status"), device_id=device_id, limit=max(args["limit"] * 3, 10)
        )
        summaries: list[dict] = []
        for task in tasks:
            steps = service.get_steps(task.task_id)
            if args.get("command") and args["command"] not in [s.command for s in steps]:
                continue
            summary = _task_summary(service, task, steps)
            summary["error_code"] = _latest_error_code(service, task.task_id)
            summaries.append(summary)
            if len(summaries) >= args["limit"]:
                break
        return ToolResult.ok({"tasks": summaries})

    async def get_task_detail(db: Session, args: dict) -> ToolResult:
        service = TaskService(db)
        try:
            task = service.get(args["task_id"])
        except TaskNotFound:
            return ToolResult.fail(
                ToolErrorCodes.TASK_NOT_FOUND, f"task '{args['task_id']}' not found"
            )
        steps = service.get_steps(task.task_id)
        detail = _task_summary(service, task, steps)
        detail["priority"] = task.priority
        detail["max_attempts"] = task.max_attempts
        detail["steps"] = [
            {"step_id": s.step_id, "command": s.command, "status": s.status, "params": s.params}
            for s in steps
        ]
        detail["attempts"] = [
            {
                "attempt_id": a.attempt_id,
                "attempt_no": a.attempt_no,
                "status": a.status,
                "error_code": a.error_code,
                "error_message": a.error_message,
            }
            for a in service.get_attempts(task.task_id)
        ]
        detail["result"] = _final_result(service, task.task_id)
        return ToolResult.ok(detail)

    async def get_task_events(db: Session, args: dict) -> ToolResult:
        service = TaskService(db)
        try:
            service.get(args["task_id"])
        except TaskNotFound:
            return ToolResult.fail(
                ToolErrorCodes.TASK_NOT_FOUND, f"task '{args['task_id']}' not found"
            )
        events = service.get_events(args["task_id"])
        # context compression (PDF §109): drop bulky payloads, keep what the
        # LLM needs to reason - type, time, attempt, error fields.
        trimmed = [
            {
                "event_type": e.event_type,
                "created_at": e.created_at.isoformat() if e.created_at else None,
                "attempt_id": e.attempt_id,
                "error_code": (e.payload or {}).get("error_code") or ((e.payload or {}).get("error") or {}).get("code"),
                "message": (e.payload or {}).get("message") or ((e.payload or {}).get("error") or {}).get("message"),
            }
            for e in events
        ]
        return ToolResult.ok({"events": trimmed[-args["limit"]:]})

    async def retry_task(db: Session, args: dict) -> ToolResult:
        service = TaskService(db)
        try:
            task = service.get(args["task_id"])
        except TaskNotFound:
            return ToolResult.fail(ToolErrorCodes.TASK_NOT_FOUND, f"task '{args['task_id']}' not found")
        if task.status not in RETRYABLE_TASK_STATES:
            return ToolResult.fail(
                ToolErrorCodes.INVALID_TASK_STATE,
                f"task is {task.status}; only {sorted(RETRYABLE_TASK_STATES)} tasks can be retried",
                data={"task_id": task.task_id, "status": task.status},
            )
        try:
            service.request_retry(args["task_id"])
        except TaskError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        # dispatch immediately; if offline it stays PENDING and TaskMonitor
        # re-dispatches when the device returns.
        if hub is not None:
            await TaskDispatcher(hub).dispatch_task(args["task_id"])
        status, result = await _wait_terminal(args["task_id"], settings.agent_tool_wait_max)
        return ToolResult.ok({"task_id": args["task_id"], "status": status, "result": result})

    async def cancel_task(db: Session, args: dict) -> ToolResult:
        service = TaskService(db)
        try:
            outcome = service.request_cancel(args["task_id"])
        except TaskNotFound:
            return ToolResult.fail(ToolErrorCodes.TASK_NOT_FOUND, f"task '{args['task_id']}' not found")
        except TaskError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        if outcome.get("notify_device") and hub is not None:
            try:
                await TaskMonitor(hub).notify_cancel(args["task_id"])
            except Exception:  # noqa: BLE001 - notify is best-effort (API parity)
                logger.exception("cancel notify failed for %s", args["task_id"])
        status, _ = await _wait_terminal(args["task_id"], min(settings.agent_tool_wait_max, 60))
        return ToolResult.ok({"task_id": args["task_id"], "status": status or "CANCELLED"})

    registry.register(
        AgentTool(
            name="get_recent_tasks",
            description="查询设备最近的（历史）任务列表，可按命令/状态过滤。诊断'昨天为什么失败'从这里开始。",
            handler=get_recent_tasks,
            args_schema=RecentTasksArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="get_task_detail",
            description="查询单个任务的完整详情（步骤/尝试/最终结果）。",
            handler=get_task_detail,
            args_schema=TaskIdArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="get_task_events",
            description="查询任务的生命周期事件流（派发/运行/失败及错误码），用于判断失败原因。",
            handler=get_task_events,
            args_schema=TaskEventsArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="retry_task",
            description="重试一个失败/超时的任务（会重新派发执行并等待结果）。需要用户确认。",
            handler=retry_task,
            args_schema=RetryTaskArgs,
            risk_level=RiskLevel.WRITE,
            requires_confirmation=True,
            max_calls=1,
            waits_task=True,
        )
    )
    registry.register(
        AgentTool(
            name="cancel_task",
            description="取消一个正在运行或等待中的任务。需要用户确认。",
            handler=cancel_task,
            args_schema=TaskIdArgs,
            risk_level=RiskLevel.WRITE,
            requires_confirmation=True,
            max_calls=2,
            waits_task=True,
        )
    )
