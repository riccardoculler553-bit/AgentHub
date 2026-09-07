"""TaskService: task lifecycle business logic (the authoritative state layer).

MySQL is the source of truth for task state; LangGraph/memory only hold
reasoning context. Transport ACK (message_ack) and Task ACK (task.accept) are
strictly different things and are handled separately.
"""

import uuid
from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.capability.service import CapabilityService
from app.command.service import CommandDisabled, CommandError, CommandNotFound, CommandService, InvalidParams
from app.core.config import settings
from app.core.exceptions import DeviceLinkError, DeviceNotFound
from app.db.models import utcnow
from app.device.service import DeviceService
from app.task.db_models import Task, TaskAttempt, TaskEvent, TaskStep
from app.task.errors import InvalidTaskState, TaskNotFound, TaskValidationFailed
from app.task.models import TaskCreateIn

TERMINAL_TASK_STATES = {"SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"}
# Task statuses that mean "a dispatch may be flying / worker may still be alive"
LIVE_TASK_STATES = {"DISPATCHING", "SENT", "ACCEPTED", "RUNNING"}
CANCELABLE_TASK_STATES = {"PENDING", "DISPATCHING", "SENT", "ACCEPTED", "RUNNING"}
RETRYABLE_TASK_STATES = {"FAILED", "TIMEOUT"}


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class TaskService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ------------------------------------------------------------------ create

    def create(self, payload: TaskCreateIn, created_by: str = "admin") -> Task:
        """Validate the ExecutionPlan against Registry/Validator, then persist
        Task + Steps + first event. Task starts in PENDING."""
        problems: list[str] = []
        if payload.target_device_id is None:
            problems.append("target_device_id is required in V1.0")
            device = None
        else:
            try:
                device = DeviceService(self.db).get_device(payload.target_device_id)
            except DeviceNotFound:
                device = None
                problems.append(f"target device not found: {payload.target_device_id}")
            else:
                if device.revoked_at is not None:
                    problems.append(f"target device is revoked: {payload.target_device_id}")

        command_service = CommandService(self.db)
        capability_service = CapabilityService(self.db)
        if device is not None:
            for index, step in enumerate(payload.steps):
                try:
                    command = command_service.require_executable(step.command)
                except CommandNotFound:
                    problems.append(f"step {index + 1}: unknown command: {step.command}")
                    continue
                except CommandDisabled:
                    problems.append(f"step {index + 1}: command is disabled: {step.command}")
                    continue
                try:
                    command_service.validate_params(command, step.params)
                except InvalidParams as exc:
                    problems.append(f"step {index + 1}: {exc}")
                    continue
                if not capability_service.has_capability(device.device_id, step.command):
                    problems.append(
                        f"step {index + 1}: device {device.device_id} does not report capability: {step.command}"
                    )
        if problems:
            raise TaskValidationFailed(problems)

        task = Task(
            task_id=_new_id("task"),
            name=payload.name or payload.steps[0].command,
            created_by=created_by,
            target_device_id=device.device_id,
            status="PENDING",
            max_attempts=settings.task_max_attempts,
        )
        self.db.add(task)
        for index, step in enumerate(payload.steps, start=1):
            self.db.add(
                TaskStep(
                    step_id=_new_id("step"),
                    task_id=task.task_id,
                    order_no=index,
                    device_id=device.device_id,
                    command=step.command,
                    params=step.params,
                    status="PENDING",
                )
            )
        self.db.flush()
        self._record(task.task_id, "task.created", payload={"name": task.name, "steps": len(payload.steps)})
        self.db.commit()
        return task

    # ------------------------------------------------------------------ queries

    def get(self, task_id: str) -> Task:
        row = self.db.scalars(select(Task).where(Task.task_id == task_id)).first()
        if row is None:
            raise TaskNotFound(task_id)
        return row

    def get_steps(self, task_id: str) -> list[TaskStep]:
        return list(
            self.db.scalars(
                select(TaskStep).where(TaskStep.task_id == task_id).order_by(TaskStep.order_no)
            )
        )

    def get_attempts(self, task_id: str) -> list[TaskAttempt]:
        return list(
            self.db.scalars(
                select(TaskAttempt).where(TaskAttempt.task_id == task_id).order_by(TaskAttempt.created_at)
            )
        )

    def get_events(self, task_id: str) -> list[TaskEvent]:
        return list(
            self.db.scalars(
                select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.id)
            )
        )

    def list_tasks(self, status: str | None = None, device_id: str | None = None, limit: int = 100) -> list[Task]:
        stmt = select(Task).order_by(Task.id.desc()).limit(max(1, min(limit, 500)))
        if status:
            stmt = stmt.where(Task.status == status)
        if device_id:
            stmt = stmt.where(Task.target_device_id == device_id)
        return list(self.db.scalars(stmt))

    def next_pending_step(self, task_id: str) -> TaskStep | None:
        return self.db.scalars(
            select(TaskStep)
            .where(TaskStep.task_id == task_id, TaskStep.status == "PENDING")
            .order_by(TaskStep.order_no)
        ).first()

    def latest_open_attempt(self, task_id: str, step_id: str) -> TaskAttempt | None:
        return self.db.scalars(
            select(TaskAttempt)
            .where(TaskAttempt.task_id == task_id, TaskAttempt.step_id == step_id)
            .order_by(TaskAttempt.id.desc())
            .limit(1)
        ).first()

    def count_attempts(self, task_id: str, step_id: str) -> int:
        return len(
            self.db.scalars(
                select(TaskAttempt.id).where(
                    TaskAttempt.task_id == task_id, TaskAttempt.step_id == step_id
                )
            ).all()
        )

    # ------------------------------------------------------- device-side events

    def handle_device_event(self, device_id: str, msg_type: str, data: dict) -> dict:
        """Apply task.accept/running/progress/result from a Worker.

        Returns {"task_id", "advance"} where advance=True means the next step
        should be dispatched (the caller triggers the Dispatcher)."""
        task_id = str(data.get("task_id", ""))
        step_id = str(data.get("step_id", ""))
        task = self.get(task_id)
        step = self.db.scalars(
            select(TaskStep).where(TaskStep.step_id == step_id, TaskStep.task_id == task_id)
        ).first()
        if step is None:
            raise TaskNotFound(task_id)

        attempt = self.latest_open_attempt(task_id, step_id)
        if attempt is not None and attempt.device_id not in (None, device_id):
            # A device may only report its own attempts (security boundary).
            return {"task_id": task_id, "advance": False}

        now = utcnow()
        advance = False

        if msg_type == "task.accept":
            if attempt is not None and attempt.status in ("DISPATCHING", "SENT"):
                attempt.status = "ACCEPTED"
                attempt.accepted_at = now
            if task.status in ("DISPATCHING", "SENT"):
                task.status = "ACCEPTED"
            self._record(task_id, "task.accepted", step_id=step_id)

        elif msg_type == "task.running":
            if attempt is not None and attempt.status in ("DISPATCHING", "SENT", "ACCEPTED"):
                attempt.status = "RUNNING"
                attempt.started_at = now
            if step.status in ("PENDING",):
                step.status = "RUNNING"
                step.started_at = now
            if task.status in ("DISPATCHING", "SENT", "ACCEPTED"):
                task.status = "RUNNING"
                task.started_at = now
            self._record(task_id, "task.running", step_id=step_id)

        elif msg_type == "task.progress":
            self._record(
                task_id,
                "task.progress",
                step_id=step_id,
                payload={"progress": data.get("progress"), "message": data.get("message")},
            )

        elif msg_type == "task.result":
            result_status = str(data.get("status", "failed")).lower()
            error = data.get("error") or {}
            result = data.get("result") or {}
            if result_status == "success":
                if attempt is not None:
                    attempt.status = "SUCCESS"
                    attempt.finished_at = now
                step.status = "SUCCESS"
                step.finished_at = now
                if task.status == "CANCELLED":
                    # Late result for an already cancelled task: keep CANCELLED.
                    self._record(task_id, "task.cancelled", step_id=step_id, payload={"late_result": True})
                elif self.next_pending_step(task_id) is not None:
                    advance = True
                    task.status = "RUNNING"
                    self._record(task_id, "task.progress", step_id=step_id, payload={"step_result": result, "step_status": "success"})
                else:
                    task.status = "SUCCESS"
                    task.finished_at = now
                    self._record(task_id, "task.success", step_id=step_id, payload={"result": result})
            elif result_status == "cancelled":
                if attempt is not None and attempt.status not in TERMINAL_TASK_STATES:
                    attempt.status = "CANCELLED"
                    attempt.finished_at = now
                step.status = "CANCELLED"
                step.finished_at = now
                if task.status != "CANCELLED":
                    task.status = "CANCELLED"
                    task.finished_at = now
                self._record(task_id, "task.cancelled", step_id=step_id, payload={"by": "worker"})
            else:  # failed
                error_code = str(error.get("code", "EXECUTOR_FAILED"))[:64]
                error_message = str(error.get("message", ""))[:500]
                if attempt is not None and attempt.status not in TERMINAL_TASK_STATES:
                    attempt.status = "FAILED"
                    attempt.finished_at = now
                    attempt.error_code = error_code
                    attempt.error_message = error_message
                step.status = "FAILED"
                step.finished_at = now
                if task.status != "CANCELLED":
                    task.status = "FAILED"
                    task.finished_at = now
                self._record(task_id, "task.failed", step_id=step_id, payload={"error_code": error_code, "error_message": error_message})
        else:
            return {"task_id": task_id, "advance": False}

        self.db.commit()
        return {"task_id": task_id, "advance": advance}

    # ------------------------------------------------------- admin-side actions

    def request_cancel(self, task_id: str) -> dict:
        task = self.get(task_id)
        if task.status in TERMINAL_TASK_STATES:
            raise InvalidTaskState(task_id, task.status, "cancel")
        now = utcnow()
        task.status = "CANCELLED"
        task.finished_at = now
        self._record(task_id, "task.cancelled", payload={"by": "admin"})
        self.db.commit()
        return {"task_id": task_id, "status": task.status, "notify_device": task.status == "CANCELLED"}

    def request_retry(self, task_id: str) -> dict:
        task = self.get(task_id)
        if task.status not in RETRYABLE_TASK_STATES:
            raise InvalidTaskState(task_id, task.status, "retry")
        step = self.db.scalars(
            select(TaskStep)
            .where(
                TaskStep.task_id == task_id,
                # "PENDING" covers the watchdog case: the attempt timed out
                # before the device ever accepted, so the step never ran.
                TaskStep.status.in_(["FAILED", "TIMEOUT", "PENDING"]),
            )
            .order_by(TaskStep.order_no)
            .limit(1)
        ).first()
        if step is None:
            raise InvalidTaskState(task_id, task.status, "retry (no failed step)")
        if self.count_attempts(task_id, step.step_id) >= task.max_attempts:
            raise InvalidTaskState(task_id, task.status, f"retry (max_attempts={task.max_attempts} reached)")
        step.status = "PENDING"
        step.finished_at = None
        task.status = "PENDING"
        task.finished_at = None
        task.timeout_at = None
        self._record(task_id, "task.retry_requested", step_id=step.step_id)
        self.db.commit()
        return {"task_id": task_id, "status": task.status}

    def timeout_running(self, task: Task) -> dict:
        """Server-side watchdog: a live task past timeout_at becomes TIMEOUT."""
        now = utcnow()
        if task.status in ("SENT", "ACCEPTED", "RUNNING"):
            attempt = None
            for step in self.get_steps(task.task_id):
                if step.status in ("RUNNING", "PENDING"):
                    open_attempt = self.latest_open_attempt(task.task_id, step.step_id)
                    if open_attempt is not None and open_attempt.status not in TERMINAL_TASK_STATES:
                        attempt = open_attempt
                        break
            if attempt is not None:
                attempt.status = "TIMEOUT"
                attempt.finished_at = now
                attempt.error_code = "EXECUTOR_TIMEOUT"
                step = self.db.scalars(select(TaskStep).where(TaskStep.step_id == attempt.step_id)).first()
                if step is not None and step.status == "RUNNING":
                    step.status = "TIMEOUT"
                    step.finished_at = now
            task.status = "TIMEOUT"
            task.finished_at = now
            self._record(task.task_id, "task.timeout", payload={"reason": "timeout_at elapsed"})
            self.db.commit()
        return {"task_id": task.task_id, "status": task.status, "notify_device": True}

    def timeout_pending(self, task: Task) -> dict:
        """Device stayed offline longer than the max wait: PENDING -> TIMEOUT."""
        if task.status == "PENDING":
            task.status = "TIMEOUT"
            task.finished_at = utcnow()
            self._record(task.task_id, "task.timeout", payload={"reason": "device_offline_max_wait"})
            self.db.commit()
        return {"task_id": task.task_id, "status": task.status, "notify_device": False}

    # ------------------------------------------------------------------ helpers

    def _record(self, task_id: str, event_type: str, step_id: str | None = None, attempt_id: str | None = None, payload: dict | None = None) -> None:
        self.db.add(TaskEvent(task_id=task_id, step_id=step_id, attempt_id=attempt_id, event_type=event_type, payload=payload or {}))
