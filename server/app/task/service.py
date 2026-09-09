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
from app.task.events import notify_task_terminal
from app.task.models import TaskCreateIn
from app.task.state import ATTEMPT_TERMINAL_STATES, TASK_TERMINAL_STATES, can_transition
from app.task.waiters import task_waiters

# Task statuses that mean "a dispatch may be flying / worker may still be alive"
LIVE_TASK_STATES = {"DISPATCHING", "SENT", "ACCEPTED", "RUNNING"}
LIVE_ATTEMPT_STATES = {"DISPATCHING", "SENT", "ACCEPTED", "RUNNING"}
CANCELABLE_TASK_STATES = {"PENDING", "DISPATCHING", "SENT", "ACCEPTED", "RUNNING"}
RETRYABLE_TASK_STATES = {"FAILED", "TIMEOUT"}
# Backwards-compatible alias (state.py is the source of truth in V1.1).
TERMINAL_TASK_STATES = TASK_TERMINAL_STATES


def _new_id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


class TaskService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ------------------------------------------------------------------ create

    def create(self, payload: TaskCreateIn, created_by: str = "admin") -> Task:
        """Validate the ExecutionPlan against Registry/Validator, then persist
        Task + Steps + first event. Task starts in PENDING.

        V1.4 §32/§73: two execution types share the same Task Engine.
        - LEGACY_COMMAND: V1.0-V1.3 behavior (device + command registry).
        - CAPABILITY: the step command is an automation capability name; the
          device may be omitted and gets resolved by the CapabilityResolver at
          dispatch time (Lazy Pull installs it on the worker, §52)."""
        execution_type = payload.execution_type or "LEGACY_COMMAND"
        if execution_type == "CAPABILITY":
            return self._create_capability_task(payload, created_by)
        return self._create_command_task(payload, created_by)

    def _create_command_task(self, payload: TaskCreateIn, created_by: str) -> Task:
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
            # V1.3 provenance (§32/§33): AGENT tools / WORKFLOW engine mark it;
            # API stays the default.
            source_type=payload.source_type or ("AGENT" if created_by == "tool_agent" else "API"),
            workflow_run_id=payload.workflow_run_id,
            workflow_step_run_id=payload.workflow_step_run_id,
            execution_type="LEGACY_COMMAND",
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

    def _create_capability_task(self, payload: TaskCreateIn, created_by: str) -> Task:
        """V1.4 CAPABILITY task (§21/§32): exactly one step whose command is
        the capability name. Validation covers capability existence + a
        PUBLISHED version; the worker is resolved at dispatch time."""
        from app.capability_runtime.errors import CapabilityError
        from app.capability_runtime.service import CapabilityService

        problems: list[str] = []
        if len(payload.steps) != 1:
            problems.append("a CAPABILITY task takes exactly one capability step")
            capability_step = None
        else:
            capability_step = payload.steps[0]

        capability = None
        if capability_step is not None:
            try:
                service = CapabilityService(self.db)
                capability = service.require_capability(capability_step.command)
                if not capability.enabled:
                    from app.capability_runtime.errors import CapabilityDisabled

                    raise CapabilityDisabled(capability.name)
                published = service.get_published_version(capability.name, payload.capability_version)
                # Pin the concrete version that will run (§56 reproducibility).
                payload.capability_version = published.version
            except CapabilityError as exc:
                problems.append(f"step 1: {exc}")

        device = None
        if payload.target_device_id is not None:
            try:
                device = DeviceService(self.db).get_device(payload.target_device_id)
            except DeviceNotFound:
                problems.append(f"target device not found: {payload.target_device_id}")
            else:
                if device.revoked_at is not None:
                    problems.append(f"target device is revoked: {payload.target_device_id}")
        if problems or capability_step is None:
            raise TaskValidationFailed(problems or ["capability step missing"])

        task = Task(
            task_id=_new_id("task"),
            name=payload.name or capability_step.command,
            created_by=created_by,
            target_device_id=device.device_id if device else None,
            status="PENDING",
            max_attempts=settings.task_max_attempts,
            source_type=payload.source_type or ("AGENT" if created_by == "tool_agent" else "API"),
            workflow_run_id=payload.workflow_run_id,
            workflow_step_run_id=payload.workflow_step_run_id,
            execution_type="CAPABILITY",
            capability_name=capability.name,
            capability_version=payload.capability_version,
            artifact_ids=[],
        )
        self.db.add(task)
        self.db.add(
            TaskStep(
                step_id=_new_id("step"),
                task_id=task.task_id,
                order_no=1,
                device_id=device.device_id if device else None,
                command=capability_step.command,
                params=capability_step.params,
                status="PENDING",
            )
        )
        self.db.flush()
        self._record(
            task.task_id, "task.created",
            payload={
                "name": task.name, "steps": 1, "execution_type": "CAPABILITY",
                "capability": task.capability_name, "capability_version": task.capability_version,
            },
        )
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
        """Apply task.accept/running/progress/result from a Worker (V1.1 §8/§33).

        Event gate order:
        1. resolve Task / Step / Attempt
        2. security: a device may only report attempts dispatched to it
        3. stale gate: an event naming a non-current attempt is AUDIT ONLY
           (recorded with attempt_id, never mutates Task/Step/Attempt state)
        4. terminal gate: a terminal Task can never be reopened by an event
        5. state machine gate: only legal transitions are applied
        6. record event (always, with attempt_id) -> commit -> wake waiters

        Returns {"task_id", "advance"} where advance=True means the next step
        should be dispatched (the caller triggers the Dispatcher)."""
        task_id = str(data.get("task_id", ""))
        step_id = str(data.get("step_id", ""))
        attempt_id = str(data.get("attempt_id", "")) or None
        task = self.get(task_id)
        step = self.db.scalars(
            select(TaskStep).where(TaskStep.step_id == step_id, TaskStep.task_id == task_id)
        ).first()
        if step is None:
            raise TaskNotFound(task_id)

        attempt = None
        if attempt_id:
            # Exact attempt matching (idempotency + late results).
            attempt = self.db.scalars(
                select(TaskAttempt).where(
                    TaskAttempt.attempt_id == attempt_id, TaskAttempt.task_id == task_id
                )
            ).first()
        if attempt is None:
            # Legacy senders without attempt_id keep the latest-open fallback.
            attempt = self.latest_open_attempt(task_id, step_id)
        if attempt is not None and attempt.device_id not in (None, device_id):
            # A device may only report its own attempts (security boundary).
            return {"task_id": task_id, "advance": False}

        now = utcnow()
        advance = False

        # --- stale gate (V1.1 §7/§8): report against an old attempt = audit only
        if attempt_id is not None and step.current_attempt_id != attempt_id:
            self._record(
                task_id, "task.late_event", step_id=step_id, attempt_id=attempt_id,
                payload={"event": msg_type, "reason": "stale_attempt",
                         "current_attempt_id": step.current_attempt_id},
            )
            self.db.commit()
            return {"task_id": task_id, "advance": False}

        # --- terminal gate (V1.1 §6): terminal tasks are never reopened
        if task.status in TERMINAL_TASK_STATES:
            payload = {"event": msg_type, "reason": "task_terminal", "task_status": task.status}
            if msg_type == "task.result":
                payload["result_status"] = str(data.get("status", "failed")).lower()
                # The attempt keeps its factual outcome when the transition is
                # legal (RUNNING -> SUCCESS); an already-terminal attempt
                # (e.g. TIMEOUT) is never flipped (state machine rejects it).
                if attempt is not None and self._apply_attempt_result(attempt, data, now):
                    payload["attempt_status"] = attempt.status
            self._record(task_id, "task.late_result", step_id=step_id, attempt_id=attempt_id, payload=payload)
            self.db.commit()
            return {"task_id": task_id, "advance": False}

        if msg_type == "task.accept":
            if attempt is not None and can_transition("attempt", attempt.status, "ACCEPTED"):
                attempt.status = "ACCEPTED"
                attempt.accepted_at = now
            if can_transition("task", task.status, "ACCEPTED"):
                task.status = "ACCEPTED"
            self._record(task_id, "task.accepted", step_id=step_id, attempt_id=attempt_id)

        elif msg_type == "task.running":
            if attempt is not None and can_transition("attempt", attempt.status, "RUNNING"):
                attempt.status = "RUNNING"
                attempt.started_at = now
            if can_transition("task", task.status, "RUNNING"):
                task.status = "RUNNING"
                task.started_at = now
            if step.status == "PENDING":
                step.status = "RUNNING"
                step.started_at = now
            self._record(task_id, "task.running", step_id=step_id, attempt_id=attempt_id)

        elif msg_type == "task.progress":
            self._record(
                task_id,
                "task.progress",
                step_id=step_id,
                attempt_id=attempt_id,
                payload={"progress": data.get("progress"), "message": data.get("message")},
            )

        elif msg_type == "task.result":
            result_status = str(data.get("status", "failed")).lower()
            error = data.get("error") or {}
            result = data.get("result") or {}
            if result_status == "success":
                self._apply_attempt_result(attempt, data, now)
                step.status = "SUCCESS"
                step.finished_at = now
                # autoflush=False: make the just-closed step visible to the
                # next_pending_step query below (SQLite/MySQL alike).
                self.db.flush()
                if self.next_pending_step(task_id) is not None:
                    advance = True
                    if can_transition("task", task.status, "RUNNING"):
                        task.status = "RUNNING"
                    self._record(task_id, "task.progress", step_id=step_id, attempt_id=attempt_id,
                                 payload={"step_result": result, "step_status": "success"})
                else:
                    if can_transition("task", task.status, "SUCCESS"):
                        task.status = "SUCCESS"
                        task.finished_at = now
                        # V1.4 §45: collect artifact references from the final
                        # capability result so the Task row is self-contained.
                        artifact_refs = [
                            str(a.get("artifact_id"))
                            for a in (result.get("artifacts") or [])
                            if isinstance(a, dict) and a.get("artifact_id")
                        ]
                        if artifact_refs:
                            task.artifact_ids = artifact_refs
                        self._record(task_id, "task.success", step_id=step_id, attempt_id=attempt_id, payload={"result": result})
                    else:
                        # Illegal (e.g. task CANCELLED mid-flight): audit only.
                        self._record(task_id, "task.late_result", step_id=step_id, attempt_id=attempt_id,
                                     payload={"reason": "illegal_transition", "task_status": task.status})
            elif result_status == "cancelled":
                if attempt is not None and can_transition("attempt", attempt.status, "CANCELLED"):
                    attempt.status = "CANCELLED"
                    attempt.finished_at = now
                if step.status not in ("SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"):
                    step.status = "CANCELLED"
                    step.finished_at = now
                if can_transition("task", task.status, "CANCELLED"):
                    task.status = "CANCELLED"
                    task.finished_at = now
                self._record(task_id, "task.cancelled", step_id=step_id, attempt_id=attempt_id, payload={"by": "worker"})
            else:  # failed
                error_code = str(error.get("code", "EXECUTOR_FAILED"))[:64]
                error_message = str(error.get("message", ""))[:500]
                if attempt is not None and can_transition("attempt", attempt.status, "FAILED"):
                    attempt.status = "FAILED"
                    attempt.finished_at = now
                    attempt.error_code = error_code
                    attempt.error_message = error_message
                if step.status not in ("SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"):
                    step.status = "FAILED"
                    step.finished_at = now
                if can_transition("task", task.status, "FAILED"):
                    task.status = "FAILED"
                    task.finished_at = now
                self._record(task_id, "task.failed", step_id=step_id, attempt_id=attempt_id,
                             payload={"error_code": error_code, "error_message": error_message})
        else:
            return {"task_id": task_id, "advance": False}

        self.db.commit()
        if msg_type == "task.result" and task.status in TERMINAL_TASK_STATES:
            # Wake in-process waiters (MVP agent) - DB polling stays as fallback.
            task_waiters.notify(task_id)
            # V1.3 §121: workflow advancement hooks the same terminal fact.
            notify_task_terminal(task_id)
        return {"task_id": task_id, "advance": advance}

    def _apply_attempt_result(self, attempt: TaskAttempt | None, data: dict, now) -> bool:
        """Apply the factual result to the attempt when the state machine
        allows it. Returns True when the attempt status was changed."""
        if attempt is None:
            return False
        result_status = str(data.get("status", "failed")).lower()
        target = {"success": "SUCCESS", "cancelled": "CANCELLED"}.get(result_status, "FAILED")
        if not can_transition("attempt", attempt.status, target):
            return False
        attempt.status = target
        attempt.finished_at = now
        if target == "FAILED":
            error = data.get("error") or {}
            attempt.error_code = str(error.get("code", "EXECUTOR_FAILED"))[:64]
            attempt.error_message = str(error.get("message", ""))[:500]
        return True

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
        # V1.1: the re-created attempt becomes the new current context.
        step.current_attempt_id = None
        task.status = "PENDING"
        task.finished_at = None
        task.timeout_at = None
        self._record(task_id, "task.retry_requested", step_id=step.step_id)
        self.db.commit()
        return {"task_id": task_id, "status": task.status}

    def timeout_running(self, task: Task) -> dict:
        """Server-side watchdog: a live task past timeout_at becomes TIMEOUT
        (V1.1 §14/§15: conditional updates - whoever wins the CAS closes the
        task; a concurrent task.result then lands in the late-result lane)."""
        now = utcnow()
        attempt_id = step_id = None
        # CAS the attempt: only a live attempt may be closed as TIMEOUT.
        for step in self.get_steps(task.task_id):
            if step.status not in ("RUNNING", "PENDING"):
                continue
            open_attempt = self.latest_open_attempt(task.task_id, step.step_id)
            if open_attempt is None or open_attempt.status not in LIVE_ATTEMPT_STATES:
                continue
            updated = (
                self.db.query(TaskAttempt)
                .filter(
                    TaskAttempt.attempt_id == open_attempt.attempt_id,
                    TaskAttempt.status.in_(LIVE_ATTEMPT_STATES),
                )
                .update(
                    {"status": "TIMEOUT", "finished_at": now, "error_code": "EXECUTOR_TIMEOUT"},
                    synchronize_session=False,
                )
            )
            if updated:
                attempt_id, step_id = open_attempt.attempt_id, open_attempt.step_id
                if step.status == "RUNNING":
                    step.status = "TIMEOUT"
                    step.finished_at = now
                break
        # CAS the task: exactly one watchdog/result path closes it.
        updated_task = (
            self.db.query(Task)
            .filter(Task.task_id == task.task_id, Task.status.in_(LIVE_TASK_STATES))
            .update({"status": "TIMEOUT", "finished_at": now}, synchronize_session=False)
        )
        if updated_task:
            self._record(
                task.task_id, "task.timeout", step_id=step_id, attempt_id=attempt_id,
                payload={"reason": "timeout_at elapsed"},
            )
        self.db.commit()
        if updated_task:
            notify_task_terminal(task.task_id)  # V1.3 §121: TIMEOUT is terminal
        return {
            "task_id": task.task_id,
            "status": "TIMEOUT" if updated_task else task.status,
            "notify_device": bool(updated_task),
            "attempt_id": attempt_id,
            "step_id": step_id,
        }

    def timeout_pending(self, task: Task) -> dict:
        """Device stayed offline longer than the max wait: PENDING -> TIMEOUT."""
        if task.status == "PENDING":
            task.status = "TIMEOUT"
            task.finished_at = utcnow()
            self._record(task.task_id, "task.timeout", payload={"reason": "device_offline_max_wait"})
            self.db.commit()
        return {"task_id": task.task_id, "status": task.status, "notify_device": False}

    def _current_attempt_ids(self, task_id: str) -> tuple[str | None, str | None]:
        """(attempt_id, step_id) of the live attempt a cancel envelope should
        address (V1.1 §16: cancel is bound to the current attempt)."""
        for step in self.get_steps(task_id):
            open_attempt = self.latest_open_attempt(task_id, step.step_id)
            if open_attempt is not None and open_attempt.status in LIVE_ATTEMPT_STATES:
                return open_attempt.attempt_id, step.step_id
        return None, None

    # ------------------------------------------------------------------ helpers

    def _record(self, task_id: str, event_type: str, step_id: str | None = None, attempt_id: str | None = None, payload: dict | None = None) -> None:
        self.db.add(TaskEvent(task_id=task_id, step_id=step_id, attempt_id=attempt_id, event_type=event_type, payload=payload or {}))
