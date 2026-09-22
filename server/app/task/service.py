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

# V1.6 P0 0.14 (§3.7): worker-reported failures eligible for automatic
# re-dispatch of READ-risk capabilities - the data plane failed before or
# during a read-only run, so re-running cannot double-apply a write.
AUTO_RETRY_ERROR_CODES = {
    "ARTIFACT_CHECKSUM_MISMATCH",  # re-download can fix corrupted bytes
    "ARTIFACT_DOWNLOAD_FAILED",
    "ARTIFACT_DOWNLOAD_STALLED",
    "ARTIFACT_DOWNLOAD_TIMEOUT",
}


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
            pending_since=utcnow(),
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

        # V1.5 §15: validate input artifact references exist (blob health is
        # re-checked by the Worker at download time - ARTIFACT_NOT_FOUND there).
        input_artifacts: list[dict] = []
        from app.artifact.service import ArtifactNotFound, ArtifactService

        artifact_service = ArtifactService(self.db)
        for ref in payload.input_artifacts:
            try:
                artifact_service.get_artifact(ref.artifact_id)
            except ArtifactNotFound:
                problems.append(f"input artifact not found: {ref.artifact_id} (role {ref.role})")
                continue
            input_artifacts.append({"artifact_id": ref.artifact_id, "role": ref.role})
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
            # V1.6 P0 0.13: pin package identity on the Task row at creation -
            # dispatch envelopes may only echo these values, never re-resolve.
            package_id=published.package_id,
            package_checksum=published.checksum,
            artifact_ids=input_artifacts,
            timeout_seconds=payload.timeout_seconds,
            pending_since=utcnow(),
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
        # V1.6 0.14: failure traits carried into the auto-retry policy.
        worker_retryable = False
        failure_error_code: str | None = None

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
            # Phase 8: monotonic progress - the attempt stores the LATEST
            # snapshot; a stale/reordered event (seq <= stored) never moves it
            # backwards. Legacy workers send no seq: payload-only updates.
            seq = data.get("seq")
            current_seq = attempt.progress_seq if attempt is not None else None
            if attempt is not None and (seq is None or current_seq is None or seq > current_seq):
                attempt.progress_seq = seq if seq is not None else (current_seq or 0)
                attempt.progress_json = {
                    "progress": data.get("progress"),
                    "message": data.get("message"),
                    "seq": seq if seq is not None else current_seq or 0,
                    "updated_at": now.isoformat(),
                }
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
                worker_retryable = bool(error.get("retryable"))
                failure_error_code = error_code
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
            # V1.6 P0 0.14: risk-aware auto retry AFTER the terminal fact is
            # recorded and notified - the failure is real, the retry is policy.
            if task.status == "FAILED":
                self.maybe_auto_retry(
                    task_id, error_code=failure_error_code, worker_retryable=worker_retryable
                )
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
        # Phase 3: CANCELLED is terminal - the notification layer must see it.
        notify_task_terminal(task_id)
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
        # Phase 2: fresh offline-max-wait window for the new PENDING stint.
        task.pending_since = utcnow()
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

    def timeout_accepted_stalled(self, task: Task) -> dict:
        """V1.6 P0 0.1 (audit H3): an attempt accepted but never running within
        the liveness window is converged to TIMEOUT (ACCEPTED_STALLED) instead
        of sitting until timeout_at. The worker enqueued the dispatch but never
        started it - nothing executed, so closing is safe even for WRITE/ACTION
        capabilities; retry re-enters PENDING normally.

        CAS discipline: the task is closed only from ACCEPTED (a task that just
        transitioned to RUNNING is left alone), and the attempt likewise."""
        now = utcnow()
        updated_task = (
            self.db.query(Task)
            .filter(Task.task_id == task.task_id, Task.status == "ACCEPTED")
            .update({"status": "TIMEOUT", "finished_at": now}, synchronize_session=False)
        )
        attempt_id = step_id = None
        if updated_task:
            for step in self.get_steps(task.task_id):
                open_attempt = self.latest_open_attempt(task.task_id, step.step_id)
                if open_attempt is None or open_attempt.status != "ACCEPTED":
                    continue
                updated_attempt = (
                    self.db.query(TaskAttempt)
                    .filter(TaskAttempt.attempt_id == open_attempt.attempt_id,
                            TaskAttempt.status == "ACCEPTED")
                    .update(
                        {
                            "status": "TIMEOUT",
                            "finished_at": now,
                            "error_code": "ACCEPTED_STALLED",
                            "error_message": (
                                "worker accepted but never reported running "
                                "within the liveness window"
                            ),
                        },
                        synchronize_session=False,
                    )
                )
                if updated_attempt:
                    attempt_id, step_id = open_attempt.attempt_id, open_attempt.step_id
                    break
            self._record(
                task.task_id, "task.timeout", step_id=step_id, attempt_id=attempt_id,
                payload={"reason": "accepted_stalled"},
            )
        self.db.commit()
        if updated_task:
            notify_task_terminal(task.task_id)  # TIMEOUT is terminal
        return {
            "task_id": task.task_id,
            "status": "TIMEOUT" if updated_task else task.status,
            "notify_device": bool(updated_task),
            "attempt_id": attempt_id,
            "step_id": step_id,
        }

    def _current_attempt_ids(self, task_id: str) -> tuple[str | None, str | None]:
        """(attempt_id, step_id) of the live attempt a cancel envelope should
        address (V1.1 §16: cancel is bound to the current attempt)."""
        for step in self.get_steps(task_id):
            open_attempt = self.latest_open_attempt(task_id, step.step_id)
            if open_attempt is not None and open_attempt.status in LIVE_ATTEMPT_STATES:
                return open_attempt.attempt_id, step.step_id
        return None, None

    # ------------------------------------------------------------- V1.6 0.14

    def _capability_risk_level(self, task: Task) -> str | None:
        """risk_level of the pinned capability (READ/WRITE/ACTION); None for
        legacy commands (conservative: no auto-retry without a risk owner)."""
        if task.execution_type != "CAPABILITY" or not task.capability_name:
            return None
        from app.capability_runtime.service import CapabilityService as RuntimeCapabilityService

        row = RuntimeCapabilityService(self.db).get_capability(task.capability_name)
        return row.risk_level if row is not None else None

    def maybe_auto_retry(
        self,
        task_id: str,
        *,
        error_code: str | None = None,
        worker_retryable: bool = False,
        sent_unaccepted: bool = False,
    ) -> bool:
        """V1.6 P0 0.14 (§3.7): risk-aware automatic re-dispatch.

        - READ capabilities may automatically enter a new attempt when the
          failure is a transient data-plane error (AUTO_RETRY_ERROR_CODES),
          the worker explicitly marked the failure retryable, or the task
          died SENT-unaccepted on a flapping link (sent_unaccepted=True).
        - WRITE/ACTION NEVER blind-retry: the write may already have landed
          on the target; the operator decides (worker retryable=true cannot
          override the risk level).
        - max_attempts still bounds every retry (request_retry enforces).

        Returns True when a new attempt was queued."""
        import logging

        task = self.get(task_id)
        if task.status not in RETRYABLE_TASK_STATES:
            return False
        risk = self._capability_risk_level(task)
        if risk != "READ":
            return False
        if not sent_unaccepted and error_code not in AUTO_RETRY_ERROR_CODES and not worker_retryable:
            return False
        try:
            self.request_retry(task_id)
        except Exception as exc:  # InvalidTaskState: attempts exhausted etc.
            logging.getLogger(__name__).info("auto-retry declined for %s: %s", task_id, exc)
            return False
        self._record(
            task_id, "task.auto_retried",
            payload={
                "error_code": error_code,
                "worker_retryable": worker_retryable,
                "sent_unaccepted": sent_unaccepted,
                "risk_level": risk,
            },
        )
        self.db.commit()
        logging.getLogger(__name__).warning("READ task %s auto-retried (%s)", task_id, error_code or "sent_unaccepted")
        return True

    def reconcile_terminal_attempts(self, resend_window_seconds: int) -> list[dict]:
        """Phase 2 reconciliation: an attempt may never outlive its task.

        Called every monitor sweep. Three situations are repaired here:

        1. Superseded attempt (step.current_attempt_id points elsewhere):
           closed STALE immediately - it is no longer the execution context.
        2. Terminal task + live attempt, resend window NOT elapsed: the device
           gets a (re-)sent stop instruction - the original cancel/timeout may
           have been undeliverable (device offline). The caller dedupes sends.
        3. Terminal task + live attempt past the resend window: the device is
           unreachable - the attempt is closed STALE so neither the DB nor the
           dashboard shows a RUNNING attempt under a terminal task forever.

        Returns the (re-)send actions for case 2:
        [{task_id, attempt_id, step_id, device_id}]."""
        now = utcnow()
        actions: list[dict] = []
        live_attempts = list(
            self.db.scalars(
                select(TaskAttempt).where(TaskAttempt.status.in_(LIVE_ATTEMPT_STATES))
            )
        )
        for attempt in live_attempts:
            task = self.db.scalars(
                select(Task).where(Task.task_id == attempt.task_id)
            ).first()
            if task is None:
                continue
            step = self.db.scalars(
                select(TaskStep).where(TaskStep.step_id == attempt.step_id)
            ).first()

            # 1. superseded by a newer attempt on the same step
            if step is not None and step.current_attempt_id not in (None, attempt.attempt_id):
                attempt.status = "STALE"
                attempt.finished_at = now
                self._record(
                    attempt.task_id, "task.attempt_reconciled",
                    step_id=attempt.step_id, attempt_id=attempt.attempt_id,
                    payload={"reason": "superseded"},
                )
                continue

            if task.status not in TERMINAL_TASK_STATES:
                continue

            # 2/3. terminal task with a live attempt
            finished_at = task.finished_at or now
            elapsed = (now - finished_at).total_seconds()
            if elapsed <= resend_window_seconds:
                actions.append(
                    {
                        "task_id": attempt.task_id,
                        "attempt_id": attempt.attempt_id,
                        "step_id": attempt.step_id,
                        "device_id": attempt.device_id,
                    }
                )
            else:
                attempt.status = "STALE"
                attempt.finished_at = now
                attempt.error_code = attempt.error_code or "ATTEMPT_STALE"
                attempt.error_message = (
                    f"attempt outlived its {task.status} task; stop delivery unconfirmed"
                )
                self._record(
                    attempt.task_id, "task.attempt_reconciled",
                    step_id=attempt.step_id, attempt_id=attempt.attempt_id,
                    payload={"reason": "terminal_task", "task_status": task.status},
                )
        self.db.commit()
        return actions

    # ------------------------------------------------------------------ helpers

    def _record(self, task_id: str, event_type: str, step_id: str | None = None, attempt_id: str | None = None, payload: dict | None = None) -> None:
        self.db.add(TaskEvent(task_id=task_id, step_id=step_id, attempt_id=attempt_id, event_type=event_type, payload=payload or {}))
