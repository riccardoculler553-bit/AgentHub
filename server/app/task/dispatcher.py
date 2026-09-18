"""TaskDispatcher: Task -> Device delivery.

Only dispatches. Execution belongs to the Worker; result bookkeeping to the
TaskService. Transport ACK (message_ack) is implied by a successful hub send;
Task ACK (task.accept) arrives later as a device envelope.
"""

import logging
from datetime import timedelta

from sqlalchemy import select

from app.capability.service import CapabilityService
from app.command.service import CommandService
from app.db.database import SessionLocal
from app.db.models import utcnow
from app.task.db_models import Task, TaskAttempt, TaskStep
from app.task.device_link import DeviceLinkService
from app.task.service import LIVE_TASK_STATES, TaskService
from app.websocket.protocol import Envelope, MessageType, new_message_id

logger = logging.getLogger(__name__)


class TaskDispatcher:
    def __init__(self, hub) -> None:
        self.device_link = DeviceLinkService(hub)

    async def dispatch_task(self, task_id: str) -> bool:
        """Dispatch the next pending step of a PENDING task. Returns True when
        the task envelope hit at least one live connection.

        V1.1 §10: the PENDING -> DISPATCHING claim is a single conditional
        UPDATE. When several dispatchers race (Monitor / MVP agent / API),
        exactly one wins the CAS and creates the attempt; the losers see 0
        affected rows and return False - duplicate attempts are impossible."""
        with SessionLocal() as db:
            service = TaskService(db)
            try:
                service.get(task_id)  # 404 fast-path
            except Exception:
                return False

            # ---- CAS claim (atomic): 1 row = this dispatcher owns the task
            claimed = (
                db.query(Task)
                .filter(Task.task_id == task_id, Task.status == "PENDING")
                .update({"status": "DISPATCHING"}, synchronize_session=False)
            )
            db.commit()
            if claimed != 1:
                return False
            db.expire_all()  # re-read the freshly claimed task
            task = service.get(task_id)

            step = service.next_pending_step(task_id)
            if step is None:
                # Every step already terminal (raced a final result while we
                # held the claim): close the task instead of leaving it stuck.
                task.status = "FAILED"
                task.finished_at = utcnow()
                service._record(task_id, "task.failed", payload={"error_code": "NO_PENDING_STEP", "error_message": "no PENDING step to dispatch"})
                db.commit()
                return False

            # ---- V1.4 Capability Runtime branch (§21/§51/§65): resolves its
            # own worker, so it must run BEFORE the legacy device check.
            if task.execution_type == "CAPABILITY":
                return await self._dispatch_capability(db, service, task, step)

            if task.target_device_id is None:
                # Defensive: create() enforces a target device in V1.0.
                task.status = "FAILED"
                task.finished_at = utcnow()
                service._record(task_id, "task.failed", payload={"error_code": "TASK_VALIDATION", "error_message": "target_device_id is required"})
                db.commit()
                return False

            device_id = task.target_device_id

            if not self.device_link.is_online(device_id):
                # Stay dispatchable: revert to PENDING, the monitor retries
                # while the task is within its offline max wait.
                task.status = "PENDING"
                task.pending_since = utcnow()
                db.commit()
                return False

            # Device lock (PDF §81): only one live task per device. The MVP
            # agent checks this before creating the task; this is the race
            # backstop. DEVICE_BUSY fails the task instead of queueing (§80).
            busy = db.scalars(
                select(Task.task_id).where(
                    Task.target_device_id == device_id,
                    Task.status.in_(LIVE_TASK_STATES),
                    Task.task_id != task_id,
                )
            ).first()
            if busy is not None:
                task.status = "FAILED"
                task.finished_at = utcnow()
                service._record(
                    task_id, "task.failed", step_id=step.step_id,
                    payload={"error_code": "DEVICE_BUSY", "error_message": f"device busy with task {busy}"},
                )
                db.commit()
                return False

            if not CapabilityService(db).has_capability(device_id, step.command):
                task.status = "FAILED"
                task.finished_at = utcnow()
                service._record(
                    task_id, "task.failed", step_id=step.step_id,
                    payload={"error_code": "DEVICE_CAPABILITY_MISSING", "error_message": step.command},
                )
                db.commit()
                return False

            command = CommandService(db).get_command(step.command)

            attempt = TaskAttempt(
                attempt_id=f"attempt_{new_message_id('a')[2:]}",
                task_id=task_id,
                step_id=step.step_id,
                device_id=device_id,
                attempt_no=service.count_attempts(task_id, step.step_id) + 1,
                status="DISPATCHING",
            )
            db.add(attempt)
            # V1.1 §7: this attempt becomes the step's current context; events
            # naming any other attempt are recorded as stale but never applied.
            step.current_attempt_id = attempt.attempt_id
            service._record(task_id, "task.dispatching", step_id=step.step_id, attempt_id=attempt.attempt_id)
            db.commit()

            envelope = Envelope(
                id=new_message_id(),
                type=MessageType.TASK_DISPATCH,
                data={
                    "task_id": task_id,
                    "step_id": step.step_id,
                    "attempt_id": attempt.attempt_id,
                    "command": step.command,
                    "params": step.params,
                    "timeout": int(command.timeout),
                },
            )
            sent = await self.device_link.send_task(device_id, envelope)
            if sent == 0:
                # Race with disconnect: roll back to PENDING, monitor retries.
                db.delete(attempt)
                task.status = "PENDING"
                task.pending_since = utcnow()
                step.current_attempt_id = None
                service._record(task_id, "task.dispatch_failed", step_id=step.step_id, payload={"reason": "device_offline"})
                db.commit()
                return False

            attempt.status = "SENT"
            attempt.dispatch_message_id = envelope.id
            task.status = "SENT"
            task.timeout_at = utcnow() + timedelta(seconds=int(command.timeout))
            # V1.1 §14: the timeout is bound to THIS attempt, not just the task.
            attempt.timeout_at = task.timeout_at
            service._record(
                task_id, "task.sent", step_id=step.step_id, attempt_id=attempt.attempt_id,
                payload={"message_id": envelope.id, "connections": sent},
            )
            db.commit()
            logger.info("task %s step %s dispatched (attempt %s, %s connection(s))", task_id, step.step_id, attempt.attempt_no, sent)
            return True

    # ------------------------------------------------------------- capability

    async def _dispatch_capability(self, db, service, task: Task, step: TaskStep) -> bool:
        """Dispatch a CAPABILITY task (V1.4 §21/§51/§65).

        Server resolves capability -> version -> worker (§21), then hands the
        execution to the worker with package identity so Lazy Pull can verify
        checksums. Retry/timeout/cancel stay with the Task Engine (§67/§69)."""
        from app.capability_runtime.errors import CapabilityError, CapabilityNoWorker
        from app.capability_runtime.package_service import PackageService
        from app.capability_runtime.resolver import CapabilityResolver
        from app.capability_runtime.service import CapabilityService
        from app.core.config import settings

        task_id = task.task_id
        try:
            capability_service = CapabilityService(db)
            version = capability_service.get_published_version(
                task.capability_name, task.capability_version
            )
        except CapabilityError as exc:
            task.status = "FAILED"
            task.finished_at = utcnow()
            service._record(
                task_id, "task.failed",
                payload={"error_code": "CAPABILITY_UNAVAILABLE", "error_message": str(exc)[:500]},
            )
            db.commit()
            logger.warning("capability task %s failed to resolve: %s", task_id, exc)
            return False

        try:
            device_id = CapabilityResolver(db).resolve_worker(
                task.capability_name, version.version, task.target_device_id
            )
        except CapabilityNoWorker:
            # Transient: keep dispatchable, monitor sweeps retry within the
            # offline max-wait window (same policy as legacy offline tasks).
            task.status = "PENDING"
            task.pending_since = utcnow()
            db.commit()
            logger.info("capability task %s has no online worker yet; stays PENDING", task_id)
            return False

        # Device lock (PDF §81) applies to capability executions too.
        busy = db.scalars(
            select(Task.task_id).where(
                Task.target_device_id == device_id,
                Task.status.in_(LIVE_TASK_STATES),
                Task.task_id != task_id,
            )
        ).first()
        if busy is not None:
            task.status = "PENDING"
            task.pending_since = utcnow()
            db.commit()
            logger.info("capability task %s deferred: worker %s busy with %s", task_id, device_id, busy)
            return False

        if not self.device_link.is_online(device_id):
            task.status = "PENDING"
            task.pending_since = utcnow()
            db.commit()
            return False

        package = PackageService(db).get_package(version.package_id)
        # V1.5: per-task timeout override; None falls back to the global default
        timeout = task.timeout_seconds or settings.capability_default_timeout

        # V1.5 §15/§26: resolve input artifact references into dispatch data.
        # The Worker downloads over HTTP and verifies each checksum (§54); a
        # missing artifact row is permanent -> fail the task here, not loop.
        from app.artifact.service import ArtifactNotFound, ArtifactService

        input_artifacts: list[dict] = []
        for ref in task.artifact_ids or []:
            if isinstance(ref, str):  # defensive: legacy plain-id entries
                ref = {"artifact_id": ref, "role": "input"}
            try:
                artifact_row = ArtifactService(db).get_artifact(str(ref.get("artifact_id", "")))
            except ArtifactNotFound:
                task.status = "FAILED"
                task.finished_at = utcnow()
                service._record(
                    task_id, "task.failed",
                    payload={
                        "error_code": "ARTIFACT_NOT_FOUND",
                        "error_message": f"input artifact missing: {ref}",
                    },
                )
                db.commit()
                logger.warning("capability task %s failed: input artifact missing", task_id)
                return False
            input_artifacts.append(
                {
                    "artifact_id": str(ref.get("artifact_id", "")),
                    "name": artifact_row.name,
                    "checksum": artifact_row.checksum,
                    "role": str(ref.get("role") or "input"),
                }
            )

        attempt = TaskAttempt(
            attempt_id=f"attempt_{new_message_id('a')[2:]}",
            task_id=task.task_id,
            step_id=step.step_id,
            device_id=device_id,
            attempt_no=service.count_attempts(task_id, step.step_id) + 1,
            status="DISPATCHING",
        )
        db.add(attempt)
        step.current_attempt_id = attempt.attempt_id
        # Persist the resolved worker on the task (§32 worker_id mapping).
        task.target_device_id = device_id
        service._record(task_id, "task.dispatching", step_id=step.step_id, attempt_id=attempt.attempt_id)
        db.commit()

        envelope = Envelope(
            id=new_message_id(),
            type=MessageType.CAPABILITY_EXECUTE,
            data={
                "task_id": task_id,
                "step_id": step.step_id,
                "attempt_id": attempt.attempt_id,
                "execution_id": attempt.attempt_id,
                "capability": task.capability_name,
                "version": version.version,
                "params": step.params,
                "timeout": timeout,
                "package_id": package.package_id,
                "checksum": package.checksum,
                "input_artifacts": input_artifacts,
                "workflow_run_id": task.workflow_run_id,
                "step_run_id": task.workflow_step_run_id,
            },
        )
        sent = await self.device_link.send_task(device_id, envelope)
        if sent == 0:
            db.delete(attempt)
            task.status = "PENDING"
            task.pending_since = utcnow()
            step.current_attempt_id = None
            service._record(task_id, "task.dispatch_failed", step_id=step.step_id, payload={"reason": "worker_offline"})
            db.commit()
            return False

        attempt.status = "SENT"
        attempt.dispatch_message_id = envelope.id
        task.status = "SENT"
        task.timeout_at = utcnow() + timedelta(seconds=timeout)
        attempt.timeout_at = task.timeout_at
        service._record(
            task_id, "task.sent", step_id=step.step_id, attempt_id=attempt.attempt_id,
            payload={
                "message_id": envelope.id, "connections": sent,
                "capability": task.capability_name, "capability_version": version.version,
                "package_id": package.package_id,
            },
        )
        db.commit()
        logger.info(
            "capability task %s step %s dispatched %s@%s -> worker %s (attempt %s)",
            task_id, step.step_id, task.capability_name, version.version, device_id, attempt.attempt_no,
        )
        return True
