"""TaskMonitor: background loops for dispatch waiting + timeouts.

1. PENDING dispatch loop: re-tries PENDING tasks whenever their device comes
   online; tasks waiting longer than task_offline_max_wait become TIMEOUT.
2. Timeout watchdog: live tasks (SENT/ACCEPTED/RUNNING) past timeout_at become
   TIMEOUT and a best-effort task.cancel is sent to the device.

Deliberately mirrors HeartbeatMonitor: a simple periodic sweep, no external
queue/Redis in V1.0.
"""

import asyncio
import logging
from datetime import timedelta

from sqlalchemy import select

from app.core.config import settings
from app.db.database import SessionLocal
from app.db.models import utcnow
from app.task.db_models import Task
from app.task.dispatcher import TaskDispatcher
from app.task.service import TaskService
from app.websocket.protocol import Envelope, MessageType, new_message_id

logger = logging.getLogger(__name__)


class TaskMonitor:
    def __init__(self, hub, sweep_interval: float = 3.0) -> None:
        self.hub = hub
        self.sweep_interval = sweep_interval
        self.dispatcher = TaskDispatcher(hub)
        # Phase 2: stop instructions already (re-)delivered per
        # (task_id, attempt_id) - one delivery is enough per attempt.
        self._stop_sent: set[tuple[str, str]] = set()

    async def run(self) -> None:
        logger.info(
            "TaskMonitor started (sweep every %ss, offline max wait %ss)",
            self.sweep_interval,
            settings.task_offline_max_wait,
        )
        while True:
            try:
                await self.dispatch_pending()
                await self.timeout_scan()
                await self.accepted_liveness_scan()
                await self.reconcile_stale()
            except Exception:
                logger.exception("task monitor sweep failed")
            await asyncio.sleep(self.sweep_interval)

    async def dispatch_pending(self) -> None:
        """Dispatch PENDING tasks whose device is online; expire stale ones."""
        with SessionLocal() as db:
            svc = TaskService(db)
            now = utcnow()
            max_wait = timedelta(seconds=settings.task_offline_max_wait)
            pending = list(
                db.scalars(select(Task).where(Task.status == "PENDING").order_by(Task.created_at))
            )
            dispatchable: list[str] = []
            for task in pending:
                # Phase 2: the offline-max-wait window restarts every time the
                # task enters PENDING (create/retry/rollback); created_at alone
                # insta-timed-out retried old tasks.
                pending_since = task.pending_since or task.created_at
                if now - pending_since > max_wait:
                    svc.timeout_pending(task)
                    logger.warning("task %s TIMEOUT (device offline max wait elapsed)", task.task_id)
                elif task.execution_type == "CAPABILITY" or (
                    task.target_device_id and self.hub.is_device_online(task.target_device_id)
                ):
                    # V1.4 §65: capability tasks resolve their worker at
                    # dispatch time - a None target just means "no eligible
                    # worker yet"; let the dispatcher's resolver retry.
                    dispatchable.append(task.task_id)
        for task_id in dispatchable:
            await self.dispatcher.dispatch_task(task_id)

    async def timeout_scan(self) -> None:
        with SessionLocal() as db:
            svc = TaskService(db)
            now = utcnow()
            live = list(
                db.scalars(
                    select(Task).where(
                        Task.status.in_(["SENT", "ACCEPTED", "RUNNING"]),
                        Task.timeout_at.is_not(None),
                        Task.timeout_at < now,
                    )
                )
            )
            # Phase 2: a SENT task whose device dropped before accepting gets
            # a bounded wait too (offline max wait), not the full timeout -
            # TIMEOUT is retryable, an ACCEPTED-forever state is not.
            max_wait = timedelta(seconds=settings.task_offline_max_wait)
            sent_offline_timed_out: list[str] = []
            for task in db.scalars(select(Task).where(Task.status == "SENT")):
                if task.target_device_id and not self.hub.is_device_online(task.target_device_id):
                    attempts = svc.get_attempts(task.task_id)
                    last_dispatch = max((a.created_at for a in attempts), default=None)
                    if last_dispatch is not None and now - last_dispatch > max_wait:
                        result = svc.timeout_running(task)
                        if result.get("notify_device") or result.get("status") == "TIMEOUT":
                            sent_offline_timed_out.append(task.task_id)
            expired = [svc.timeout_running(task) for task in live]
        for result in expired:
            if not result.get("notify_device"):
                continue  # lost the CAS: a result path closed the task already
            logger.warning("task %s TIMEOUT (watchdog)", result["task_id"])
            await self._send_cancel(result["task_id"], result.get("attempt_id"), result.get("step_id"))
        # V1.6 P0 0.14 (§3.7): "SENT, never accepted, link died" is an
        # auto-retry case for READ capabilities - nothing executed.
        with SessionLocal() as db:
            svc = TaskService(db)
            for task_id in sent_offline_timed_out:
                svc.maybe_auto_retry(task_id, sent_unaccepted=True)

    async def accepted_liveness_scan(self) -> None:
        """V1.6 P0 0.1: ACCEPTED is not RUNNING (the worker enqueues on accept,
        dequeues later). An attempt that stays ACCEPTED past the liveness
        window without ever reporting task.running is converged to TIMEOUT
        (ACCEPTED_STALLED) instead of sitting until timeout_at - the audit H3
        zombie-queue hole (sub-computer worker restarted mid-queue)."""
        window = timedelta(seconds=settings.task_accepted_liveness)
        results: list[dict] = []
        with SessionLocal() as db:
            svc = TaskService(db)
            now = utcnow()
            for task in db.scalars(select(Task).where(Task.status == "ACCEPTED")):
                stalled = any(
                    attempt.status == "ACCEPTED"
                    and attempt.accepted_at is not None
                    and now - attempt.accepted_at > window
                    for attempt in svc.get_attempts(task.task_id)
                )
                if stalled:
                    results.append(svc.timeout_accepted_stalled(task))
        for result in results:
            if not result.get("notify_device"):
                continue  # lost the CAS: the task is not ACCEPTED anymore
            logger.warning(
                "task %s TIMEOUT (accepted_stalled, attempt %s)",
                result["task_id"],
                result.get("attempt_id"),
            )
            await self._send_cancel(result["task_id"], result.get("attempt_id"), result.get("step_id"))

    async def notify_cancel(self, task_id: str) -> None:
        """Best-effort task.cancel delivery for an admin-initiated cancel.
        The envelope addresses the CURRENT attempt (V1.1 §16) so a cancel for
        an old attempt can never kill a newer execution."""
        with SessionLocal() as db:
            attempt_id, step_id = TaskService(db)._current_attempt_ids(task_id)
        await self._send_cancel(task_id, attempt_id, step_id)

    async def reconcile_stale(self) -> None:
        """Phase 2: attempts may never outlive their task (§17 error recovery).

        Re-delivers stop instructions to devices that missed the original
        cancel/timeout (bounded by task_cancel_resend_window), then closes the
        attempt STALE when the device stays unreachable."""
        with SessionLocal() as db:
            actions = TaskService(db).reconcile_terminal_attempts(
                settings.task_cancel_resend_window
            )
        for action in actions:
            key = (action["task_id"], action["attempt_id"])
            if key in self._stop_sent:
                continue
            sent = await self._send_cancel(
                action["task_id"], action["attempt_id"], action["step_id"]
            )
            if sent:
                self._stop_sent.add(key)
                logger.warning(
                    "re-sent stop for terminal task %s (attempt %s)", action["task_id"], action["attempt_id"]
                )

    async def _send_cancel(self, task_id: str, attempt_id: str | None, step_id: str | None) -> int:
        task_row = await self._load(task_id)
        if not task_row or not task_row.target_device_id:
            return 0
        data: dict = {"task_id": task_id}
        if step_id:
            data["step_id"] = step_id
        if attempt_id:
            data["attempt_id"] = attempt_id
        sent = await self.hub.send_to_device(
            task_row.target_device_id,
            Envelope(id=new_message_id(), type=MessageType.TASK_CANCEL, data=data),
        )
        if not sent:
            # Phase 2: undeliverable stop is observable, never silent.
            logger.warning(
                "task.cancel for %s undelivered (device %s offline)", task_id, task_row.target_device_id
            )
        return sent

    def recover_stuck_dispatching(self) -> int:
        """Server-restart recovery (V1.1 §42): tasks left in DISPATCHING by a
        crash between CAS claim and send are parked back to PENDING with their
        DISPATCHING attempt closed FAILED (SERVER_RESTART). DB-state based -
        no blind re-dispatch; normal max_attempts accounting still applies."""
        recovered = 0
        with SessionLocal() as db:
            svc = TaskService(db)
            stuck = list(db.scalars(select(Task).where(Task.status == "DISPATCHING")))
            for task in stuck:
                for attempt in svc.get_attempts(task.task_id):
                    if attempt.status == "DISPATCHING":
                        attempt.status = "FAILED"
                        attempt.finished_at = utcnow()
                        attempt.error_code = "SERVER_RESTART"
                        attempt.error_message = "dispatch interrupted by server restart"
                for step in svc.get_steps(task.task_id):
                    if step.status == "PENDING":
                        step.current_attempt_id = None
                task.status = "PENDING"
                task.pending_since = utcnow()
                svc._record(task.task_id, "task.recovered", payload={"reason": "server_restart"})
                recovered += 1
            if recovered:
                db.commit()
        if recovered:
            logger.warning("recovered %s task(s) stuck in DISPATCHING from a previous run", recovered)
        return recovered

    async def _load(self, task_id: str):
        with SessionLocal() as db:
            task = db.scalars(select(Task).where(Task.task_id == task_id)).first()
            if task is None:
                return None
            db.expunge(task)
            return task
