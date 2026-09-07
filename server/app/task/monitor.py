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
                if now - task.created_at > max_wait:
                    svc.timeout_pending(task)
                    logger.warning("task %s TIMEOUT (device offline max wait elapsed)", task.task_id)
                elif task.target_device_id and self.hub.is_device_online(task.target_device_id):
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
            expired = [svc.timeout_running(task) for task in live]
        for result in expired:
            if not result.get("notify_device"):
                continue  # lost the CAS: a result path closed the task already
            logger.warning("task %s TIMEOUT (watchdog)", result["task_id"])
            await self._send_cancel(result["task_id"], result.get("attempt_id"), result.get("step_id"))

    async def notify_cancel(self, task_id: str) -> None:
        """Best-effort task.cancel delivery for an admin-initiated cancel.
        The envelope addresses the CURRENT attempt (V1.1 §16) so a cancel for
        an old attempt can never kill a newer execution."""
        with SessionLocal() as db:
            attempt_id, step_id = TaskService(db)._current_attempt_ids(task_id)
        await self._send_cancel(task_id, attempt_id, step_id)

    async def _send_cancel(self, task_id: str, attempt_id: str | None, step_id: str | None) -> None:
        task_row = await self._load(task_id)
        if not task_row or not task_row.target_device_id:
            return
        data: dict = {"task_id": task_id}
        if step_id:
            data["step_id"] = step_id
        if attempt_id:
            data["attempt_id"] = attempt_id
        await self.hub.send_to_device(
            task_row.target_device_id,
            Envelope(id=new_message_id(), type=MessageType.TASK_CANCEL, data=data),
        )

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
