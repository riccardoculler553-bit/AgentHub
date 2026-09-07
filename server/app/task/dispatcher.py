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
from app.task.service import TaskService
from app.websocket.protocol import Envelope, MessageType, new_message_id

logger = logging.getLogger(__name__)


class TaskDispatcher:
    def __init__(self, hub) -> None:
        self.device_link = DeviceLinkService(hub)

    async def dispatch_task(self, task_id: str) -> bool:
        """Dispatch the next pending step of a PENDING task. Returns True when
        the task envelope hit at least one live connection."""
        with SessionLocal() as db:
            service = TaskService(db)
            try:
                task = service.get(task_id)
            except Exception:
                return False
            if task.status != "PENDING" or task.target_device_id is None:
                return False
            step = service.next_pending_step(task_id)
            if step is None:
                return False
            device_id = task.target_device_id

            if not self.device_link.is_online(device_id):
                return False  # stay PENDING; the monitor retries while within max wait

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
            task.status = "DISPATCHING"
            service._record(task_id, "task.dispatching", step_id=step.step_id, attempt_id=attempt.attempt_id)
            db.commit()

            envelope = Envelope(
                id=new_message_id(),
                type=MessageType.TASK_DISPATCH,
                data={
                    "task_id": task_id,
                    "step_id": step.step_id,
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
                service._record(task_id, "task.dispatch_failed", step_id=step.step_id, payload={"reason": "device_offline"})
                db.commit()
                return False

            attempt.status = "SENT"
            attempt.dispatch_message_id = envelope.id
            task.status = "SENT"
            task.timeout_at = utcnow() + timedelta(seconds=int(command.timeout))
            service._record(
                task_id, "task.sent", step_id=step.step_id, attempt_id=attempt.attempt_id,
                payload={"message_id": envelope.id, "connections": sent},
            )
            db.commit()
            logger.info("task %s step %s dispatched (attempt %s, %s connection(s))", task_id, step.step_id, attempt.attempt_no, sent)
            return True
