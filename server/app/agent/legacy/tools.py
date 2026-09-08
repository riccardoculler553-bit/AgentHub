"""Agent tools: service-backed capabilities exposed to the graph.

The Agent never gets raw SQL tools (PDF §68) and never a WebSocket tool
(PDF §69) - it goes through the service layer, which goes through
DeviceLinkService for anything transport related.
"""

from sqlalchemy.orm import Session

from app.capability.service import CapabilityService
from app.command.service import CommandService
from app.device.service import DeviceService
from app.task.models import TaskCreateIn
from app.task.service import TaskService


def list_devices(db: Session) -> list[dict]:
    return [
        {
            "device_id": d.device_id,
            "name": d.name,
            "status": d.status,
            "online": d.status == "online",
            "platform": d.platform,
        }
        for d in DeviceService(db).list_devices()
    ]


def search_devices_by_capability(db: Session, command_name: str) -> list[dict]:
    """Devices that report the capability, with their online status."""
    hub_online = None  # filled by caller if needed; status from DB is enough here
    ids = CapabilityService(db).devices_with_capability(command_name)
    result = []
    for device_id in ids:
        try:
            d = DeviceService(db).get_device(device_id)
        except Exception:
            continue
        result.append({"device_id": d.device_id, "name": d.name, "online": d.status == "online"})
    _ = hub_online
    return result


def get_device(db: Session, device_id: str) -> dict | None:
    try:
        d = DeviceService(db).get_device(device_id)
    except Exception:
        return None
    return {"device_id": d.device_id, "name": d.name, "status": d.status, "online": d.status == "online"}


def list_commands(db: Session) -> list[dict]:
    return [
        {
            "command_name": c.command_name,
            "version": c.version,
            "description": c.description,
            "executor_type": c.executor_type,
            "params_schema": c.params_schema,
            "timeout": c.timeout,
            "enabled": c.enabled,
        }
        for c in CommandService(db).list_commands()
    ]


def get_command(db: Session, name: str) -> dict | None:
    try:
        c = CommandService(db).get_command(name)
    except Exception:
        return None
    return {
        "command_name": c.command_name,
        "version": c.version,
        "params_schema": c.params_schema,
        "timeout": c.timeout,
        "enabled": c.enabled,
    }


def create_task(db: Session, payload: TaskCreateIn, created_by: str = "main_agent") -> dict:
    task = TaskService(db).create(payload, created_by=created_by)
    return {"task_id": task.task_id, "status": task.status}


def get_task_detail(db: Session, task_id: str) -> dict | None:
    service = TaskService(db)
    try:
        task = service.get(task_id)
    except Exception:
        return None
    return {
        "task_id": task.task_id,
        "status": task.status,
        "target_device_id": task.target_device_id,
        "created_at": task.created_at.isoformat() if task.created_at else None,
        "finished_at": task.finished_at.isoformat() if task.finished_at else None,
        "steps": [
            {"step_id": s.step_id, "command": s.command, "status": s.status} for s in service.get_steps(task_id)
        ],
        "attempts": [
            {
                "attempt_no": a.attempt_no,
                "status": a.status,
                "error_code": a.error_code,
                "error_message": a.error_message,
            }
            for a in service.get_attempts(task_id)
        ],
        "result": _final_result(service, task_id),
    }


def _final_result(service: TaskService, task_id: str) -> dict | None:
    events = service.get_events(task_id)
    for event in reversed(events):
        if event.event_type in ("task.success", "task.failed", "task.timeout"):
            return {
                "event": event.event_type,
                "payload": event.payload,
            }
    return None


def retry_task(db: Session, task_id: str) -> dict:
    return TaskService(db).request_retry(task_id)


def cancel_task(db: Session, task_id: str) -> dict:
    return TaskService(db).request_cancel(task_id)
