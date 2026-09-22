"""Device tools (PDF §27-§29): what the Agent may learn about devices.

Handlers receive the hub captured at registration time so online status is
live (hub connection count, V1.1 §17.7), not the lagging DB heartbeat view.
"""

from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agent.tools.base import AgentTool, EmptyArgs, RiskLevel, ToolErrorCodes, ToolResult
from app.agent.tools.registry import ToolRegistry
from app.capability.service import CapabilityService
from app.db.models import Device
from app.task.service import LIVE_TASK_STATES
from app.task.db_models import Task


class DeviceByNameArgs(BaseModel):
    model_config = {"extra": "forbid"}

    device_name: str = Field(min_length=1, description="设备名称，如 办公室电脑02")


# A device with queued (PENDING) work is busy too: dispatching another task
# would only hit the dispatcher's DEVICE_BUSY lock when the device reconnects.
BUSY_TASK_STATES = {"PENDING", *LIVE_TASK_STATES}


def _live_busy_device_ids(db: Session) -> set[str]:
    return {
        row[0]
        for row in db.execute(
            select(Task.target_device_id).where(
                Task.status.in_(BUSY_TASK_STATES),
                Task.target_device_id.isnot(None),
            )
        ).all()
    }


def _resolve_device(db: Session, device_name: str) -> tuple[Device | None, ToolResult | None]:
    """Exact device_id first, then exact (case-insensitive) name. The Agent
    speaks names; ids are an implementation detail it usually copies from
    earlier tool results."""
    name = (device_name or "").strip()
    if not name:
        return None, ToolResult.fail(ToolErrorCodes.DEVICE_NOT_FOUND, "device name is empty")
    device = db.scalars(select(Device).where(Device.device_id == name)).first()
    if device is None:
        device = db.scalars(
            select(Device).where(Device.name.ilike(name), Device.revoked_at.is_(None))
        ).first()
    if device is None:
        return None, ToolResult.fail(
            ToolErrorCodes.DEVICE_NOT_FOUND, f"device '{device_name}' not found"
        )
    return device, None


def _device_brief(device: Device, hub: Any, busy_ids: set[str]) -> dict:
    return {
        "device_id": device.device_id,
        "name": device.name,
        "online": bool(hub.is_device_online(device.device_id)) if hub else False,
        # V1.6 P0 0.8: dual-axis - connectivity (online) vs scheduling (READY/BUSY)
        "scheduling_state": "BUSY" if device.device_id in busy_ids else "READY",
        "busy": device.device_id in busy_ids,
        "platform": device.platform,
        "status": device.status,
    }


def register_device_tools(registry: ToolRegistry, hub: Any = None) -> None:
    async def list_devices(db: Session, args: dict) -> ToolResult:
        devices = db.scalars(select(Device).where(Device.revoked_at.is_(None))).all()
        busy_ids = _live_busy_device_ids(db)
        return ToolResult.ok(
            {"devices": [_device_brief(d, hub, busy_ids) for d in devices]}
        )

    async def get_device_status(db: Session, args: dict) -> ToolResult:
        device, err = _resolve_device(db, args["device_name"])
        if err:
            return err
        return ToolResult.ok(_device_brief(device, hub, _live_busy_device_ids(db)))

    async def get_device_capabilities(db: Session, args: dict) -> ToolResult:
        device, err = _resolve_device(db, args["device_name"])
        if err:
            return err
        caps = CapabilityService(db).get_device_capabilities(device.device_id)
        # V1.6 P0 0.11: surface the NEW worker_capabilities plane too - the
        # installed automation packages (with freshness) are what the
        # capability resolver actually selects on, not the legacy commands.
        from app.capability_runtime.worker_registry import WorkerCapabilityService

        installed = WorkerCapabilityService(db).get_worker_capabilities(device.device_id)
        return ToolResult.ok(
            {
                "device_id": device.device_id,
                "name": device.name,
                "capabilities": [
                    {"command": c.command_name, "version": c.version, "enabled": c.enabled}
                    for c in caps
                ],
                "installed_packages": [
                    {
                        "name": i.capability_name,
                        "version": i.version,
                        "status": i.status,
                        "last_seen_at": i.last_seen_at.isoformat() if i.last_seen_at else None,
                    }
                    for i in installed
                ],
            }
        )

    registry.register(
        AgentTool(
            name="list_devices",
            description="列出所有已注册设备及其在线/忙状态。不知道有哪些设备时先调用它。",
            handler=list_devices,
            args_schema=EmptyArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="get_device_status",
            description="查询一台设备的在线与忙闲状态。",
            handler=get_device_status,
            args_schema=DeviceByNameArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="get_device_capabilities",
            description="查询一台设备支持哪些命令（能力清单）。",
            handler=get_device_capabilities,
            args_schema=DeviceByNameArgs,
            risk_level=RiskLevel.READ,
        )
    )
