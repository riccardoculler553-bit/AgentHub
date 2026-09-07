"""CapabilityService: per-device command capabilities reported by Workers.

Command Registry (server) defines what the system allows; Capability Registry
records what a specific device actually has. Both must agree before dispatch.
"""

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.capability.db_models import DeviceCapability


class CapabilityService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def replace_device_capabilities(self, device_id: str, capabilities: list[dict]) -> int:
        """Upsert the full capability set reported by a Worker (idempotent)."""
        self.db.execute(delete(DeviceCapability).where(DeviceCapability.device_id == device_id))
        seen: set[str] = set()
        for item in capabilities or []:
            name = str(item.get("name", "")).strip()
            if not name or name in seen:
                continue
            seen.add(name)
            self.db.add(
                DeviceCapability(
                    device_id=device_id,
                    command_name=name,
                    version=str(item.get("version", "1.0")),
                )
            )
        self.db.commit()
        return len(seen)

    def get_device_capabilities(self, device_id: str) -> list[DeviceCapability]:
        return list(
            self.db.scalars(
                select(DeviceCapability)
                .where(DeviceCapability.device_id == device_id)
                .order_by(DeviceCapability.command_name)
            )
        )

    def has_capability(self, device_id: str, command_name: str) -> bool:
        row = self.db.scalars(
            select(DeviceCapability).where(
                DeviceCapability.device_id == device_id,
                DeviceCapability.command_name == command_name,
                DeviceCapability.enabled.is_(True),
            )
        ).first()
        return row is not None

    def devices_with_capability(self, command_name: str) -> list[str]:
        rows = self.db.scalars(
            select(DeviceCapability.device_id).where(
                DeviceCapability.command_name == command_name,
                DeviceCapability.enabled.is_(True),
            )
        ).all()
        return sorted(set(rows))

    def get_all_grouped(self) -> list[dict]:
        rows = self.db.scalars(
            select(DeviceCapability).order_by(DeviceCapability.device_id, DeviceCapability.command_name)
        )
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            grouped.setdefault(row.device_id, []).append(
                {"name": row.command_name, "version": row.version, "enabled": row.enabled}
            )
        return [{"device_id": device_id, "capabilities": caps} for device_id, caps in grouped.items()]
