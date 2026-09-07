"""Device repository: all direct DB access for devices."""

import uuid
from datetime import timedelta

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.db.models import Device, utcnow


class DeviceRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create(
        self,
        user_id: int,
        name: str,
        hostname: str | None,
        platform: str | None,
        client_version: str | None,
    ) -> Device:
        device = Device(
            device_id=str(uuid.uuid4()),
            user_id=user_id,
            name=name,
            hostname=hostname,
            platform=platform,
            client_version=client_version,
            status="offline",
            created_at=utcnow(),
            updated_at=utcnow(),
        )
        self.db.add(device)
        self.db.flush()
        return device

    def get_by_device_id(self, device_id: str) -> Device | None:
        return self.db.scalars(select(Device).where(Device.device_id == device_id)).first()

    def list_all(self) -> list[Device]:
        return list(self.db.scalars(select(Device).order_by(Device.created_at.desc())).all())

    def set_online(self, device_id: str) -> None:
        self.db.execute(
            update(Device)
            .where(Device.device_id == device_id, Device.revoked_at.is_(None))
            .values(status="online", last_seen_at=utcnow(), updated_at=utcnow())
        )

    def touch_last_seen(self, device_id: str) -> None:
        self.db.execute(
            update(Device).where(Device.device_id == device_id).values(last_seen_at=utcnow(), updated_at=utcnow())
        )

    def set_offline(self, device_id: str) -> None:
        self.db.execute(
            update(Device).where(Device.device_id == device_id).values(status="offline", updated_at=utcnow())
        )

    def set_revoked(self, device_id: str) -> int:
        result = self.db.execute(
            update(Device)
            .where(Device.device_id == device_id, Device.revoked_at.is_(None))
            .values(revoked_at=utcnow(), status="revoked", updated_at=utcnow())
        )
        return result.rowcount

    def stale_online_devices(self, threshold_seconds: int) -> list[Device]:
        """Online devices whose last_seen is older than the offline threshold."""
        cutoff = utcnow() - timedelta(seconds=threshold_seconds)
        return list(
            self.db.scalars(
                select(Device).where(Device.status == "online", Device.last_seen_at < cutoff)
            ).all()
        )
