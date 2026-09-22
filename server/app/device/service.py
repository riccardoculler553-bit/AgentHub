"""DeviceService: device lifecycle operations (create/query/revoke/status)."""

from sqlalchemy.orm import Session

from app.auth.token import TokenService
from app.core.exceptions import DeviceNotFound
from app.db.models import Device
from app.device.models import DeviceOut
from app.device.repository import DeviceRepository


class DeviceService:
    def __init__(self, db: Session) -> None:
        self.db = db
        self.repo = DeviceRepository(db)
        self.tokens = TokenService(db)

    def create_device(
        self,
        user_id: int,
        name: str,
        hostname: str | None,
        platform: str | None,
        client_version: str | None,
    ) -> Device:
        return self.repo.create(user_id, name, hostname, platform, client_version)

    def get_device(self, device_id: str) -> Device:
        device = self.repo.get_by_device_id(device_id)
        if device is None:
            raise DeviceNotFound()
        return device

    def list_devices(self) -> list[Device]:
        return self.repo.list_all()

    def revoke_device(self, device_id: str) -> None:
        device = self.get_device(device_id)
        self.tokens.revoke_for_device(device.device_id)
        self.repo.set_revoked(device.device_id)
        # V1.6 P0 0.10 (audit: forget_worker was dead code): a revoked device's
        # capability ads must not linger as phantom install state.
        from app.capability_runtime.worker_registry import WorkerCapabilityService

        WorkerCapabilityService(self.db).forget_worker(device.device_id)

    def to_out(self, device: Device, connection_count: int = 0, live_tasks: int = 0) -> DeviceOut:
        return DeviceOut(
            device_id=device.device_id,
            name=device.name,
            hostname=device.hostname,
            platform=device.platform,
            client_version=device.client_version,
            status=device.status,
            last_seen_at=device.last_seen_at,
            created_at=device.created_at,
            revoked_at=device.revoked_at,
            connection_count=connection_count,
            # Hub truth (V1.1 §38): a device is online iff it holds >=1 live
            # connection. DB status is the grace-period view kept by the
            # HeartbeatMonitor and deliberately lags behind socket loss.
            online=connection_count > 0,
            # V1.6 P0 0.8: scheduling is a separate axis from connectivity.
            # READY = may take a new task now; BUSY = already executing one
            # (max_concurrency=1 for GUI/RPA sessions). DRAINING is P1.
            scheduling_state="BUSY" if live_tasks > 0 else "READY",
        )
