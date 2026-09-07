"""Server-side heartbeat utilities.

- heartbeat_ack replies are produced by the WS read loop.
- HeartbeatMonitor implements the offline grace period: a device goes OFFLINE
  only when its last_seen_at is older than the offline threshold, never
  immediately on socket loss.
"""

import asyncio
import logging

from app.core.config import settings
from app.db.database import SessionLocal
from app.device.repository import DeviceRepository
from app.websocket.hub import ConnectionHub

logger = logging.getLogger(__name__)


class HeartbeatMonitor:
    def __init__(self, hub: ConnectionHub, check_interval: float = 5.0) -> None:
        self.hub = hub
        self.check_interval = check_interval

    async def run(self) -> None:
        logger.info(
            "HeartbeatMonitor started (threshold=%ss, check every %ss)",
            settings.offline_threshold,
            self.check_interval,
        )
        while True:
            try:
                self.sweep()
            except Exception:
                logger.exception("heartbeat sweep failed")
            await asyncio.sleep(self.check_interval)

    def sweep(self) -> None:
        with SessionLocal() as db:
            repo = DeviceRepository(db)
            stale = repo.stale_online_devices(settings.offline_threshold)
            for device in stale:
                if self.hub.is_device_online(device.device_id) and not self._connections_stale(device.device_id):
                    continue
                repo.set_offline(device.device_id)
                logger.info("device %s marked OFFLINE (grace period elapsed)", device.device_id)
            db.commit()

    def _connections_stale(self, device_id: str) -> bool:
        connections = self.hub.get_device_connections(device_id)
        if not connections:
            return True
        from datetime import timedelta

        from app.websocket.connection import utcnow

        cutoff = utcnow() - timedelta(seconds=settings.offline_threshold)
        return all(conn.last_heartbeat_at < cutoff for conn in connections)
