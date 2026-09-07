"""ConnectionHub: in-memory realtime state.

Device != Connection. One device may hold several live connections.
The Hub owns no business logic (no DB writes, no token issuing).

A threading.Lock guards the dicts (fast, no awaits inside) so the hub can be
touched from any event loop (HTTP handlers, WS handlers, test portals).
"""

import logging
import threading

from app.websocket.connection import DeviceConnection
from app.websocket.protocol import Envelope

logger = logging.getLogger(__name__)


class ConnectionHub:
    def __init__(self) -> None:
        self._connections: dict[str, DeviceConnection] = {}
        self._device_connections: dict[str, set[str]] = {}
        self._lock = threading.Lock()

    async def register(self, connection: DeviceConnection) -> None:
        with self._lock:
            self._connections[connection.connection_id] = connection
            self._device_connections.setdefault(connection.device_id, set()).add(connection.connection_id)

    async def unregister(self, connection: DeviceConnection) -> None:
        with self._lock:
            self._connections.pop(connection.connection_id, None)
            conn_ids = self._device_connections.get(connection.device_id)
            if conn_ids is not None:
                conn_ids.discard(connection.connection_id)
                if not conn_ids:
                    self._device_connections.pop(connection.device_id, None)

    def get_connection(self, connection_id: str) -> DeviceConnection | None:
        with self._lock:
            return self._connections.get(connection_id)

    def get_device_connections(self, device_id: str) -> list[DeviceConnection]:
        with self._lock:
            conn_ids = list(self._device_connections.get(device_id, set()))
            return [self._connections[cid] for cid in conn_ids if cid in self._connections]

    def is_device_online(self, device_id: str) -> bool:
        with self._lock:
            return bool(self._device_connections.get(device_id))

    def connection_count(self, device_id: str) -> int:
        with self._lock:
            return len(self._device_connections.get(device_id, set()))

    def select_worker_connection(self, device_id: str) -> DeviceConnection | None:
        """Pick exactly ONE live connection as this device's task executor.

        task.dispatch must never be broadcast: a device with several live
        connections would run the RPA twice (PDF §78). Deterministic pick for
        stability across quick successive dispatches."""
        connections = self.get_device_connections(device_id)
        if not connections:
            return None
        return sorted(connections, key=lambda c: c.connection_id)[0]

    async def send_to_worker(self, device_id: str, envelope: Envelope) -> int:
        """Single-connection task delivery. Returns 1 when delivered, 0 when
        the device has no live connection (or the picked one just died)."""
        connection = self.select_worker_connection(device_id)
        if connection is None:
            return 0
        try:
            await connection.send(envelope)
            return 1
        except Exception:
            logger.warning("send to worker connection %s failed, pruning", connection.connection_id)
            await self.unregister(connection)
            return 0

    async def send_to_device(self, device_id: str, envelope: Envelope) -> int:
        """Send to every live connection of the device. Returns number of successful sends."""
        sent = 0
        dead: list[DeviceConnection] = []
        for connection in self.get_device_connections(device_id):
            try:
                await connection.send(envelope)
                sent += 1
            except Exception:
                logger.warning("send to connection %s failed, pruning", connection.connection_id)
                dead.append(connection)
        for connection in dead:
            await self.unregister(connection)
        return sent

    async def broadcast(self, envelope: Envelope) -> int:
        sent = 0
        for connection in self.get_device_connections_all():
            try:
                await connection.send(envelope)
                sent += 1
            except Exception:
                await self.unregister(connection)
        return sent

    def get_device_connections_all(self) -> list[DeviceConnection]:
        with self._lock:
            return list(self._connections.values())

    async def close_device(self, device_id: str, code: int = 1000, reason: str = "") -> None:
        for connection in self.get_device_connections(device_id):
            await connection.close(code=code, reason=reason)
            await self.unregister(connection)
