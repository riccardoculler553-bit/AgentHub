"""DeviceConnection: one live WebSocket session bound to a Device identity."""

import asyncio
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum

from starlette.websockets import WebSocket

from app.websocket.protocol import Envelope


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class ConnectionState(StrEnum):
    CONNECTING = "connecting"
    CONNECTED = "connected"
    ACTIVE = "active"
    DISCONNECTING = "disconnecting"
    CLOSED = "closed"


@dataclass
class DeviceConnection:
    device_id: str
    websocket: WebSocket
    connection_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    remote_ip: str | None = None
    state: ConnectionState = ConnectionState.CONNECTING
    connected_at: datetime = field(default_factory=utcnow)
    last_heartbeat_at: datetime = field(default_factory=utcnow)

    async def send(self, envelope: Envelope) -> None:
        await self.websocket.send_text(envelope.to_json())

    async def close(self, code: int = 1000, reason: str = "") -> None:
        self.state = ConnectionState.DISCONNECTING
        try:
            await self.websocket.close(code=code, reason=reason)
        except Exception:
            pass
        self.state = ConnectionState.CLOSED

    def mark_activity(self) -> None:
        self.last_heartbeat_at = utcnow()
        if self.state == ConnectionState.CONNECTED:
            self.state = ConnectionState.ACTIVE
