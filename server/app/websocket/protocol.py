"""Unified message envelope protocol.

Every message on the wire looks like:
    {"id": "...", "type": "...", "version": 1, "timestamp": 1788595200000, "data": {}}
"""

import json
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum

PROTOCOL_VERSION = 1

REQUIRED_FIELDS = ("id", "type", "version")


class MessageType(StrEnum):
    HEARTBEAT = "heartbeat"
    HEARTBEAT_ACK = "heartbeat_ack"
    DEVICE_CONNECTED = "device.connected"
    DEVICE_DISCONNECTED = "device.disconnected"
    MESSAGE = "message"
    MESSAGE_ACK = "message_ack"
    ERROR = "error"
    # --- AgentHub V1.0 task protocol (Envelope v1 reuse) ---
    TASK_DISPATCH = "task.dispatch"          # server -> device
    TASK_ACCEPT = "task.accept"              # device -> server (Task ACK, not transport ACK)
    TASK_RUNNING = "task.running"            # device -> server
    TASK_PROGRESS = "task.progress"          # device -> server
    TASK_RESULT = "task.result"              # device -> server (terminal)
    TASK_CANCEL = "task.cancel"              # server -> device
    DEVICE_CAPABILITIES = "device.capabilities"  # device -> server (capability report)
    # --- AgentHub V1.4 Capability Runtime (§65/§66) ---
    CAPABILITY_EXECUTE = "capability.execute"    # server -> device (dispatch)
    CAPABILITY_ACCEPT = "capability.accept"      # device -> server (Task ACK)
    CAPABILITY_RUNNING = "capability.running"    # device -> server
    CAPABILITY_PROGRESS = "capability.progress"  # device -> server
    CAPABILITY_RESULT = "capability.result"      # device -> server (terminal)
    WORKER_CAPABILITIES = "worker.capabilities"  # device -> server (installed packages)
    # --- AgentHub V1.7 Worker Platformization ---
    WORKER_ENVIRONMENT = "worker.environment"    # device -> server (env snapshot §22)
    PROCESS_START = "process.start"              # server -> device (§12)
    PROCESS_STOP = "process.stop"                # server -> device
    PROCESS_RESTART = "process.restart"          # server -> device
    PROCESS_STATUS = "process.status"            # device -> server (instance status)
    PROCESS_LOG = "process.log"                  # device -> server (log tail, on demand)


class ProtocolError(ValueError):
    pass


@dataclass
class Envelope:
    type: MessageType | str
    data: dict = field(default_factory=dict)
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    version: int = PROTOCOL_VERSION
    timestamp: int = field(default_factory=lambda: int(time.time() * 1000))

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "type": str(self.type),
            "version": self.version,
            "timestamp": self.timestamp,
            "data": self.data,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"))


def new_message_id(prefix: str = "msg") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def parse_envelope(raw: str | bytes) -> Envelope:
    """Parse and validate an inbound envelope. Raises ProtocolError on bad input."""
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ProtocolError("message is not valid JSON") from exc
    if not isinstance(payload, dict):
        raise ProtocolError("message must be a JSON object")
    for name in REQUIRED_FIELDS:
        if name not in payload:
            raise ProtocolError(f"missing required field: {name}")
    if not isinstance(payload.get("version"), int):
        raise ProtocolError("version must be an integer")
    if not isinstance(payload.get("type"), str) or not payload["type"]:
        raise ProtocolError("type must be a non-empty string")
    data = payload.get("data", {})
    if not isinstance(data, dict):
        raise ProtocolError("data must be a JSON object")
    return Envelope(
        id=str(payload["id"]),
        type=payload["type"],
        version=payload["version"],
        timestamp=payload.get("timestamp", int(time.time() * 1000)),
        data=data,
    )
