"""Client-side protocol helpers (mirror of the server envelope)."""

import json
import time
import uuid

PROTOCOL_VERSION = 1


class ProtocolError(ValueError):
    pass


def new_message_id(prefix: str = "evt") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"


def build_envelope(msg_type: str, data: dict | None = None, msg_id: str | None = None) -> dict:
    return {
        "id": msg_id or new_message_id(),
        "type": msg_type,
        "version": PROTOCOL_VERSION,
        "timestamp": int(time.time() * 1000),
        "data": data or {},
    }


def parse_envelope(raw: str | bytes) -> dict:
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError) as exc:
        raise ProtocolError("not valid JSON") from exc
    if not isinstance(payload, dict) or "id" not in payload or "type" not in payload:
        raise ProtocolError("invalid envelope")
    return payload
