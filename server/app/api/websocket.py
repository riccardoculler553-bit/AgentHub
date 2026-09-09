"""Device WebSocket gateway: /api/ws/device

Flow: authenticate (Bearer token) -> accept -> audit row -> hub register ->
device.connected -> read loop (heartbeat / message / ack) -> cleanup.
"""

import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from app.auth.token import TokenService
from app.capability.service import CapabilityService
from app.core.exceptions import DeviceLinkError, DeviceRevoked, TokenInvalid
from app.db.database import SessionLocal
from app.db.models import Device, WebsocketConnection, utcnow
from app.device.repository import DeviceRepository
from app.task.dispatcher import TaskDispatcher
from app.task.service import TaskService
from app.websocket.connection import DeviceConnection, ConnectionState
from app.websocket.hub import ConnectionHub
from app.websocket.protocol import Envelope, MessageType, ProtocolError, parse_envelope

logger = logging.getLogger(__name__)

router = APIRouter(tags=["websocket"])

WS_CLOSE_UNAUTHORIZED = 4401
WS_CLOSE_REVOKED = 4403


def _authenticate(authorization: str | None) -> Device:
    """Validate the Bearer token. Returns the Device; raises DeviceLinkError."""
    if not authorization or not authorization.startswith("Bearer "):
        raise TokenInvalid("missing Bearer token")
    token = authorization[len("Bearer "):].strip()
    with SessionLocal() as db:
        token_row = TokenService(db).verify(token)
        device = db.scalars(select(Device).where(Device.device_id == token_row.device_id)).first()
        if device is None:
            raise TokenInvalid()
        if device.revoked_at is not None:
            raise DeviceRevoked()
        return device


@router.websocket("/api/ws/device")
async def device_ws(websocket: WebSocket):
    hub: ConnectionHub = websocket.app.state.hub
    authorization = websocket.headers.get("authorization")

    await websocket.accept()
    try:
        device = _authenticate(authorization)
    except DeviceLinkError as exc:
        await websocket.send_text(
            Envelope(type=MessageType.ERROR, data={"code": exc.code, "message": str(exc)}).to_json()
        )
        close_code = WS_CLOSE_REVOKED if exc.code in ("token_revoked", "device_revoked") else WS_CLOSE_UNAUTHORIZED
        await websocket.close(code=close_code, reason=str(exc))
        return

    connection = DeviceConnection(
        device_id=device.device_id,
        websocket=websocket,
        remote_ip=websocket.client.host if websocket.client else None,
    )
    connection.state = ConnectionState.CONNECTED

    # Audit row + online status
    with SessionLocal() as db:
        db.add(
            WebsocketConnection(
                connection_id=connection.connection_id,
                device_id=connection.device_id,
                connected_at=connection.connected_at,
                remote_ip=connection.remote_ip,
            )
        )
        DeviceRepository(db).set_online(connection.device_id)
        db.commit()

    await hub.register(connection)
    logger.info("device %s connected (connection %s)", connection.device_id, connection.connection_id)

    close_code = 1000
    try:
        await connection.send(Envelope(type=MessageType.DEVICE_CONNECTED, data={"device_id": connection.device_id}))
        while True:
            raw = await websocket.receive_text()
            try:
                envelope = parse_envelope(raw)
            except ProtocolError as exc:
                await connection.send(
                    Envelope(type=MessageType.ERROR, data={"code": "protocol_error", "message": str(exc)})
                )
                continue

            connection.mark_activity()
            await _dispatch(connection, envelope)
            _touch_device(connection.device_id)
    except WebSocketDisconnect as exc:
        close_code = exc.code or 1000
    except Exception:
        logger.exception("connection %s crashed", connection.connection_id)
        close_code = 1011
    finally:
        await hub.unregister(connection)
        with SessionLocal() as db:
            row = db.query(WebsocketConnection).filter_by(connection_id=connection.connection_id).one_or_none()
            if row is not None:
                row.disconnected_at = utcnow()
                row.close_code = close_code
            DeviceRepository(db).touch_last_seen(connection.device_id)
            db.commit()
        logger.info(
            "device %s disconnected (connection %s, code %s)",
            connection.device_id,
            connection.connection_id,
            close_code,
        )


async def _dispatch(connection: DeviceConnection, envelope: Envelope) -> None:
    msg_type = envelope.type
    if msg_type == MessageType.HEARTBEAT:
        await connection.send(
            Envelope(
                id=envelope.id,
                type=MessageType.HEARTBEAT_ACK,
                data={"server_time": Envelope(type=MessageType.HEARTBEAT_ACK).timestamp},
            )
        )
    elif msg_type == MessageType.MESSAGE:
        await connection.send(
            Envelope(id=envelope.id, type=MessageType.MESSAGE_ACK, data={"success": True})
        )
    elif msg_type == MessageType.MESSAGE_ACK:
        # Ack for a server-initiated message; nothing to do in V1.0 beyond liveness.
        logger.debug("message_ack from device %s (id=%s)", connection.device_id, envelope.id)
    elif msg_type == MessageType.DEVICE_CAPABILITIES:
        with SessionLocal() as db:
            count = CapabilityService(db).replace_device_capabilities(
                connection.device_id, envelope.data.get("capabilities", [])
            )
        logger.info("device %s reported %s capability(ies)", connection.device_id, count)
        await connection.send(
            Envelope(id=envelope.id, type=MessageType.MESSAGE_ACK, data={"success": True, "stored": count})
        )
    elif msg_type == MessageType.WORKER_CAPABILITIES:
        # V1.4 §17/§63: installed automation capability packages
        with SessionLocal() as db:
            from app.capability_runtime.worker_registry import WorkerCapabilityService

            count = WorkerCapabilityService(db).replace_worker_capabilities(
                connection.device_id, envelope.data.get("capabilities", [])
            )
        logger.info("device %s reported %s automation capability package(s)", connection.device_id, count)
        await connection.send(
            Envelope(id=envelope.id, type=MessageType.MESSAGE_ACK, data={"success": True, "stored": count})
        )
    elif msg_type in (
        MessageType.TASK_ACCEPT,
        MessageType.TASK_RUNNING,
        MessageType.TASK_PROGRESS,
        MessageType.TASK_RESULT,
        # V1.4 §65: capability lifecycle rides the same Task state machine
        MessageType.CAPABILITY_ACCEPT,
        MessageType.CAPABILITY_RUNNING,
        MessageType.CAPABILITY_PROGRESS,
        MessageType.CAPABILITY_RESULT,
    ):
        # Map capability.* onto the task lifecycle handlers so Retry/Timeout/
        # Cancel/Recovery stay owned by the Task Engine (§67/§69/§70).
        lifecycle_type = str(msg_type)
        if lifecycle_type.startswith("capability."):
            lifecycle_type = "task." + lifecycle_type.split(".", 1)[1]
        with SessionLocal() as db:
            result = TaskService(db).handle_device_event(
                connection.device_id, lifecycle_type, envelope.data
            )
        await connection.send(
            Envelope(id=envelope.id, type=MessageType.MESSAGE_ACK, data={"success": True})
        )
        if result.get("advance"):
            hub: ConnectionHub = connection.websocket.app.state.hub
            task_id = result["task_id"]
            logger.info("task %s advancing to next step", task_id)
            asyncio.create_task(TaskDispatcher(hub).dispatch_task(task_id))
    elif msg_type == MessageType.TASK_CANCEL:
        # Server-initiated only; acknowledge and ignore device-side cancels.
        await connection.send(
            Envelope(id=envelope.id, type=MessageType.MESSAGE_ACK, data={"success": False})
        )
    elif msg_type in (MessageType.DEVICE_CONNECTED, MessageType.DEVICE_DISCONNECTED):
        pass
    else:
        await connection.send(
            Envelope(
                id=envelope.id,
                type=MessageType.ERROR,
                data={"code": "unknown_type", "message": f"unknown message type: {msg_type}"},
            )
        )


_LAST_TOUCH: dict[str, object] = {}


def _touch_device(device_id: str) -> None:
    """Update last_seen_at. Throttled to at most once every 5s per device."""
    now = utcnow()
    last = _LAST_TOUCH.get(device_id)
    if last is not None and (now - last).total_seconds() < 5:
        return
    _LAST_TOUCH[device_id] = now
    with SessionLocal() as db:
        DeviceRepository(db).touch_last_seen(device_id)
        db.commit()
