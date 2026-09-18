"""Device HTTP APIs: register, list, detail, revoke, send message."""

import asyncio
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.orm import Session

from app.auth.admin import require_admin
from app.core.background import spawn
from app.core.exceptions import DeviceLinkError
from app.db.database import get_db
from app.device.models import (
    DeviceMessageIn,
    DeviceMessageOut,
    DeviceOut,
    DeviceRegisterIn,
    DeviceRegisterOut,
)
from app.device.service import DeviceService
from app.registration.service import RegistrationService
from app.websocket.protocol import Envelope, MessageType, new_message_id

router = APIRouter(prefix="/api", tags=["devices"])


def _to_http_error(exc: DeviceLinkError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)})


@router.post("/devices/register", response_model=DeviceRegisterOut, status_code=status.HTTP_201_CREATED)
def register_device(payload: DeviceRegisterIn, db: Session = Depends(get_db)):
    """One-time registration: consume code -> create device -> issue token."""
    from app.api.registration import get_or_create_default_user

    try:
        user = get_or_create_default_user(db)
        code_row = RegistrationService(db).consume_code(payload.registration_code)
        device = DeviceService(db).create_device(
            user_id=code_row.user_id or user.id,
            name=payload.device_name or f"device-{device_uuid_suffix()}",
            hostname=payload.hostname,
            platform=payload.platform,
            client_version=payload.client_version,
        )
        token = DeviceService(db).tokens.issue_for_device(device.device_id)
        db.commit()
    except DeviceLinkError as exc:
        db.rollback()
        raise _to_http_error(exc) from exc
    return DeviceRegisterOut(device_id=device.device_id, device_token=token)


def device_uuid_suffix() -> str:
    return uuid.uuid4().hex[:6]


@router.get("/devices", response_model=list[DeviceOut], dependencies=[Depends(require_admin)])
def list_devices(request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    service = DeviceService(db)
    return [
        service.to_out(device, connection_count=hub.connection_count(device.device_id))
        for device in service.list_devices()
    ]


@router.get("/devices/{device_id}", response_model=DeviceOut, dependencies=[Depends(require_admin)])
def get_device(device_id: str, request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    try:
        device = DeviceService(db).get_device(device_id)
    except DeviceLinkError as exc:
        raise _to_http_error(exc) from exc
    return DeviceService(db).to_out(device, connection_count=hub.connection_count(device.device_id))


@router.post("/devices/{device_id}/revoke", response_model=DeviceOut, dependencies=[Depends(require_admin)])
async def revoke_device(device_id: str, request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    try:
        service = DeviceService(db)
        service.revoke_device(device_id)
        device = service.get_device(device_id)
        db.commit()
    except DeviceLinkError as exc:
        db.rollback()
        raise _to_http_error(exc) from exc
    # Drop live connections; the client should stop reconnecting on 4403.
    spawn(hub.close_device(device_id, code=4403, reason="device revoked"))
    return service.to_out(device, connection_count=0)


@router.post("/devices/{device_id}/messages", response_model=DeviceMessageOut, dependencies=[Depends(require_admin)])
async def send_message(device_id: str, payload: DeviceMessageIn, request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    try:
        DeviceService(db).get_device(device_id)
    except DeviceLinkError as exc:
        raise _to_http_error(exc) from exc

    envelope = Envelope(
        id=new_message_id(),
        type=MessageType(payload.type),
        data=payload.data,
    )
    sent = await hub.send_to_device(device_id, envelope)
    if sent == 0:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "device_offline", "message": "device has no active connection"},
        )
    return DeviceMessageOut(message_id=envelope.id, sent=sent)
