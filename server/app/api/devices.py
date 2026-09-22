"""Device HTTP APIs: register, list, detail, revoke, send message."""

import asyncio
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.auth.admin import require_admin, require_operator, require_viewer
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
from app.task.db_models import Task
from app.task.service import LIVE_TASK_STATES
from app.websocket.protocol import Envelope, MessageType, new_message_id

router = APIRouter(prefix="/api", tags=["devices"])


def _live_task_counts(db: Session, device_ids: list[str]) -> dict[str, int]:
    """V1.6 P0 0.8: live-task count per device feeds the scheduling axis."""
    if not device_ids:
        return {}
    rows = db.execute(
        select(Task.target_device_id, func.count(Task.task_id))
        .where(Task.target_device_id.in_(device_ids), Task.status.in_(LIVE_TASK_STATES))
        .group_by(Task.target_device_id)
    ).all()
    return {device_id: count for device_id, count in rows}


def _to_http_error(exc: DeviceLinkError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)})


@router.post("/devices/register", response_model=DeviceRegisterOut, status_code=status.HTTP_201_CREATED)
def register_device(payload: DeviceRegisterIn, db: Session = Depends(get_db)):
    """One-time registration: consume code -> create device -> issue token.

    V1.7 naming: the effective device name is payload.device_name, falling
    back to the name the operator assigned when creating the code."""
    from app.api.registration import get_or_create_default_user

    try:
        code_row = RegistrationService(db).consume_code(payload.registration_code)
        effective_name = (payload.device_name or "").strip() or code_row.device_name
        device = DeviceService(db).create_device(
            user_id=code_row.user_id or get_or_create_default_user(db).id,
            name=effective_name or f"device-{device_uuid_suffix()}",
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


@router.get("/devices", response_model=list[DeviceOut], dependencies=[Depends(require_viewer)])
def list_devices(request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    service = DeviceService(db)
    devices = service.list_devices()
    counts = _live_task_counts(db, [d.device_id for d in devices])
    return [
        service.to_out(
            device,
            connection_count=hub.connection_count(device.device_id),
            live_tasks=counts.get(device.device_id, 0),
        )
        for device in devices
    ]


@router.get("/devices/{device_id}", response_model=DeviceOut, dependencies=[Depends(require_viewer)])
def get_device(device_id: str, request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    try:
        device = DeviceService(db).get_device(device_id)
    except DeviceLinkError as exc:
        raise _to_http_error(exc) from exc
    counts = _live_task_counts(db, [device.device_id])
    return DeviceService(db).to_out(
        device,
        connection_count=hub.connection_count(device.device_id),
        live_tasks=counts.get(device.device_id, 0),
    )


@router.get("/devices/{device_id}/environment", dependencies=[Depends(require_viewer)])
def get_device_environment(device_id: str, db: Session = Depends(get_db)):
    """V1.7 §22: the device's latest environment snapshot (machine/runtime/
    automation/worker + fingerprint). 404 when the device never reported."""
    from app.worker.service import WorkerService

    try:
        DeviceService(db).get_device(device_id)
    except DeviceLinkError as exc:
        raise _to_http_error(exc) from exc
    row = WorkerService(db).get_environment(device_id)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail={"code": "environment_not_reported", "message": "device has not reported an environment yet"},
        )
    return {
        "device_id": row.device_id,
        "hostname": row.hostname,
        "worker_version": row.worker_version,
        "fingerprint": row.fingerprint,
        "collected_at": row.collected_at,
        "environment": row.snapshot,
    }


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


@router.post("/devices/{device_id}/messages", response_model=DeviceMessageOut, dependencies=[Depends(require_operator)])
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
