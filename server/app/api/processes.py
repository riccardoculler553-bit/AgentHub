"""Persistent Process APIs (V1.7 §12/§51).

POST /api/processes/start          - operator: launch a service-mode capability
POST /api/processes/{id}/stop      - operator
POST /api/processes/{id}/restart   - operator
GET  /api/processes/{id}           - viewer: instance status
GET  /api/processes                - viewer: list (device_id/status filters)
GET  /api/processes/{id}/logs      - viewer: log tail (request/reply over WS)

The instance registry (worker_processes) is the source of truth for state;
the DEVICE reports RUNNING/STOPPED/FAILED transitions via process.status.
Logs live on the Worker - the server requests a tail over the control plane
and matches the reply by request_id.
"""

import asyncio
import uuid

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.auth.admin import require_operator, require_viewer
from app.capability_runtime.errors import CapabilityError
from app.capability_runtime.service import CapabilityService
from app.core.exceptions import DeviceLinkError
from app.db.database import get_db
from app.device.service import DeviceService
from app.websocket.protocol import Envelope, MessageType, new_message_id
from app.worker.service import WorkerService

router = APIRouter(prefix="/api", tags=["process"])

# request_id -> future delivering the device's process.log content
_LOG_WAITERS: dict[str, asyncio.Future] = {}
_LOG_WAIT_TIMEOUT = 6.0


def deliver_process_log(request_id: str, content: str) -> None:
    """Called by api/websocket.py when a device answers a process.log request."""
    future = _LOG_WAITERS.pop(request_id, None)
    if future is not None and not future.done():
        future.set_result(content)


def _process_out(row) -> dict:
    return {
        "process_id": row.process_id,
        "device_id": row.device_id,
        "capability": row.capability,
        "version": row.version,
        "status": row.status,
        "pid": row.pid,
        "restart_count": row.restart_count,
        "requested_by": row.requested_by,
        "last_error": row.last_error,
        "started_at": row.started_at,
        "stopped_at": row.stopped_at,
        "last_health_at": row.last_health_at,
    }


class ProcessStartIn(BaseModel):
    device_id: str = Field(min_length=1)
    capability: str = Field(min_length=1)
    version: str | None = Field(default=None, max_length=32)


@router.post("/processes/start", status_code=201, dependencies=[Depends(require_operator)])
async def start_process(payload: ProcessStartIn, request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    try:
        DeviceService(db).get_device(payload.device_id)
    except DeviceLinkError as exc:
        raise HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)}) from exc
    if not hub.is_device_online(payload.device_id):
        raise HTTPException(
            status_code=409,
            detail={"code": "device_offline", "message": "device has no live connection"},
        )
    try:
        version = CapabilityService(db).get_published_version(payload.capability, payload.version)
    except CapabilityError as exc:
        raise HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)}) from exc

    row = WorkerService(db).create_process(
        payload.device_id, payload.capability, version.version,
        package_id=version.package_id, requested_by="operator",
    )
    envelope = Envelope(
        id=new_message_id("ps"),
        type=MessageType.PROCESS_START,
        data={
            "process_id": row.process_id,
            "capability": row.capability,
            "version": row.version,
            "package_id": version.package_id,
            "checksum": version.checksum,
        },
    )
    sent = await hub.send_to_device(payload.device_id, envelope)
    if sent == 0:
        WorkerService(db).update_process_status(
            row.process_id, "FAILED", error="device offline at start"
        )
        raise HTTPException(
            status_code=409,
            detail={"code": "device_offline", "message": "process start could not be delivered"},
        )
    return _process_out(WorkerService(db).get_process(row.process_id))


def _get_row(db: Session, process_id: str):
    row = WorkerService(db).get_process(process_id)
    if row is None:
        raise HTTPException(
            status_code=404, detail={"code": "process_not_found", "message": process_id}
        )
    return row


@router.post("/processes/{process_id}/stop", dependencies=[Depends(require_operator)])
async def stop_process(process_id: str, request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    row = _get_row(db, process_id)
    WorkerService(db).update_process_status(process_id, "STOPPING")
    sent = await hub.send_to_device(
        row.device_id, Envelope(id=new_message_id("ps"), type=MessageType.PROCESS_STOP,
                                data={"process_id": process_id})
    )
    if sent == 0:
        raise HTTPException(
            status_code=409, detail={"code": "device_offline", "message": "stop could not be delivered"}
        )
    return _process_out(WorkerService(db).get_process(process_id))


@router.post("/processes/{process_id}/restart", dependencies=[Depends(require_operator)])
async def restart_process(process_id: str, request: Request, db: Session = Depends(get_db)):
    hub = request.app.state.hub
    row = _get_row(db, process_id)
    WorkerService(db).update_process_status(process_id, "STARTING")
    sent = await hub.send_to_device(
        row.device_id, Envelope(id=new_message_id("ps"), type=MessageType.PROCESS_RESTART,
                                data={"process_id": process_id})
    )
    if sent == 0:
        raise HTTPException(
            status_code=409, detail={"code": "device_offline", "message": "restart could not be delivered"}
        )
    return _process_out(WorkerService(db).get_process(process_id))


@router.get("/processes/{process_id}", dependencies=[Depends(require_viewer)])
def get_process(process_id: str, db: Session = Depends(get_db)):
    return _process_out(_get_row(db, process_id))


@router.get("/processes", dependencies=[Depends(require_viewer)])
def list_processes(device_id: str | None = None, status: str | None = None, db: Session = Depends(get_db)):
    rows = WorkerService(db).list_processes(device_id=device_id, status=status)
    return [_process_out(r) for r in rows]


@router.get("/processes/{process_id}/logs", dependencies=[Depends(require_viewer)])
async def get_process_logs(
    process_id: str,
    request: Request,
    tail_bytes: int = 8000,
    db: Session = Depends(get_db),
):
    """Request a log tail from the device (logs live on the Worker). Waits up
    to a few seconds for the WS reply; 504 when the device stays silent."""
    row = _get_row(db, process_id)
    request_id = f"plog_{uuid.uuid4().hex[:12]}"
    future: asyncio.Future = asyncio.get_running_loop().create_future()
    _LOG_WAITERS[request_id] = future
    try:
        sent = await request.app.state.hub.send_to_device(
            row.device_id,
            Envelope(id=new_message_id("pl"), type=MessageType.PROCESS_LOG,
                     data={"process_id": process_id, "request_id": request_id,
                           "tail_bytes": tail_bytes}),
        )
        if sent == 0:
            raise HTTPException(
                status_code=409, detail={"code": "device_offline", "message": "device offline"}
            )
        content = await asyncio.wait_for(future, timeout=_LOG_WAIT_TIMEOUT)
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=504,
            detail={"code": "log_reply_timeout", "message": "device did not deliver the log tail in time"},
        )
    finally:
        _LOG_WAITERS.pop(request_id, None)
    return {"process_id": process_id, "content": content}
