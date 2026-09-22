"""Capability Runtime APIs (V1.4 §61-§62).

Admin endpoints (X-Admin-Token): capability definition CRUD, version upload,
publish, worker capability views.
Worker endpoint (device Bearer token OR admin): package download for Lazy Pull.
"""

import hashlib
import hmac
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.artifact.service import safe_artifact_name
from app.auth.admin import require_admin, require_viewer
from app.capability_runtime import models as schemas
from app.capability_runtime.errors import CapabilityError, CapabilityVersionNotFound
from app.capability_runtime.package_service import PackageService, peek_manifest_path
from app.capability_runtime.service import CapabilityService
from app.capability_runtime.worker_registry import WorkerCapabilityService
from app.auth.token import TokenService
from app.core.config import settings
from app.core.exceptions import DeviceLinkError
from app.db.database import get_db
from sqlalchemy import select

from app.db.models import Device

# V1.6 P0 0.18: reads require viewer; capability publishing/mutation is
# admin-only (operator may dispatch, not change the registry).
router = APIRouter(prefix="/api", tags=["capability"])
worker_router = APIRouter(prefix="/api", tags=["capability"])


def _capability_error(exc: CapabilityError) -> HTTPException:
    return HTTPException(status_code=exc.status_code, detail={"code": exc.code, "message": str(exc)})


def _capability_out(row) -> schemas.CapabilityOut:
    return schemas.CapabilityOut(
        name=row.name,
        display_name=row.display_name,
        description=row.description,
        type=row.type,
        runtime_type=row.runtime_type,
        enabled=row.enabled,
        current_version=row.current_version,
        input_schema=row.input_schema or {},
        output_schema=row.output_schema or {},
        risk_level=row.risk_level,
        requires_confirmation=bool(row.requires_confirmation),
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


def _version_out(row) -> schemas.CapabilityVersionOut:
    return schemas.CapabilityVersionOut(
        id=row.id,
        capability_name=row.capability_name,
        version=row.version,
        package_id=row.package_id,
        status=row.status,
        input_schema=row.input_schema or {},
        output_schema=row.output_schema or {},
        entrypoint=row.entrypoint,
        checksum=row.checksum,
        created_at=row.created_at,
    )


def _device_from_bearer(authorization: str | None, db: Session) -> Device:
    """Bearer device-token auth for worker-facing HTTP endpoints (§66)."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail={"code": "token_invalid", "message": "missing Bearer token"})
    token = authorization[len("Bearer "):].strip()
    try:
        token_row = TokenService(db).verify(token)
        device = db.scalars(select(Device).where(Device.device_id == token_row.device_id)).first()
        if device is None or device.revoked_at is not None:
            raise DeviceLinkError("device unavailable")
        return device
    except DeviceLinkError as exc:
        raise HTTPException(status_code=401, detail={"code": "token_invalid", "message": str(exc)}) from exc


def _require_device_or_admin(
    authorization: str | None,
    x_admin_token: str | None,
    db: Session,
) -> None:
    """Package download accepts a device token (Worker Lazy Pull) or the admin
    token (manual inspection)."""
    expected = settings.admin_token
    if expected and x_admin_token and hmac.compare_digest(x_admin_token, expected):
        return
    device = _device_from_bearer(authorization, db)
    del device


# ---------------------------------------------------------------- definitions


@router.get("/capabilities", response_model=list[schemas.CapabilityOut], dependencies=[Depends(require_viewer)])
def list_capabilities(enabled_only: bool = False, db: Session = Depends(get_db)):
    """Automation capabilities (V1.4 §61). Device command capabilities live
    under /api/device-capabilities."""
    return [_capability_out(c) for c in CapabilityService(db).list_capabilities(enabled_only)]


@router.post("/capabilities", response_model=schemas.CapabilityOut, status_code=201, dependencies=[Depends(require_admin)])
def create_capability(payload: schemas.CapabilityCreateIn, db: Session = Depends(get_db)):
    try:
        row = CapabilityService(db).create_capability(
            payload.name,
            payload.runtime_type,
            display_name=payload.display_name,
            description=payload.description,
            type=payload.type,
            input_schema=payload.input_schema,
            output_schema=payload.output_schema,
            risk_level=payload.risk_level,
            requires_confirmation=payload.requires_confirmation,
        )
    except CapabilityError as exc:
        raise _capability_error(exc) from exc
    return _capability_out(row)


@router.get("/capabilities/{name}", response_model=schemas.CapabilityOut, dependencies=[Depends(require_viewer)])
def get_capability(name: str, db: Session = Depends(get_db)):
    try:
        return _capability_out(CapabilityService(db).require_capability(name))
    except CapabilityError as exc:
        raise _capability_error(exc) from exc


@router.patch("/capabilities/{name}", response_model=schemas.CapabilityOut, dependencies=[Depends(require_admin)])
def update_capability(name: str, payload: schemas.CapabilityUpdateIn, db: Session = Depends(get_db)):
    try:
        row = CapabilityService(db).update_capability(
            name,
            enabled=payload.enabled,
            display_name=payload.display_name,
            description=payload.description,
        )
    except CapabilityError as exc:
        raise _capability_error(exc) from exc
    return _capability_out(row)


# ------------------------------------------------------------------- versions


@router.get("/capabilities/{name}/versions", response_model=list[schemas.CapabilityVersionOut], dependencies=[Depends(require_viewer)])
def list_versions(name: str, db: Session = Depends(get_db)):
    try:
        CapabilityService(db).require_capability(name)
    except CapabilityError as exc:
        raise _capability_error(exc) from exc
    return [_version_out(v) for v in CapabilityService(db).list_versions(name)]


@router.post("/capabilities/{name}/versions", response_model=schemas.CapabilityVersionOut, status_code=201, dependencies=[Depends(require_admin)])
async def upload_version(name: str, file: UploadFile = File(...), db: Session = Depends(get_db)):
    """Upload a capability package ZIP (§57: 开发 -> 上传 -> 创建版本). Version
    identity comes from the manifest inside the archive; record starts DRAFT."""
    try:
        service = CapabilityService(db)
        capability = service.require_capability(name)
        # V1.6 P0 0.5 (audit H2): stream the ZIP to a temp file inside
        # packages_root (same volume as the final store) instead of holding
        # the whole archive in memory.
        tmp_dir = PackageService.packages_root()
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp = tmp_dir / f".upl-{uuid4().hex}.zip"
        digest = hashlib.sha256()
        try:
            with tmp.open("wb") as out:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    out.write(chunk)
                    digest.update(chunk)
            manifest = peek_manifest_path(tmp)
            package, manifest = PackageService(db).save_package_from_path(
                name, manifest.version, capability.runtime_type, tmp, checksum=digest.hexdigest()
            )
        finally:
            tmp.unlink(missing_ok=True)
        version = service.create_version(
            name,
            manifest.version,
            package.package_id,
            entrypoint=manifest.entrypoint,
            input_schema=manifest.inputs,
            output_schema=manifest.outputs,
            checksum=package.checksum,
            config=manifest.config,
        )
    except CapabilityError as exc:
        raise _capability_error(exc) from exc
    return _version_out(version)


@router.post("/capability-versions/{version_id}/publish", response_model=schemas.CapabilityVersionOut, dependencies=[Depends(require_admin)])
def publish_version(version_id: int, db: Session = Depends(get_db)):
    try:
        service = CapabilityService(db)
        row = service.get_version_by_id(version_id)
        if row is None:
            raise CapabilityVersionNotFound("(id)", str(version_id))
        published = service.publish_version(row.capability_name, row.version)
    except CapabilityError as exc:
        raise _capability_error(exc) from exc
    return _version_out(published)


# -------------------------------------------------------------------- workers


@router.get("/worker-capabilities", response_model=list[schemas.WorkerCapabilityOut], dependencies=[Depends(require_viewer)])
def list_worker_capabilities(db: Session = Depends(get_db)):
    return [
        schemas.WorkerCapabilityOut(worker_id=item["worker_id"], capabilities=item["capabilities"])
        for item in WorkerCapabilityService(db).list_all()
    ]


# -------------------------------------------------------- package download (§62)


@worker_router.get("/capability-packages/{package_id}/download")
def download_package(
    package_id: str,
    authorization: str | None = Header(default=None),
    x_admin_token: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    """Worker Lazy Pull over HTTP (§66: Package Download 走 HTTP，不走 WS)."""
    _require_device_or_admin(authorization, x_admin_token, db)
    package = PackageService(db).get_package(package_id)
    path = settings.storage_dir / package.storage_path
    return FileResponse(
        path,
        media_type="application/zip",
        filename=f"{safe_artifact_name(package.name)}-{package.version}.zip",
    )
