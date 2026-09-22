"""Artifact APIs (V1.4 §64 / V1.5 §13).

POST /api/artifacts            - Worker upload (device Bearer token), multipart
GET  /api/artifacts            - Admin list (by task/workflow)
GET  /api/artifacts/{id}       - Admin detail
GET  /api/artifacts/{id}/download - Worker (device Bearer) or Admin download
DELETE /api/artifacts/{id}     - Admin delete (lifecycle-controlled)
"""

import hashlib
import hmac
from pathlib import Path
from uuid import uuid4

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, Response, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.artifact import models as schemas
from app.artifact.service import ArtifactNotFound, ArtifactService
from app.auth.admin import require_admin, require_viewer
from app.core.config import settings
from app.db.database import get_db

from app.api.capability import _device_from_bearer

# V1.6 P0 0.18: artifact metadata reads require viewer; filesystem scans,
# server-local registration and deletion stay admin-only.
admin_router = APIRouter(prefix="/api/artifacts", tags=["artifact"])
upload_router = APIRouter(prefix="/api/artifacts", tags=["artifact"])
# V1.5 §13: workers download input artifacts over HTTP (data plane).
download_router = APIRouter(prefix="/api/artifacts", tags=["artifact"])


def _artifact_out(row) -> schemas.ArtifactOut:
    return schemas.ArtifactOut(
        artifact_id=row.artifact_id,
        name=row.name,
        type=row.type,
        mime_type=row.mime_type,
        size=row.size,
        checksum=row.checksum,
        source_worker_id=row.source_worker_id,
        task_id=row.task_id,
        workflow_run_id=row.workflow_run_id,
        step_run_id=row.step_run_id,
        created_at=row.created_at,
        expires_at=row.expires_at,
    )


@upload_router.post("", response_model=schemas.ArtifactOut, status_code=201)
async def upload_artifact(
    file: UploadFile = File(...),
    name: str | None = Form(default=None),
    type: str = Form(default="file"),
    task_id: str | None = Form(default=None),
    workflow_run_id: str | None = Form(default=None),
    step_run_id: str | None = Form(default=None),
    authorization: str | None = Header(default=None),
    x_admin_token: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    """Worker artifact upload (§30: Worker -> Server Storage -> Artifact ID).
    Accepts a device token; the admin token also works for manual testing."""
    expected = settings.admin_token
    if expected and x_admin_token and hmac.compare_digest(x_admin_token, expected):
        worker_id = None
    else:
        worker_id = _device_from_bearer(authorization, db).device_id
    # V1.6 P0 0.5 (audit H2): stream to a prepared temp file instead of
    # file.read() - a 500MB upload no longer has to fit in memory. SHA-256 is
    # accumulated while streaming; the temp file is renamed into the store.
    tmp_dir = ArtifactService.artifacts_root() / ".incoming"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp = tmp_dir / f"upl_{uuid4().hex}"
    digest = hashlib.sha256()
    try:
        with tmp.open("wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                out.write(chunk)
                digest.update(chunk)
        if tmp.stat().st_size == 0:
            raise HTTPException(
                status_code=400,
                detail={"code": "empty_upload", "message": "empty file upload"},
            )
        row = ArtifactService(db).create_artifact_from_file(
            name=name or file.filename or "artifact.bin",
            source=tmp,
            type=type,
            source_worker_id=worker_id,
            task_id=task_id or None,
            workflow_run_id=workflow_run_id or None,
            step_run_id=step_run_id or None,
            checksum=digest.hexdigest(),
            move=True,
        )
    finally:
        tmp.unlink(missing_ok=True)
    return _artifact_out(row)


@admin_router.get("", response_model=list[schemas.ArtifactOut], dependencies=[Depends(require_viewer)])
def list_artifacts(
    task_id: str | None = None,
    workflow_run_id: str | None = None,
    limit: int = 100,
    offset: int = 0,
    search: str | None = None,
    response: Response = None,
    db: Session = Depends(get_db),
):
    """Artifact list with V1.7 dashboard paging: `search` matches artifact
    id / name / task id; the total row count rides in X-Total-Count."""
    service = ArtifactService(db)
    rows = service.list_artifacts(
        task_id=task_id, workflow_run_id=workflow_run_id,
        limit=limit, offset=max(0, offset), search=search,
    )
    if response is not None:
        response.headers["X-Total-Count"] = str(
            service.count_artifacts(task_id=task_id, workflow_run_id=workflow_run_id, search=search)
        )
    return [
        _artifact_out(a)
        for a in rows
    ]


@admin_router.get("/scan", dependencies=[Depends(require_admin)])
def scan_directory(dir: str):
    """List uploadable files of a server-local directory (dashboard 数据源注册).

    Admin-only; first-level files only (hidden / Excel lock files skipped).
    Must be declared BEFORE /{artifact_id} so "scan" is not swallowed."""
    path = _resolve_local_dir(dir)
    files = [
        {"name": f.name, "size": f.stat().st_size}
        for f in sorted(path.iterdir())
        if f.is_file() and not f.name.startswith(("~$", "."))
    ]
    return {"dir": str(path), "files": files}


class RegisterLocalIn(BaseModel):
    """Server-local file registration request (dashboard 数据源注册)."""

    dir: str = Field(min_length=1, max_length=500)
    name: str = Field(min_length=1, max_length=300)


def _resolve_local_dir(dir: str) -> Path:
    if not Path(dir).is_absolute():
        raise HTTPException(status_code=400, detail={"code": "invalid_dir", "message": "dir must be an absolute path"})
    path = Path(dir)
    if not path.is_dir():
        raise HTTPException(status_code=404, detail={"code": "dir_not_found", "message": f"directory not found: {dir}"})
    return path


@admin_router.post("/register-local", response_model=schemas.ArtifactOut, status_code=201, dependencies=[Depends(require_admin)])
def register_local_artifact(payload: RegisterLocalIn, db: Session = Depends(get_db)):
    """Register a server-local file as an Artifact WITHOUT pushing the bytes
    through the browser - the server reads its own disk (V1.5 §26 数据平面).

    Admin-only; the file must live under the given directory (no traversal)."""
    base = _resolve_local_dir(payload.dir)
    path = base / Path(payload.name).name  # bare filename -> no traversal
    if not path.is_file():
        raise HTTPException(
            status_code=404,
            detail={"code": "file_not_found", "message": f"file not found: {path}"},
        )
    # V1.6 P0 0.5: stream-copy instead of read_bytes() so large server-local
    # files don't get held fully in memory.
    row = ArtifactService(db).create_artifact_from_file(
        name=path.name, source=path, type="file", move=False
    )
    return _artifact_out(row)


@admin_router.get("/{artifact_id}", response_model=schemas.ArtifactOut, dependencies=[Depends(require_viewer)])
def get_artifact(artifact_id: str, db: Session = Depends(get_db)):
    try:
        return _artifact_out(ArtifactService(db).get_artifact(artifact_id))
    except ArtifactNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": str(exc)}) from exc


@download_router.get("/{artifact_id}/download")
def download_artifact(
    artifact_id: str,
    authorization: str | None = Header(default=None),
    x_admin_token: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    """Artifact bytes for the data plane (V1.5 §13): the Worker downloads its
    input artifacts with the device Bearer token; the admin token also works
    for manual inspection.

    V1.6 P0 0.15 (audit M1): a device Bearer no longer grants every blob.
    Task-bound artifacts are readable only by their task's target worker (or
    the uploading worker itself); unbound artifacts are shared datasets."""
    expected = settings.admin_token
    if expected and x_admin_token and hmac.compare_digest(x_admin_token, expected):
        device = None
    else:
        device = _device_from_bearer(authorization, db)  # raises 401 on a bad token
    try:
        row = ArtifactService(db).get_artifact(artifact_id)
    except ArtifactNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": str(exc)}) from exc
    if device is not None:
        from sqlalchemy import select as _select

        from app.task.db_models import Task
        from app.task.preflight import can_device_read_artifact

        if not can_device_read_artifact(row, device.device_id):
            # task-bound: only the target worker of that task may pull it
            task = db.scalars(
                _select(Task).where(Task.task_id == row.task_id)
            ).first() if row.task_id else None
            if task is None or task.target_device_id != device.device_id:
                raise HTTPException(
                    status_code=403,
                    detail={
                        "code": "artifact_forbidden",
                        "message": "artifact is bound to another task's worker",
                    },
                )
    # storage_path is relative to the artifacts root (V1.4 bug: was relative
    # to storage_dir, so every download 404'd with "artifact blob missing")
    path = ArtifactService.artifacts_root() / row.storage_path
    if not path.is_file():
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": "artifact blob missing"})
    from app.artifact.service import safe_artifact_name

    return FileResponse(
        path,
        media_type=row.mime_type or "application/octet-stream",
        filename=safe_artifact_name(row.name),
    )


@admin_router.delete("/{artifact_id}", status_code=204, dependencies=[Depends(require_admin)])
def delete_artifact(artifact_id: str, db: Session = Depends(get_db)):
    try:
        ArtifactService(db).delete_artifact(artifact_id)
    except ArtifactNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": str(exc)}) from exc
