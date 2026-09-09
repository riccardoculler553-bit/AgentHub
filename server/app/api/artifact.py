"""Artifact APIs (V1.4 §64).

POST /api/artifacts        - Worker upload (device Bearer token), multipart
GET  /api/artifacts        - Admin list (by task/workflow)
GET  /api/artifacts/{id}   - Admin download (artifact bytes)
DELETE /api/artifacts/{id} - Admin delete (lifecycle-controlled)
"""

import hmac

from fastapi import APIRouter, Depends, File, Form, Header, HTTPException, UploadFile
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session

from app.artifact import models as schemas
from app.artifact.service import ArtifactNotFound, ArtifactService
from app.auth.admin import require_admin
from app.core.config import settings
from app.db.database import get_db

from app.api.capability import _device_from_bearer

admin_router = APIRouter(prefix="/api/artifacts", tags=["artifact"], dependencies=[Depends(require_admin)])
upload_router = APIRouter(prefix="/api/artifacts", tags=["artifact"])


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
    content = await file.read()
    row = ArtifactService(db).create_artifact(
        name=name or file.filename or "artifact.bin",
        content=content,
        type=type,
        source_worker_id=worker_id,
        task_id=task_id or None,
        workflow_run_id=workflow_run_id or None,
        step_run_id=step_run_id or None,
    )
    return _artifact_out(row)


@admin_router.get("", response_model=list[schemas.ArtifactOut])
def list_artifacts(
    task_id: str | None = None,
    workflow_run_id: str | None = None,
    limit: int = 100,
    db: Session = Depends(get_db),
):
    return [
        _artifact_out(a)
        for a in ArtifactService(db).list_artifacts(task_id=task_id, workflow_run_id=workflow_run_id, limit=limit)
    ]


@admin_router.get("/{artifact_id}", response_model=schemas.ArtifactOut)
def get_artifact(artifact_id: str, db: Session = Depends(get_db)):
    try:
        return _artifact_out(ArtifactService(db).get_artifact(artifact_id))
    except ArtifactNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": str(exc)}) from exc


@admin_router.get("/{artifact_id}/download")
def download_artifact(artifact_id: str, db: Session = Depends(get_db)):
    try:
        row = ArtifactService(db).get_artifact(artifact_id)
    except ArtifactNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": str(exc)}) from exc
    path = settings.storage_dir / row.storage_path
    if not path.is_file():
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": "artifact blob missing"})
    from app.artifact.service import safe_artifact_name

    return FileResponse(
        path,
        media_type=row.mime_type or "application/octet-stream",
        filename=safe_artifact_name(row.name),
    )


@admin_router.delete("/{artifact_id}", status_code=204)
def delete_artifact(artifact_id: str, db: Session = Depends(get_db)):
    try:
        ArtifactService(db).delete_artifact(artifact_id)
    except ArtifactNotFound as exc:
        raise HTTPException(status_code=404, detail={"code": "artifact_not_found", "message": str(exc)}) from exc
