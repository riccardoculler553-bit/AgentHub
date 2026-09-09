"""Pydantic schemas for the Artifact APIs (V1.4 §64)."""

from datetime import datetime

from pydantic import BaseModel


class ArtifactOut(BaseModel):
    artifact_id: str
    name: str
    type: str
    mime_type: str | None
    size: int
    checksum: str
    source_worker_id: str | None
    task_id: str | None
    workflow_run_id: str | None
    step_run_id: str | None
    created_at: datetime
    expires_at: datetime | None
