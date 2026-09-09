"""ArtifactService: upload / download / delete + storage layout (V1.4 §30-§32).

Storage: storage/artifacts/YYYY/MM/art_<hex>.<ext> (local disk in V1.4).
Idempotency (§72): an identical checksum within the same (task_id, step_run_id)
returns the existing artifact instead of duplicating the blob.
"""

import mimetypes
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.artifact.db_models import Artifact
from app.core.config import settings
from app.db.models import utcnow

_UNSAFE = {"/", "\\", "..", ":"}


class ArtifactError(Exception):
    pass


class ArtifactNotFound(ArtifactError):
    def __init__(self, artifact_id: str) -> None:
        self.artifact_id = artifact_id
        super().__init__(f"artifact not found: {artifact_id}")


def sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def safe_artifact_name(name: str) -> str:
    """Strip directories/suffixes from a worker-supplied filename."""
    name = (name or "artifact.bin").replace("\\", "/").split("/")[-1].strip()
    cleaned = "".join("_" if ch in _UNSAFE else ch for ch in name)
    cleaned = cleaned.strip("._ ") or "artifact.bin"
    return cleaned[:200]


class ArtifactService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ---------------------------------------------------------------- storage

    @staticmethod
    def artifacts_root() -> Path:
        return settings.storage_dir / "artifacts"

    def create_artifact(
        self,
        *,
        name: str,
        content: bytes,
        type: str = "file",
        source_worker_id: str | None = None,
        task_id: str | None = None,
        workflow_run_id: str | None = None,
        step_run_id: str | None = None,
        checksum: str | None = None,
        mime_type: str | None = None,
    ) -> Artifact:
        """Persist artifact bytes + metadata. Dedupes per (task, step, checksum)."""
        checksum = checksum or sha256_bytes(content)
        if task_id and step_run_id:
            existing = self.db.scalars(
                select(Artifact).where(
                    Artifact.task_id == task_id,
                    Artifact.step_run_id == step_run_id,
                    Artifact.checksum == checksum,
                )
            ).first()
            if existing is not None:
                return existing

        artifact_id = f"art_{uuid4().hex[:16]}"
        now = utcnow()
        safe_name = safe_artifact_name(name)
        relative_dir = Path(f"{now.year:04d}") / f"{now.month:02d}"
        storage_name = f"{artifact_id}{Path(safe_name).suffix[:16]}"
        relative_path = relative_dir / storage_name

        target = self.artifacts_root() / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)

        row = Artifact(
            artifact_id=artifact_id,
            name=safe_name,
            type=type,
            mime_type=mime_type or mimetypes.guess_type(safe_name)[0],
            size=len(content),
            storage_path=str(relative_path).replace("\\", "/"),
            checksum=checksum,
            source_worker_id=source_worker_id,
            task_id=task_id,
            workflow_run_id=workflow_run_id,
            step_run_id=step_run_id,
            created_at=now,
        )
        self.db.add(row)
        self.db.commit()
        return row

    def get_artifact(self, artifact_id: str) -> Artifact:
        row = self.db.scalars(
            select(Artifact).where(Artifact.artifact_id == artifact_id)
        ).first()
        if row is None:
            raise ArtifactNotFound(artifact_id)
        return row

    def read_artifact_bytes(self, artifact_id: str) -> tuple[Artifact, bytes]:
        row = self.get_artifact(artifact_id)
        path = settings.storage_dir / row.storage_path
        try:
            return row, path.read_bytes()
        except OSError as exc:
            raise ArtifactNotFound(artifact_id) from exc

    def delete_artifact(self, artifact_id: str) -> None:
        row = self.get_artifact(artifact_id)
        path = settings.storage_dir / row.storage_path
        try:
            path.unlink(missing_ok=True)
        except OSError:
            pass  # blob already gone; metadata deletion still proceeds
        self.db.delete(row)
        self.db.commit()

    def list_artifacts(
        self,
        task_id: str | None = None,
        workflow_run_id: str | None = None,
        limit: int = 100,
    ) -> list[Artifact]:
        stmt = select(Artifact).order_by(Artifact.id.desc()).limit(max(1, min(limit, 500)))
        if task_id:
            stmt = stmt.where(Artifact.task_id == task_id)
        if workflow_run_id:
            stmt = stmt.where(Artifact.workflow_run_id == workflow_run_id)
        return list(self.db.scalars(stmt))

    def purge_expired(self, now: datetime | None = None) -> int:
        """Best-effort sweep for expired artifacts (lifecycle control, §64)."""
        now = now or datetime.now(UTC).replace(tzinfo=None)
        rows = list(self.db.scalars(select(Artifact).where(Artifact.expires_at.is_not(None))))
        removed = 0
        for row in rows:
            if row.expires_at and row.expires_at <= now:
                try:
                    self.delete_artifact(row.artifact_id)
                    removed += 1
                except ArtifactNotFound:
                    pass
        return removed
