"""WorkerCapabilityService: which Worker reports which automation capability.

Worker-side installed packages are reported via the worker.capabilities
envelope (registration + after each install). Server stores them here and the
CapabilityResolver consumes them for worker selection (V1.4 §17/§63).
"""

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.capability_runtime.db_models import WorkerCapability
from app.db.models import utcnow


class WorkerCapabilityService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def replace_worker_capabilities(self, worker_id: str, capabilities: list[dict]) -> int:
        """Upsert the full installed-capability set reported by a Worker."""
        self.db.execute(
            delete(WorkerCapability).where(WorkerCapability.worker_id == worker_id)
        )
        seen: set[tuple[str, str]] = set()
        for item in capabilities or []:
            name = str(item.get("name", "")).strip()
            version = str(item.get("version", "")).strip()
            if not name or not version or (name, version) in seen:
                continue
            seen.add((name, version))
            self.db.add(
                WorkerCapability(
                    worker_id=worker_id,
                    capability_name=name,
                    version=version,
                    status="READY",
                    last_seen_at=utcnow(),
                )
            )
        self.db.commit()
        return len(seen)

    def mark_worker_status(self, worker_id: str, capability_name: str, version: str, status: str) -> bool:
        """Point update (e.g. DOWNLOAD_FAILED) without a full replace."""
        row = self.db.scalars(
            select(WorkerCapability).where(
                WorkerCapability.worker_id == worker_id,
                WorkerCapability.capability_name == capability_name,
                WorkerCapability.version == version,
            )
        ).first()
        if row is None:
            return False
        row.status = status
        row.last_seen_at = utcnow()
        self.db.commit()
        return True

    def get_worker_capabilities(self, worker_id: str) -> list[WorkerCapability]:
        return list(
            self.db.scalars(
                select(WorkerCapability)
                .where(WorkerCapability.worker_id == worker_id)
                .order_by(WorkerCapability.capability_name, WorkerCapability.version)
            )
        )

    def list_all(self) -> list[dict]:
        rows = self.db.scalars(
            select(WorkerCapability).order_by(
                WorkerCapability.worker_id, WorkerCapability.capability_name
            )
        )
        grouped: dict[str, list[dict]] = {}
        for row in rows:
            grouped.setdefault(row.worker_id, []).append(
                {"name": row.capability_name, "version": row.version, "status": row.status,
                 "last_seen_at": row.last_seen_at}
            )
        return [{"worker_id": worker_id, "capabilities": caps} for worker_id, caps in grouped.items()]

    def forget_worker(self, worker_id: str) -> None:
        self.db.execute(delete(WorkerCapability).where(WorkerCapability.worker_id == worker_id))
        self.db.commit()
