"""WorkerService: environment inventory + process instance registry (V1.7)."""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.models import utcnow
from app.worker.db_models import WorkerEnvironment, WorkerProcess

PROCESS_STATUSES = {"STARTING", "RUNNING", "STOPPING", "STOPPED", "FAILED"}


class WorkerService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ------------------------------------------------------------ environment

    def upsert_environment(self, device_id: str, snapshot: dict) -> WorkerEnvironment:
        """Store the latest environment report; the fingerprint drives drift
        detection (§28): a changed fingerprint is visible server-side."""
        row = self.db.scalars(
            select(WorkerEnvironment).where(WorkerEnvironment.device_id == device_id)
        ).first()
        fingerprint = snapshot.get("fingerprint")
        hostname = snapshot.get("hostname")
        worker_version = (snapshot.get("worker") or {}).get("version")
        if row is None:
            row = WorkerEnvironment(device_id=device_id)
            self.db.add(row)
        row.snapshot = snapshot
        row.fingerprint = fingerprint
        row.hostname = hostname
        row.worker_version = worker_version
        row.collected_at = utcnow()
        self.db.commit()
        return row

    def get_environment(self, device_id: str) -> WorkerEnvironment | None:
        return self.db.scalars(
            select(WorkerEnvironment).where(WorkerEnvironment.device_id == device_id)
        ).first()

    def environment_drift(self, device_id: str, fingerprint: str) -> bool:
        """True when the last stored fingerprint differs (§28)."""
        row = self.get_environment(device_id)
        return row is not None and row.fingerprint is not None and row.fingerprint != fingerprint

    # -------------------------------------------------------------- processes

    def get_process(self, process_id: str) -> WorkerProcess | None:
        return self.db.scalars(
            select(WorkerProcess).where(WorkerProcess.process_id == process_id)
        ).first()

    def get_process_by_target(self, device_id: str, capability: str, version: str) -> WorkerProcess | None:
        return self.db.scalars(
            select(WorkerProcess).where(
                WorkerProcess.device_id == device_id,
                WorkerProcess.capability == capability,
                WorkerProcess.version == version,
            )
        ).first()

    def list_processes(self, device_id: str | None = None, status: str | None = None, limit: int = 100) -> list[WorkerProcess]:
        stmt = select(WorkerProcess).order_by(WorkerProcess.id.desc()).limit(max(1, min(limit, 500)))
        if device_id:
            stmt = stmt.where(WorkerProcess.device_id == device_id)
        if status:
            stmt = stmt.where(WorkerProcess.status == status)
        return list(self.db.scalars(stmt))

    def create_process(
        self,
        device_id: str,
        capability: str,
        version: str,
        *,
        package_id: str | None = None,
        requested_by: str | None = None,
    ) -> WorkerProcess:
        """Idempotent per (device, capability, version): an existing live
        instance is returned instead of a second one."""
        from uuid import uuid4

        existing = self.get_process_by_target(device_id, capability, version)
        if existing is not None and existing.status in ("STARTING", "RUNNING", "STOPPING"):
            return existing
        if existing is not None:
            # reuse the row for the same target (restart of a STOPPED instance)
            existing.status = "STARTING"
            existing.requested_by = requested_by
            existing.last_error = None
            existing.stopped_at = None
            existing.pid = None
            self.db.commit()
            return existing
        row = WorkerProcess(
            process_id=f"proc_{uuid4().hex[:16]}",
            device_id=device_id,
            capability=capability,
            version=version,
            package_id=package_id,
            status="STARTING",
            requested_by=requested_by,
        )
        self.db.add(row)
        self.db.commit()
        return row

    def update_process_status(
        self,
        process_id: str,
        status: str,
        *,
        pid: int | None = None,
        error: str | None = None,
        device_reported: bool = False,
    ) -> WorkerProcess | None:
        """Device reports or server-side transition. STARTING/RUNNING only
        move forward from the DEVICE's word (the OS pid lives there); the
        server may only mark STOPPING on a stop request."""
        if status not in PROCESS_STATUSES:
            raise ValueError(f"invalid process status: {status}")
        row = self.get_process(process_id)
        if row is None:
            return None
        now = utcnow()
        row.status = status
        if pid is not None:
            row.pid = pid
        if error is not None:
            row.last_error = error[:500]
        if status == "RUNNING":
            row.started_at = row.started_at or now
            row.last_health_at = now
            row.stopped_at = None
        elif status in ("STOPPED", "FAILED"):
            row.stopped_at = now
        self.db.commit()
        return row

    def health_touch(self, process_id: str) -> WorkerProcess | None:
        row = self.get_process(process_id)
        if row is not None:
            row.last_health_at = utcnow()
            self.db.commit()
        return row
