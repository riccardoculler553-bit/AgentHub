"""CapabilityResolver: capability + version -> Worker (V1.4 §21/§22).

Selection order (deterministic, no clever scheduling in V1.4):
1. explicit worker (task.target_device_id) - must be online
2. online workers reporting the capability in worker_capabilities
   (version-compatible when a version is pinned)
3. fallback: any online worker - Lazy Pull installs on demand (§52)

Candidates are ordered by current live-task load, then worker_id.
"""

import logging

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.capability_runtime.db_models import WorkerCapability
from app.capability_runtime.errors import CapabilityNoWorker
from app.db.models import Device
from app.task.db_models import Task
from app.task.service import LIVE_TASK_STATES

logger = logging.getLogger(__name__)


class CapabilityResolver:
    def __init__(self, db: Session) -> None:
        self.db = db

    def _online_worker_ids(self) -> list[str]:
        return list(
            self.db.scalars(
                select(Device.device_id)
                .where(
                    Device.status == "online",
                    Device.revoked_at.is_(None),
                )
                .order_by(Device.device_id)
            )
        )

    def _order_by_load(self, worker_ids: list[str]) -> list[str]:
        """Lowest live-task count first, then worker_id (stable ordering)."""
        if not worker_ids:
            return []
        counts = dict(
            self.db.execute(
                select(Task.target_device_id, func.count(Task.task_id))
                .where(
                    Task.target_device_id.in_(worker_ids),
                    Task.status.in_(LIVE_TASK_STATES),
                )
                .group_by(Task.target_device_id)
            ).all()
        )
        return sorted(worker_ids, key=lambda wid: (counts.get(wid, 0), wid))

    def resolve_worker(
        self,
        capability_name: str,
        version: str | None,
        explicit_worker_id: str | None = None,
    ) -> str:
        """Return a worker (device_id) able to run the capability.
        Raises CapabilityNoWorker when nothing is eligible."""
        online = set(self._online_worker_ids())

        # 1. explicit worker wins (still must be online)
        if explicit_worker_id:
            if explicit_worker_id in online:
                return explicit_worker_id
            raise CapabilityNoWorker(capability_name, version or "(current)")

        # 2. workers already reporting the capability
        stmt = select(WorkerCapability.worker_id).where(
            WorkerCapability.capability_name == capability_name
        )
        if version:
            stmt = stmt.where(WorkerCapability.version == version)
        reporters = [
            wid for wid in dict.fromkeys(self.db.scalars(stmt).all()) if wid in online
        ]
        ordered = self._order_by_load(reporters)
        if ordered:
            return ordered[0]

        # 3. lazy-pull fallback: any online worker (§52)
        fallback = self._order_by_load(sorted(online))
        if fallback:
            return fallback[0]
        raise CapabilityNoWorker(capability_name, version or "(current)")
