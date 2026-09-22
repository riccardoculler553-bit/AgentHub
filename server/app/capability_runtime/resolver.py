"""CapabilityResolver: capability + version -> Worker (V1.4 §21/§22; V1.6 §3.5).

Selection order (deterministic resolution, NOT a cluster scheduler):
1. explicit worker (task.target_device_id) - must be Hub-online and not revoked
2. workers advertising the capability in worker_capabilities
   (version-compatible when pinned, ad fresher than the TTL)
3. READY (no live task) preferred; ties broken by live-task load then id

V1.6 P0 0.10 changes:
- online truth = the Hub (live connections), not the lagging DB status
- expired worker_capabilities ads (older than WORKER_AD_TTL) never select
- the "any online worker" lazy-pull fallback is REMOVED: a device that never
  advertised the capability is not silently chosen. Lazy Pull remains
  available for an explicitly named device (operator intent).
"""

import logging
from datetime import timedelta

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.capability_runtime.db_models import WorkerCapability
from app.capability_runtime.errors import CapabilityNoWorker
from app.core.config import settings
from app.db.models import Device, utcnow
from app.task.db_models import Task
from app.task.service import LIVE_TASK_STATES

logger = logging.getLogger(__name__)


class CapabilityResolver:
    def __init__(self, db: Session, hub=None) -> None:
        self.db = db
        self.hub = hub

    def _online_worker_ids(self) -> list[str]:
        """Hub-truth online (V1.6 P0 0.8): a device is schedulable iff it is
        not revoked AND holds >= 1 live connection. Without a hub (unit-test
        construction) the lagging DB heartbeat view is the fallback."""
        candidates = list(
            self.db.scalars(
                select(Device.device_id)
                .where(Device.revoked_at.is_(None))
                .order_by(Device.device_id)
            )
        )
        if self.hub is None:
            offline = set(
                self.db.scalars(
                    select(Device.device_id).where(Device.status != "online")
                )
            )
            return [wid for wid in candidates if wid not in offline]
        return [wid for wid in candidates if self.hub.is_device_online(wid)]

    def _fresh_reporters(self, capability_name: str, version: str | None) -> list[str]:
        """Workers whose capability ad is version-compatible AND fresher than
        the ad TTL (V1.6 P0 0.10: stale ads never select)."""
        cutoff = utcnow() - timedelta(seconds=settings.worker_ad_ttl)
        stmt = select(WorkerCapability.worker_id).where(
            WorkerCapability.capability_name == capability_name,
            WorkerCapability.last_seen_at.is_not(None),
            WorkerCapability.last_seen_at >= cutoff,
        )
        if version:
            stmt = stmt.where(WorkerCapability.version == version)
        return list(dict.fromkeys(self.db.scalars(stmt).all()))

    def _live_counts(self, worker_ids: list[str]) -> dict[str, int]:
        if not worker_ids:
            return {}
        return dict(
            self.db.execute(
                select(Task.target_device_id, func.count(Task.task_id))
                .where(
                    Task.target_device_id.in_(worker_ids),
                    Task.status.in_(LIVE_TASK_STATES),
                )
                .group_by(Task.target_device_id)
            ).all()
        )

    def _order_by_load(self, worker_ids: list[str]) -> list[str]:
        """READY first (zero live tasks), then lowest live-task count, then
        worker_id (stable ordering)."""
        if not worker_ids:
            return []
        counts = self._live_counts(worker_ids)
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

        # 1. explicit worker wins (still must be online; operator intent
        #    unlocks Lazy Pull there - preflight/worker handle the install)
        if explicit_worker_id:
            if explicit_worker_id in online:
                return explicit_worker_id
            raise CapabilityNoWorker(capability_name, version or "(current)")

        # 2. workers advertising the capability (ad must be fresh)
        reporters = [wid for wid in self._fresh_reporters(capability_name, version) if wid in online]
        ordered = self._order_by_load(reporters)
        if ordered:
            return ordered[0]

        # 3. no arbitrary-online lazy-pull fallback: selecting a worker that
        #    never advertised the capability silently violates the pin (§3.3).
        #    Staying unresolved keeps the task PENDING (bounded wait) so an
        #    operator can install/point the capability instead.
        raise CapabilityNoWorker(capability_name, version or "(current)")
