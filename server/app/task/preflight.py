"""Preflight: read-only checks BEFORE a dispatch creates an Attempt (V1.6 §3.6).

Today's dispatcher validated in three silent places (resolver / busy lock /
artifact lookup) and only after burning an Attempt row. Preflight centralises
the checks so a dispatch either passes cleanly or fails with an actionable
error_code - without creating an attempt ("不空跑 Attempt").

P0 checklist (§3.6):
- device ONLINE (Hub truth) and not revoked          -> DEVICE_OFFLINE
- scheduling READY (no other LIVE task on it)        -> DEVICE_BUSY
- pinned version PUBLISHED + checksum known and
  consistent with the Task row (0.13 pin)            -> PIN_MISMATCH / PACKAGE_MISSING
- capability ad fresher than TTL (unless the device
  was explicitly named - operator unlocks Lazy Pull) -> AD_STALE
- input artifacts exist + target may read them       -> ARTIFACT_NOT_FOUND / ARTIFACT_FORBIDDEN
"""

from datetime import timedelta

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.artifact.db_models import Artifact
from app.artifact.service import ArtifactNotFound, ArtifactService
from app.capability_runtime.db_models import CapabilityVersion
from app.capability_runtime.worker_registry import WorkerCapabilityService
from app.core.config import settings
from app.db.models import utcnow
from app.db.models import Device
from app.task.db_models import Task
from app.task.service import LIVE_TASK_STATES


class PreflightFailed(Exception):
    def __init__(self, code: str, message: str, blocking_task_id: str | None = None) -> None:
        self.code = code
        self.message = message
        self.blocking_task_id = blocking_task_id
        super().__init__(f"{code}: {message}")


def can_device_read_artifact(artifact: Artifact, device_id: str) -> bool:
    """V1.6 P0 0.15 ACL rule, shared by preflight and the download endpoint:
    - unbound artifacts (no task) are shared datasets: every device reads them
    - task-bound artifacts are readable by the task's target worker
    - the uploading worker can always re-read its own upload"""
    if artifact.task_id is None:
        return True
    if artifact.source_worker_id == device_id:
        return True
    return False


def check_capability_dispatch(
    db: Session,
    hub,
    task: Task,
    device_id: str,
    version: CapabilityVersion,
    *,
    explicit_device: bool,
) -> None:
    """Raise PreflightFailed on the first violated check; return None when the
    dispatch may proceed. Read-only: no commits, no state changes."""
    device = db.scalars(select(Device).where(Device.device_id == device_id)).first()
    if device is None or device.revoked_at is not None:
        raise PreflightFailed("DEVICE_UNAVAILABLE", f"device {device_id} is missing or revoked")
    if hub is not None and not hub.is_device_online(device_id):
        raise PreflightFailed("DEVICE_OFFLINE", f"device {device_id} holds no live connection")

    # scheduling axis (§3.1): one live task per device; self-conflicts (retry
    # sweep racing the terminal result) are excluded.
    blocking = db.scalars(
        select(Task.task_id).where(
            Task.target_device_id == device_id,
            Task.status.in_(LIVE_TASK_STATES),
            Task.task_id != task.task_id,
        )
    ).first()
    if blocking is not None:
        raise PreflightFailed(
            "DEVICE_BUSY",
            f"device {device_id} is busy with task {blocking}",
            blocking_task_id=blocking,
        )

    # pin integrity (§3.3 / 0.13): the Task row and the version row must agree
    if version.status != "PUBLISHED":
        raise PreflightFailed("PIN_MISMATCH", f"version {version.version} is {version.status}, not PUBLISHED")
    if not version.checksum:
        raise PreflightFailed("PIN_MISMATCH", f"version {version.version} has no checksum")
    if task.package_checksum and task.package_checksum != version.checksum:
        raise PreflightFailed(
            "PIN_MISMATCH",
            "task pin drifted from the version row "
            f"({task.package_checksum[:12]}... != {version.checksum[:12]}...)",
        )

    # ad freshness (0.10): a resolved worker must have advertised the
    # capability recently; an explicitly named device may Lazy Pull instead.
    if not explicit_device:
        cutoff = utcnow() - timedelta(seconds=settings.worker_ad_ttl)
        ads = WorkerCapabilityService(db).get_worker_capabilities(device_id)
        fresh = any(
            ad.capability_name == task.capability_name
            and ad.last_seen_at is not None
            and ad.last_seen_at >= cutoff
            for ad in ads
        )
        if not fresh:
            raise PreflightFailed(
                "AD_STALE",
                f"device {device_id} has no fresh ad for {task.capability_name}",
            )

    # input artifacts (0.12 checklist + 0.15 ACL)
    artifact_service = ArtifactService(db)
    for ref in task.artifact_ids or []:
        if isinstance(ref, str):  # defensive: legacy plain-id entries
            ref = {"artifact_id": ref}
        artifact_id = str(ref.get("artifact_id", ""))
        try:
            artifact = artifact_service.get_artifact(artifact_id)
        except ArtifactNotFound as exc:
            raise PreflightFailed("ARTIFACT_NOT_FOUND", f"input artifact missing: {artifact_id}") from exc
        if not can_device_read_artifact(artifact, device_id):
            # bound to another task and not uploaded by this worker
            raise PreflightFailed(
                "ARTIFACT_FORBIDDEN",
                f"device {device_id} may not read artifact {artifact_id}",
            )
