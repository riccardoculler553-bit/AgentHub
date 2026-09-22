"""Execution history reader (V1.7 §33-§37).

Runtime facts come from the EXISTING task/attempt tables - never a second
execution ledger (§48). For a capability we compute, per device:

    successful-attempt count, mean duration, mean duration per input MB

Input size joins the artifacts referenced by task.artifact_ids (best effort:
inputs registered without a task binding contribute nothing).
"""

from datetime import datetime

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.artifact.db_models import Artifact
from app.task.db_models import Task, TaskAttempt


class DeviceRuntimeStats:
    def __init__(self, count: int, mean_duration_sec: float, mean_duration_per_mb: float | None) -> None:
        self.count = count
        self.mean_duration_sec = mean_duration_sec
        self.mean_duration_per_mb = mean_duration_per_mb


def _duration_sec(started: datetime | None, finished: datetime | None) -> float | None:
    if started is None or finished is None:
        return None
    seconds = (finished - started).total_seconds()
    return seconds if seconds > 0 else None


class ExecutionHistory:
    def __init__(self, db: Session) -> None:
        self.db = db

    def device_stats(self, capability_name: str, version: str | None = None) -> dict[str, DeviceRuntimeStats]:
        """Successful CAPABILITY executions grouped by target device."""
        rows = self.db.execute(
            select(Task, TaskAttempt)
            .join(TaskAttempt, TaskAttempt.task_id == Task.task_id)
            .where(
                Task.execution_type == "CAPABILITY",
                Task.capability_name == capability_name,
                TaskAttempt.status == "SUCCESS",
                TaskAttempt.started_at.isnot(None),
                TaskAttempt.finished_at.isnot(None),
            )
            .order_by(TaskAttempt.id.desc())
            .limit(200)  # bounded history window: recent runs dominate
        ).all()
        if version:
            rows = [(t, a) for t, a in rows if t.capability_version == version]

        input_sizes = self._input_size_by_task([t for t, _ in rows])
        per_device: dict[str, dict] = {}
        for task, attempt in rows:
            device_id = attempt.device_id or task.target_device_id
            if not device_id:
                continue
            duration = _duration_sec(attempt.started_at, attempt.finished_at)
            if duration is None:
                continue
            bucket = per_device.setdefault(device_id, {"durations": [], "durations_per_mb": []})
            bucket["durations"].append(duration)
            input_mb = input_sizes.get(task.task_id)
            if input_mb and input_mb >= 1:  # below 1MB the per-MB rate is noise
                bucket["durations_per_mb"].append(duration / input_mb)

        stats: dict[str, DeviceRuntimeStats] = {}
        for device_id, bucket in per_device.items():
            durations = bucket["durations"]
            per_mb = bucket["durations_per_mb"]
            stats[device_id] = DeviceRuntimeStats(
                count=len(durations),
                mean_duration_sec=sum(durations) / len(durations),
                mean_duration_per_mb=(sum(per_mb) / len(per_mb)) if per_mb else None,
            )
        return stats

    def input_size_mb(self, task: Task) -> float:
        """Total input MB for one task (executor-path input, §41)."""
        if not task.task_id:
            return 0.0
        return self._input_size_by_task([task]).get(task.task_id, 0.0)

    def _input_size_by_task(self, tasks: list[Task]) -> dict[str, float]:
        """task_id -> total input MB (from the artifacts the task references)."""
        refs: dict[str, list[str]] = {}
        for task in tasks:
            ids = [
                str(ref.get("artifact_id"))
                for ref in (task.artifact_ids or [])
                if isinstance(ref, dict) and ref.get("artifact_id")
            ]
            if ids:
                refs[task.task_id] = ids
        if not refs:
            return {}
        flat = {aid for ids in refs.values() for aid in ids}
        sizes = {
            row[0]: row[1]
            for row in self.db.execute(
                select(Artifact.artifact_id, Artifact.size).where(Artifact.artifact_id.in_(flat))
            ).all()
        }
        result: dict[str, float] = {}
        for task_id, ids in refs.items():
            total_mb = sum((sizes.get(aid) or 0) for aid in ids) / (1024 * 1024)
            result[task_id] = total_mb
        return result
