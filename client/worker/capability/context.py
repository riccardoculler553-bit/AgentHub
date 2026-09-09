"""ExecutionContext: what a Runtime receives per execution (V1.4 §41).

Every capability execution carries the same context - Runtimes never read the
raw dispatch envelope. The context is also persisted as context.json in the
execution dir so a package can log/debug against it.
"""

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

from worker.capability.manifest import Manifest
from worker.capability.cache import work_root


@dataclass
class ExecutionContext:
    execution_id: str
    task_id: str
    step_id: str
    attempt_id: str
    capability: str
    version: str
    worker_id: str
    params: dict = field(default_factory=dict)
    manifest: Manifest | None = None
    package_dir: Path | None = None
    workflow_run_id: str | None = None
    step_run_id: str | None = None
    timeout: int = 600
    started_at: str = ""

    def __post_init__(self) -> None:
        if not self.started_at:
            self.started_at = datetime.now(UTC).isoformat(timespec="seconds")

    def execution_dir(self) -> Path:
        """Per-execution scratch/output dir: <work>/executions/<execution_id>."""
        path = work_root() / "executions" / self.execution_id
        path.mkdir(parents=True, exist_ok=True)
        return path

    def to_dict(self) -> dict:
        return {
            "execution_id": self.execution_id,
            "task_id": self.task_id,
            "step_id": self.step_id,
            "attempt_id": self.attempt_id,
            "workflow_run_id": self.workflow_run_id,
            "step_run_id": self.step_run_id,
            "capability": self.capability,
            "version": self.version,
            "worker_id": self.worker_id,
            "params": self.params,
            "package_dir": str(self.package_dir) if self.package_dir else "",
            "timeout": self.timeout,
            "started_at": self.started_at,
        }

    def write_context(self) -> Path:
        path = self.execution_dir() / "context.json"
        path.write_text(json.dumps(self.to_dict(), ensure_ascii=False, indent=2), encoding="utf-8")
        return path
