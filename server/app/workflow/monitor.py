"""WorkflowMonitor: slow safety net for workflow runs (V1.3 §83/§120/§124).

The task-terminal observer advances runs in real time; this sweep repairs
what a missed notification (server restart, crash) can leave behind:
- RUNNING runs whose current task already reached a terminal state in the DB
  -> re-sync via the engine (facts only, tasks are never recreated, §54).
- RUNNING steps without a task id -> orphan detection (§56).
- PENDING runs (crash between create and start) -> start them.
- runs with no active step but unfinished steps -> advance.

Deliberately mirrors TaskMonitor's structure; it never re-implements task
timeout/online logic (§84).
"""

import asyncio
import logging

from sqlalchemy import select

from app.core.config import settings
from app.db.database import SessionLocal
from app.workflow.db_models import WorkflowRun
from app.workflow.engine import WorkflowEngine

logger = logging.getLogger(__name__)


class WorkflowMonitor:
    def __init__(self, hub, sweep_interval: float | None = None) -> None:
        self.hub = hub
        self.sweep_interval = sweep_interval or settings.workflow_monitor_interval

    async def run(self) -> None:
        logger.info("WorkflowMonitor started (sweep every %ss)", self.sweep_interval)
        while True:
            try:
                for task_id in self.sweep():
                    self._dispatch(task_id)
            except Exception:
                logger.exception("workflow monitor sweep failed")
            await asyncio.sleep(self.sweep_interval)

    def _dispatch(self, task_id: str) -> None:
        if not task_id or self.hub is None:
            return
        from app.task.dispatcher import TaskDispatcher

        asyncio.ensure_future(TaskDispatcher(self.hub).dispatch_task(task_id))

    def sweep(self) -> list[str]:
        """One repair pass. Returns task ids needing dispatch."""
        dispatch_ids: list[str] = []
        with SessionLocal() as db:
            engine = WorkflowEngine(db)
            active_runs = list(
                db.scalars(
                    select(WorkflowRun).where(WorkflowRun.status.in_(["PENDING", "RUNNING"]))
                )
            )
            for run in active_runs:
                try:
                    dispatch_ids.extend(engine.recover_run(run))
                except Exception:  # noqa: BLE001 - one broken run never blocks the sweep
                    logger.exception("workflow recovery failed for %s", run.run_id)
        return dispatch_ids

    def recover_from_restart(self) -> int:
        """Startup pass (§54). Returns the number of repaired runs."""
        repaired = len(self.sweep())
        if repaired:
            logger.info("workflow recovery: processed %s active run(s)", repaired)
        return repaired
