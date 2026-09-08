"""Workflow runtime: process-wide plumbing between the Task Engine and the
Workflow Engine (V1.3 §28/§29/§121).

- on_task_terminal: the task-terminal-observer callback. Syncs workflow state
  from DB facts in its own session (thread-safe: called from WS handler
  threads / portals), then schedules async dispatch for tasks the engine
  just created or retried.
- schedule_dispatch: hops onto the bound main loop (call_soon_threadsafe) so
  TaskDispatcher runs on the app's event loop from any thread.

Nothing here is authoritative: DB state is. A missed notification is repaired
by the WorkflowMonitor sweep (§28: no full-DB polling loop, just a slow
safety net).
"""

import asyncio
import logging
import threading
from typing import Any

logger = logging.getLogger(__name__)


class WorkflowRuntime:
    def __init__(self) -> None:
        self.hub: Any = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._lock = threading.Lock()

    def bind(self, hub: Any, loop: asyncio.AbstractEventLoop | None = None) -> None:
        self.hub = hub
        self._loop = loop

    # ------------------------------------------------------------- dispatch

    def schedule_dispatch(self, task_id: str) -> None:
        """Dispatch a PENDING task on the main loop (thread-safe)."""
        if self.hub is None:
            return  # no runtime (pure unit test): TaskMonitor sweep covers it
        loop = self._loop
        if loop is None or loop.is_closed():
            return

        def _dispatch() -> None:
            from app.task.dispatcher import TaskDispatcher

            asyncio.ensure_future(TaskDispatcher(self.hub).dispatch_task(task_id))

        try:
            loop.call_soon_threadsafe(_dispatch)
        except RuntimeError:
            pass

    # --------------------------------------------------- task terminal hook

    def on_task_terminal(self, task_id: str) -> None:
        """Observer callback: a task reached a terminal state."""
        try:
            from app.db.database import SessionLocal
            from app.workflow.engine import WorkflowEngine

            with SessionLocal() as db:
                engine = WorkflowEngine(db)
                dispatch_ids = engine.handle_task_result(task_id)
            for tid in dispatch_ids:
                self.schedule_dispatch(tid)
        except Exception:  # noqa: BLE001 - observer boundary (task/events.py)
            logger.exception("workflow advance failed for task %s", task_id)


workflow_runtime = WorkflowRuntime()
