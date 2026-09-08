"""Workflow waiters: in-process run-terminal events (V1.3 §119/§120).

Mirrors task_waiters: notification is an accelerator, DB state stays the
source of truth. A missed notify can never hang a caller - the waiters'
users poll the DB as fallback.
"""

import asyncio
import threading


class WorkflowWaiters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._waiters: dict[str, set[tuple[asyncio.Event, asyncio.AbstractEventLoop]]] = {}

    def register(self, run_id: str) -> asyncio.Event:
        event = asyncio.Event()
        loop = asyncio.get_running_loop()
        with self._lock:
            self._waiters.setdefault(run_id, set()).add((event, loop))
        return event

    def unregister(self, run_id: str, event: asyncio.Event) -> None:
        with self._lock:
            waiters = self._waiters.get(run_id)
            if waiters:
                self._waiters[run_id] = {w for w in waiters if w[0] is not event}
                if not self._waiters[run_id]:
                    self._waiters.pop(run_id, None)

    def notify(self, run_id: str) -> None:
        with self._lock:
            waiters = list(self._waiters.get(run_id, ()))
        for event, loop in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:  # loop already closed
                pass


workflow_waiters = WorkflowWaiters()
