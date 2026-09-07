"""TaskWaiters: in-process completion events for long-running callers.

The MVP agent (DingTalk flow) awaits a task's terminal state without a
`while: sleep(1)` polling loop (MVP-Real §103-104). TaskService.notify()
fires the event when a terminal task.result is applied.

Deliberately NOT persistence: MySQL stays the authoritative task state; the
waiter only accelerates the current process. DB polling remains the fallback
(cross-loop / cross-process safety), so a missed notification can never hang
a run forever.
"""

import asyncio
import threading


class TaskWaiters:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # task_id -> {(event, loop that registered it)}
        self._waiters: dict[str, set[tuple[asyncio.Event, asyncio.AbstractEventLoop]]] = {}

    def register(self, task_id: str) -> asyncio.Event:
        event = asyncio.Event()
        loop = asyncio.get_running_loop()
        with self._lock:
            self._waiters.setdefault(task_id, set()).add((event, loop))
        return event

    def unregister(self, task_id: str, event: asyncio.Event) -> None:
        with self._lock:
            waiters = self._waiters.get(task_id)
            if waiters:
                self._waiters[task_id] = {w for w in waiters if w[0] is not event}
                if not self._waiters[task_id]:
                    self._waiters.pop(task_id, None)

    def notify(self, task_id: str) -> None:
        """Set every waiter for the task. Thread-safe: the event is set on the
        loop that registered it, so callers from WS handler loops / portals work."""
        with self._lock:
            waiters = list(self._waiters.get(task_id, ()))
        for event, loop in waiters:
            try:
                loop.call_soon_threadsafe(event.set)
            except RuntimeError:  # loop already closed
                pass


task_waiters = TaskWaiters()
