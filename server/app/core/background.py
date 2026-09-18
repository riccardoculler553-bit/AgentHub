"""Strong references for fire-and-forget background tasks (Phase 5).

asyncio holds only weak references to Tasks: a bare ``asyncio.create_task(c)``
whose result nobody stores can be garbage-collected mid-flight - killing the
coroutine AND every pending Event.wait child it owns (the 2026-09-15
"Task was destroyed but it is pending" flood had two causes; this was the
aggravating one). ``spawn()`` keeps a strong reference until completion.
"""

import asyncio
import logging

logger = logging.getLogger(__name__)

_tasks: set[asyncio.Task] = set()


def spawn(coro, *, name: str | None = None) -> asyncio.Task:
    """create_task + strong reference + auto-release on completion."""
    task = asyncio.create_task(coro, name=name)
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
    return task


def pending_count() -> int:
    """Introspection helper (metrics/logging)."""
    return len(_tasks)
