"""Phase 5 regression: Event/waiter lifecycle closure.

- _wait_terminal's poll loop used asyncio.shield(event.wait()): every timeout
  round leaked one pending Event.wait Task until GC (the 2026-09-15 flood of
  52x "Task was destroyed but it is pending"). Without the shield wait_for
  cancels the waiter itself - zero residue.
- app.core.background.spawn keeps a strong reference to fire-and-forget tasks
  until completion.
"""

import asyncio

import pytest

from app.agent.tools.task import _wait_terminal
from app.core import background
from app.core.background import spawn
from app.core.config import settings


def _event_wait_tasks() -> list[asyncio.Task]:
    out = []
    for task in asyncio.all_tasks():
        coro = task.get_coro()
        if coro is not None and "Event.wait" in getattr(coro, "__qualname__", ""):
            out.append(task)
    return out


@pytest.mark.anyio
async def test_wait_terminal_poll_loop_does_not_leak_waiter_tasks(monkeypatch):
    monkeypatch.setattr(settings, "agent_poll_interval", 0.02, raising=False)
    for _ in range(5):
        status, result = await _wait_terminal("task_does_not_exist_phase5", timeout=0.15)
        assert result is None  # reported honestly, never lied into success
    await asyncio.sleep(0.1)
    assert _event_wait_tasks() == []


@pytest.mark.anyio
async def test_spawn_keeps_strong_reference_until_completion():
    release = asyncio.Event()
    started = asyncio.Event()

    async def work() -> str:
        started.set()
        await release.wait()
        return "done"

    task = spawn(work())
    try:
        await started.wait()
        assert task in background._tasks  # referenced while pending
    finally:
        release.set()
    assert await task == "done"
    await asyncio.sleep(0)
    assert task not in background._tasks  # released after completion
