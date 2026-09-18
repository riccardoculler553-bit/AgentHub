"""Phase 3/4 regression: async run_capability + proactive terminal notification.

- run_capability defaults to async: returns CONFIGURED immediately, links
  Task -> AgentRun, never blocks the LLM tool loop.
- Terminal notification fires exactly once when the agent run has already
  finished before the task; skipped while the run is still open.
"""

import asyncio

import pytest
from sqlalchemy import select

from app.agent import notify as notify_mod
from app.agent.db_models import AgentRun
from app.agent.notify import install_agent_task_notifier
from app.agent.runs import AgentRunService
from app.agent.tools.context import current_run_id
from app.agent.tools.registry import build_default_registry
from app.core.config import settings
from app.db.database import SessionLocal

from ._worker import FakeWorker, register_device, wait_until
from .test_workflow_capability import _publish_capability


@pytest.fixture()
def hub(client):
    return client.app.state.hub


@pytest.fixture()
def registry(hub, monkeypatch):
    monkeypatch.setattr(settings, "agent_tool_wait_max", 15, raising=False)
    return build_default_registry(hub)


@pytest.fixture()
def device(client):
    payload = register_device(client, "通知测试机")
    from app.capability.service import CapabilityService

    with SessionLocal() as db:
        CapabilityService(db).replace_device_capabilities(
            payload["device_id"], [{"name": "echo", "version": "1.0"}]
        )
    return payload


class _RecordingSender:
    def __init__(self) -> None:
        self.replies: list[dict] = []

    async def send_reply(self, *, channel: str, conversation_id: str, text: str, webhook) -> None:
        self.replies.append(
            {"channel": channel, "conversation_id": conversation_id, "text": text, "webhook": webhook}
        )


@pytest.fixture()
def notifier():
    """Install the terminal notifier with a recording sender; neutralize it
    after the test (the listener itself stays subscribed but inert)."""
    notify_mod._dedupe.clear()
    sender = _RecordingSender()
    install_agent_task_notifier(sender)
    yield sender
    notify_mod._sender = None
    notify_mod._dedupe.clear()


def _make_run(input_text: str, *, status: str = "FAILED") -> str:
    """Create an AgentRun row; returns its generated run_id."""
    with SessionLocal() as db:
        run = AgentRunService(db).create(
            channel="dingtalk",
            message_id=f"msg_{input_text}",
            conversation_id=f"cid_{input_text}",
            sender_id="sender_1",
            sender_name="测试用户",
            input_text=input_text,
        )
        run_id = run.run_id
        if status != "RUNNING":
            AgentRunService(db).finish(run_id=run_id, status=status, final_reply="先去忙了")
    return run_id


def _task_status(task_id: str) -> str:
    from app.task.db_models import Task

    with SessionLocal() as db:
        row = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        return row.status if row else "<missing>"


async def _call_run(registry, args: dict, run_id: str):
    tool = registry.get("run_capability")
    assert tool is not None
    with SessionLocal() as db:
        token = current_run_id.set(run_id)
        try:
            return await tool.run(db, args)
        finally:
            current_run_id.reset(token)


@pytest.mark.anyio
async def test_run_capability_async_returns_configured_and_links_run(client, registry, device):
    """Default (wait=False): the tool returns CONFIGURED immediately - even
    with no worker connected - and Task -> AgentRun is linked for the
    terminal notification."""
    _publish_capability(client)
    run_id = _make_run("notify-link", status="FAILED")  # finished run: the gap case

    r = await _call_run(registry, {"capability": "erp.order.export", "device": "通知测试机"}, run_id)
    assert r.success, r
    assert r.data["status"] == "CONFIGURED"
    task_id = r.data["task_id"]

    with SessionLocal() as db:
        run = db.scalars(select(AgentRun).where(AgentRun.run_id == run_id)).first()
        assert run.task_id == task_id


@pytest.mark.anyio
async def test_terminal_notification_sent_once_after_run_finished(client, registry, device, notifier):
    """Production gap closed: run finished first, task terminates later -> the
    group gets exactly one completion push; the same terminal state never
    notifies twice."""
    _publish_capability(client)
    run_id = _make_run("notify-success", status="FAILED")

    r = await _call_run(registry, {"capability": "erp.order.export"}, run_id)
    assert r.success and r.data["status"] == "CONFIGURED"
    task_id = r.data["task_id"]

    # No worker connected: drive the task terminal through the real cancel
    # path (which also broadcasts the terminal fact).
    res = client.post(f"/api/tasks/{task_id}/cancel")
    assert res.status_code == 200, res.text
    assert wait_until(lambda: _task_status(task_id) == "CANCELLED", timeout=5)

    assert wait_until(lambda: len(notifier.replies) == 1, timeout=5)
    text = notifier.replies[0]["text"]
    assert task_id in text and "已取消" in text

    # idempotent: re-firing the same terminal fact never pushes again
    from app.task.events import notify_task_terminal

    notify_task_terminal(task_id)
    await asyncio.sleep(0.2)
    assert len(notifier.replies) == 1


@pytest.mark.anyio
async def test_no_notification_while_run_still_open(client, registry, device, notifier):
    """The agent is still running (inline wait) -> the agent itself reports the
    result; the proactive layer must stay silent (no double messaging)."""
    _publish_capability(client)
    run_id = _make_run("notify-open", status="RUNNING")

    worker = FakeWorker(client, device["device_token"], behaviour="success")
    worker.start()
    try:
        r = await _call_run(
            registry,
            {"capability": "erp.order.export", "device": "通知测试机", "wait": True},
            run_id,
        )
        assert r.success and r.data["status"] == "SUCCESS"
        task_id = r.data["task_id"]
        assert wait_until(lambda: _task_status(task_id) == "SUCCESS", timeout=5)
        await asyncio.sleep(0.3)  # allow a (wrong) background notification to fire
        assert notifier.replies == []  # the agent reports it - no duplicate push
    finally:
        worker.stop()
