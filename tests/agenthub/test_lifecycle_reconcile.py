"""Phase 2 regression: Task/Worker lifecycle has no permanent holes.

Server side: an attempt may never outlive its task (reconcile sweep), the
offline-max-wait window restarts on every PENDING entry, and a SENT task whose
device dropped gets a bounded wait instead of the full timeout.

Worker side: a cancel arriving while a task is queued closes it CANCELLED
without executing, and duplicate dispatches echo the truthful phase
(accept = received, not executing).
"""

import asyncio
import uuid
from datetime import timedelta

import pytest
from sqlalchemy import select

try:
    from agenthub._worker import FakeWorker, register_device
except ImportError:  # pragma: no cover - depends on pytest import mode
    from _worker import FakeWorker, register_device

from app.capability.db_models import DeviceCapability
from app.core.config import settings
from app.db.database import SessionLocal
from app.db.models import utcnow
from app.task.db_models import Task, TaskAttempt, TaskEvent, TaskStep
from app.task.monitor import TaskMonitor
from app.task.service import TaskService

import protocol
from worker.ledger import ExecutionLedger
from worker.manager import TaskManager


# ----------------------------------------------------------------- server side


def _plant_attempt(
    device_id: str,
    *,
    task_status: str = "CANCELLED",
    finished_delta: float = 0.0,
    superseded: bool = False,
    attempt_status: str = "RUNNING",
):
    """Insert a task + step + attempt directly; returns (task_id, attempt_id)."""
    now = utcnow()
    suffix = uuid.uuid4().hex[:10]
    task_id, step_id, attempt_id = f"task_{suffix}", f"step_{suffix}", f"attempt_{suffix}"
    with SessionLocal() as db:
        task = Task(
            task_id=task_id,
            name="reconcile-fixture",
            target_device_id=device_id,
            status=task_status,
            execution_type="LEGACY_COMMAND",
            created_at=now,
            finished_at=(now - timedelta(seconds=finished_delta)) if task_status in ("CANCELLED", "SUCCESS", "FAILED", "TIMEOUT") else None,
        )
        step = TaskStep(
            step_id=step_id,
            task_id=task_id,
            command="echo",
            status="RUNNING",
            current_attempt_id=None,
        )
        attempt = TaskAttempt(
            attempt_id=attempt_id,
            task_id=task_id,
            step_id=step_id,
            device_id=device_id,
            status=attempt_status,
            created_at=now,
        )
        step.current_attempt_id = f"attempt_newer_{suffix}" if superseded else attempt.attempt_id
        db.add_all([task, step, attempt])
        db.commit()
    return task_id, attempt_id


def _attempt_status(attempt_id: str) -> str:
    with SessionLocal() as db:
        row = db.scalars(
            select(TaskAttempt).where(TaskAttempt.attempt_id == attempt_id)
        ).first()
        return row.status if row else "<missing>"


def _events(task_id: str, event_type: str) -> list[TaskEvent]:
    with SessionLocal() as db:
        return list(
            db.scalars(
                select(TaskEvent).where(
                    TaskEvent.task_id == task_id, TaskEvent.event_type == event_type
                )
            )
        )


def test_reconcile_within_window_requests_resend(client):
    """Terminal task + live attempt inside the resend window -> resend action,
    attempt untouched (the device may still honour a (re-)delivered stop)."""
    device = register_device(client, "对账机A")
    task_id, attempt_id = _plant_attempt(device["device_id"])
    actions = TaskService(SessionLocal()).reconcile_terminal_attempts(
        settings.task_cancel_resend_window
    )
    match = [a for a in actions if a["task_id"] == task_id]
    assert match and match[0]["attempt_id"] == attempt_id
    assert _attempt_status(attempt_id) == "RUNNING"


def test_reconcile_marks_stale_after_window(client):
    """Terminal task + live attempt past the resend window -> attempt STALE
    with an audit event; no further resend actions are produced."""
    device = register_device(client, "对账机B")
    window = settings.task_cancel_resend_window
    task_id, attempt_id = _plant_attempt(
        device["device_id"], finished_delta=window + 60
    )
    actions = TaskService(SessionLocal()).reconcile_terminal_attempts(window)
    assert [a for a in actions if a["task_id"] == task_id] == []
    assert _attempt_status(attempt_id) == "STALE"
    assert _events(task_id, "task.attempt_reconciled")


def test_reconcile_superseded_attempt_is_stale_immediately(client):
    """A superseded attempt (newer attempt owns the step) is STALE even when
    its task is still live."""
    device = register_device(client, "对账机C")
    task_id, attempt_id = _plant_attempt(
        device["device_id"], task_status="RUNNING", superseded=True
    )
    actions = TaskService(SessionLocal()).reconcile_terminal_attempts(
        settings.task_cancel_resend_window
    )
    assert [a for a in actions if a["task_id"] == task_id] == []
    assert _attempt_status(attempt_id) == "STALE"


def test_reconcile_leaves_live_task_alone(client):
    """Live task + live attempt = normal operation; reconcile must not touch it."""
    device = register_device(client, "对账机D")
    task_id, attempt_id = _plant_attempt(device["device_id"], task_status="RUNNING")
    TaskService(SessionLocal()).reconcile_terminal_attempts(settings.task_cancel_resend_window)
    assert _attempt_status(attempt_id) == "RUNNING"


def test_monitor_delivers_stop_once_for_terminal_task(client):
    """Monitor path: the (re-)sent stop is delivered exactly once per live
    attempt (dedupe by task_id+attempt_id), and the attempt stays live inside
    the resend window. The monitor gets a stub hub - the app's shared hub must
    never be patched (conftest reuses one app instance across tests)."""
    device = register_device(client, "对账机E")
    task_id, attempt_id = _plant_attempt(device["device_id"])
    monitor = TaskMonitor(client.app.state.hub)
    hub = _StubHub(1)
    monitor.hub = hub
    asyncio.run(monitor.reconcile_stale())
    assert hub.calls == [device["device_id"]]
    assert (task_id, attempt_id) in monitor._stop_sent
    asyncio.run(monitor.reconcile_stale())  # second sweep: no duplicate send
    assert hub.calls.count(device["device_id"]) == 1
    assert _attempt_status(attempt_id) == "RUNNING"


def test_monitor_skips_resend_when_undeliverable(client):
    """send_to_device == 0 (device offline): the stop is NOT marked as sent so
    later sweeps keep retrying until the window closes the attempt STALE."""
    device = register_device(client, "对账机F")
    task_id, attempt_id = _plant_attempt(device["device_id"])
    monitor = TaskMonitor(client.app.state.hub)
    monitor.hub = _StubHub(0)
    asyncio.run(monitor.reconcile_stale())
    assert monitor._stop_sent == set()  # retry on the next sweep
    assert _attempt_status(attempt_id) == "RUNNING"


def test_retry_gets_fresh_offline_window(client):
    """A retried task must not be insta-timed-out by its original created_at:
    the offline-max-wait window restarts when the task re-enters PENDING."""
    device = register_device(client, "retry窗口机")
    with SessionLocal() as db:
        db.add(DeviceCapability(device_id=device["device_id"], command_name="echo"))
        db.commit()
    res = client.post(
        "/api/tasks",
        json={
            "name": "retry-window",
            "target_device_id": device["device_id"],
            "steps": [{"command": "echo", "params": {"message": "p2"}}],
        },
    )
    assert res.status_code == 201, res.text
    task_id = res.json()["task_id"]

    # Age the task past the offline max wait -> the sweep times it out (old bug).
    monitor = TaskMonitor(client.app.state.hub)
    with SessionLocal() as db:
        row = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        row.pending_since = utcnow() - timedelta(seconds=settings.task_offline_max_wait + 60)
        db.commit()
    asyncio.run(monitor.dispatch_pending())
    with SessionLocal() as db:
        assert db.scalars(select(Task).where(Task.task_id == task_id)).first().status == "TIMEOUT"

    # Retry -> fresh window: the sweep may dispatch-fail (device offline) but
    # must NOT time the task out.
    res = client.post(f"/api/tasks/{task_id}/retry")
    assert res.status_code == 200, res.text
    asyncio.run(monitor.dispatch_pending())
    with SessionLocal() as db:
        assert db.scalars(select(Task).where(Task.task_id == task_id)).first().status == "PENDING"


def test_sent_offline_task_times_out_after_max_wait(client):
    """A SENT task whose device dropped before accepting gets the offline max
    wait as its acceptance deadline, then TIMEOUT (retryable) - never an
    ACCEPTED-forever state."""
    device2 = register_device(client, "sent掉线机")
    with SessionLocal() as db:
        db.add(DeviceCapability(device_id=device2["device_id"], command_name="echo"))
        db.commit()
    res = client.post(
        "/api/tasks",
        json={
            "name": "sent-offline",
            "target_device_id": device2["device_id"],
            "steps": [{"command": "echo", "params": {"message": "p2"}}],
        },
    )
    task_id = res.json()["task_id"]

    # Force the task into SENT as if the send raced a disconnect.
    with SessionLocal() as db:
        task = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        step = db.scalars(select(TaskStep).where(TaskStep.task_id == task_id)).first()
        task.status = "SENT"
        task.timeout_at = utcnow() + timedelta(hours=1)
        step.current_attempt_id = f"attempt_{uuid.uuid4().hex[:10]}"
        attempt = TaskAttempt(
            attempt_id=step.current_attempt_id,
            task_id=task_id,
            step_id=step.step_id,
            device_id=device2["device_id"],
            status="SENT",
            created_at=utcnow() - timedelta(seconds=settings.task_offline_max_wait + 60),
        )
        db.add(attempt)
        db.commit()

    monitor = TaskMonitor(client.app.state.hub)
    asyncio.run(monitor.timeout_scan())
    with SessionLocal() as db:
        assert db.scalars(select(Task).where(Task.task_id == task_id)).first().status == "TIMEOUT"


# ----------------------------------------------------------------- worker side


class _GateExecutor:
    """Legacy executor that blocks until an asyncio.Event is set."""

    def __init__(self, gate: asyncio.Event, label: str, ran: list[str]) -> None:
        self.gate = gate
        self.label = label
        self.ran = ran

    def validate(self, params) -> None:  # noqa: ARG002
        return None

    async def execute(self, params, config, progress, cancel):  # noqa: ARG002
        await self.gate.wait()
        self.ran.append(self.label)
        return {"label": self.label}


def _dispatch_envelope(task_id: str, attempt_id: str, command: str) -> dict:
    return protocol.build_envelope(
        "task.dispatch",
        {
            "task_id": task_id,
            "step_id": f"step_{task_id}",
            "attempt_id": attempt_id,
            "command": command,
            "params": {},
            "timeout": 30,
        },
    )


class _StubHub:
    """Record-only hub replacement for monitor tests. Never patch the app's
    shared hub: conftest reuses one app instance across all tests."""

    def __init__(self, result: int) -> None:
        self._result = result
        self.calls: list[str] = []

    async def send_to_device(self, device_id: str, envelope) -> int:  # noqa: ARG002
        self.calls.append(device_id)
        return self._result


@pytest.mark.anyio
async def test_cancel_while_queued_never_executes(tmp_path):
    """Phase 2 core fix: cancel arriving between intake and dequeue closes the
    queued attempt CANCELLED - it must never execute, never sit ACCEPTED."""
    gate = asyncio.Event()
    ran: list[str] = []
    blocker = _GateExecutor(gate, "task_1", ran)
    queued = _GateExecutor(gate, "task_2", ran)

    tm = TaskManager(ledger=ExecutionLedger(tmp_path / "worker.db"))
    sent: list[dict] = []

    class WS:
        async def send(self, envelope: dict) -> None:
            sent.append(envelope)

    tm.bind(WS())
    tm.registry["blocker"] = blocker
    tm.registry["queued"] = queued

    await tm.on_dispatch(_dispatch_envelope("task_1", "attempt_1", "blocker"))
    await tm.on_dispatch(_dispatch_envelope("task_2", "attempt_2", "queued"))
    # task_1 occupies the single consumer slot; task_2 sits queued.
    await tm.on_cancel({"task_id": "task_2"})
    gate.set()
    await tm._queue.join()

    results = {
        e["data"]["task_id"]: e["data"]["status"]
        for e in sent
        if e["type"] == "task.result"
    }
    assert results["task_1"] == "success"
    assert results["task_2"] == "cancelled"
    assert "task_2" not in ran  # never executed
    assert tm.ledger.get("attempt_2")["status"] == "CANCELLED"


@pytest.mark.anyio
async def test_duplicate_dispatch_while_queued_reports_accept(tmp_path):
    """ACCEPTED must mean received, never executing: a duplicate dispatch for
    a still-queued attempt echoes task.accept, not task.running - and the
    attempt still runs exactly once."""
    gate = asyncio.Event()
    ran: list[str] = []
    tm = TaskManager(ledger=ExecutionLedger(tmp_path / "worker.db"))
    sent: list[dict] = []

    class WS:
        async def send(self, envelope: dict) -> None:
            sent.append(envelope)

    tm.bind(WS())
    tm.registry["blocker"] = _GateExecutor(gate, "task_1", ran)
    tm.registry["blocker2"] = _GateExecutor(gate, "task_2", ran)

    await tm.on_dispatch(_dispatch_envelope("task_1", "attempt_1", "blocker"))
    await tm.on_dispatch(_dispatch_envelope("task_2", "attempt_2", "blocker2"))
    # Consumer is parked in task_1; task_2 is queued. Redeliver task_2.
    await tm.on_dispatch(_dispatch_envelope("task_2", "attempt_2", "blocker2"))
    echoes = [
        e["type"]
        for e in sent
        if e["type"] in ("task.accept", "task.running")
        and e["data"]["attempt_id"] == "attempt_2"
        and e["data"]["task_id"] == "task_2"
    ]
    assert echoes == ["task.accept", "task.accept"]  # intake + duplicate: both queued-phase
    gate.set()
    await tm._queue.join()
    assert ran.count("task_2") == 1  # executed exactly once
