"""AgentHub V1.0 acceptance: isolation, offline redispatch, idempotency, attempts."""

import asyncio
from pathlib import Path

try:
    from agenthub._worker import FakeWorker, register_device, wait_for_capabilities, wait_until
except ImportError:  # pragma: no cover - depends on pytest import mode
    from _worker import FakeWorker, register_device, wait_for_capabilities, wait_until

from client.worker.manager import TaskManager


def test_multi_device_isolation(client):
    """A task targeted at device A must never reach device B."""
    a = register_device(client, "设备A")
    b = register_device(client, "设备B")
    worker_a = FakeWorker(client, a["device_token"], max_dispatches=1)
    worker_b = FakeWorker(client, b["device_token"], max_dispatches=1)
    worker_a.start()
    worker_b.start()
    try:
        assert wait_for_capabilities(client, a["device_id"])
        assert wait_for_capabilities(client, b["device_id"])
        res = client.post(
            "/api/tasks",
            json={
                "name": "isolation",
                "target_device_id": a["device_id"],
                "steps": [{"command": "echo", "params": {"message": "only-a"}}],
            },
        )
        assert res.status_code == 201, res.text
        task_id = res.json()["task_id"]

        assert wait_until(lambda: client.get(f"/api/tasks/{task_id}").json()["status"] == "SUCCESS")
        assert len(worker_a.dispatches) == 1
        assert worker_b.dispatches == []
    finally:
        worker_a.stop()
        worker_b.stop()


def test_offline_device_pends_then_redispatches_on_connect(client):
    """Device reports capability then disconnects: task stays PENDING until it
    reconnects, and TaskMonitor dispatches it automatically."""
    device = register_device(client, "离线设备")
    token = device["device_token"]

    # Short-lived session: register capabilities, then go offline.
    caps_worker = FakeWorker(client, token, behaviour="caps_only")
    caps_worker.start()
    caps_worker.join(timeout=10)
    assert caps_worker.errors == []
    assert wait_for_capabilities(client, device["device_id"])

    live_worker = None
    try:
        res = client.post(
            "/api/tasks",
            json={
                "name": "offline-wait",
                "target_device_id": device["device_id"],
                "steps": [{"command": "echo", "params": {"message": "reconnected"}}],
            },
        )
        assert res.status_code == 201, res.text
        task_id = res.json()["task_id"]

        # Device is offline: the task must stay PENDING (dispatch skipped).
        assert client.get(f"/api/tasks/{task_id}").json()["status"] == "PENDING"

        # Reconnect: the monitor sweep dispatches the pending task.
        live_worker = FakeWorker(client, token, max_dispatches=1)
        live_worker.start()
        assert wait_until(lambda: client.get(f"/api/tasks/{task_id}").json()["status"] == "SUCCESS", timeout=15)
        assert live_worker.dispatches and live_worker.dispatches[0]["data"]["params"] == {"message": "reconnected"}
    finally:
        if live_worker is not None:
            live_worker.stop()


def test_worker_dedups_duplicate_dispatch(tmp_path):
    """Client-side idempotency: a repeated task.dispatch is reported, not re-run."""

    class RecordingClient:
        def __init__(self) -> None:
            self.sent: list[dict] = []

        async def send(self, envelope: dict) -> None:
            self.sent.append(envelope)

    async def scenario(tmp: str) -> tuple[list[dict], int]:
        from worker.ledger import ExecutionLedger

        manager = TaskManager(ledger=ExecutionLedger(Path(tmp) / "worker.db"))
        ws = RecordingClient()
        manager.bind(ws)
        envelope = {
            "id": "m1", "type": "task.dispatch", "version": 1, "timestamp": 1,
            "data": {"task_id": "task_1", "step_id": "step_1", "command": "echo",
                     "params": {"message": "hi"}, "timeout": 30},
        }
        await manager.on_dispatch(dict(envelope))
        queued = manager._queue.qsize()  # exactly one execution queued
        await manager._queue.join()  # execution #1 fully done
        accepted = len([e for e in ws.sent if e["type"] == "task.accept"])
        await manager.on_dispatch(dict(envelope))  # duplicate: re-report, no re-run
        await manager._queue.join()
        return ws.sent, queued, accepted

    sent, queued, accepted = asyncio.run(scenario(str(tmp_path)))
    types = [e["type"] for e in sent]
    # second dispatch must NOT queue a second execution; it only re-reports
    assert queued == 1
    assert accepted == 1
    assert types.count("task.accept") == 1
    assert types[-1] == "task.result"  # duplicate path reports the stored result


def test_retry_attempts_bookkeeping(client):
    """Each retry adds one attempt (attempt_no grows); exceeding max_attempts -> 409."""
    device = register_device(client, "故障设备")
    worker = FakeWorker(client, device["device_token"], behaviour="fail", max_dispatches=3)
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"])
        res = client.post(
            "/api/tasks",
            json={
                "name": "bookkeeping",
                "target_device_id": device["device_id"],
                "steps": [{"command": "echo", "params": {"message": "x"}}],
            },
        )
        assert res.status_code == 201, res.text
        task_id = res.json()["task_id"]

        # First attempt fails at the worker.
        assert wait_until(lambda: client.get(f"/api/tasks/{task_id}").json()["status"] == "FAILED", timeout=15)

        # Retry twice: attempts 2 and 3.
        for expected_attempts in (2, 3):
            assert client.post(f"/api/tasks/{task_id}/retry").status_code == 200
            assert wait_until(
                lambda: client.get(f"/api/tasks/{task_id}").json()["status"] == "FAILED", timeout=15
            )
            detail = client.get(f"/api/tasks/{task_id}").json()
            assert len(detail["attempts"]) == expected_attempts
            assert [a["attempt_no"] for a in detail["attempts"]] == list(range(1, expected_attempts + 1))

        # Attempts exhausted: the 4th retry is refused with 409.
        res = client.post(f"/api/tasks/{task_id}/retry")
        assert res.status_code == 409
        assert res.json()["detail"]["code"] == "invalid_task_state"
    finally:
        worker.stop()
