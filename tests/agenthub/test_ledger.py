"""ExecutionLedger unit tests (PDF §76-§77/§110): idempotent claims, result
persistence, reconnect re-report, startup recovery."""

import asyncio

import pytest

from client.worker.ledger import ExecutionLedger
from client.worker.manager import TaskManager


@pytest.fixture()
def ledger(tmp_path):
    led = ExecutionLedger(tmp_path / "worker.db")
    yield led
    led.close()


def test_claim_is_idempotent_per_attempt(ledger):
    assert ledger.claim("t1", "s1", "a1", "echo") is None  # fresh
    assert ledger.claim("t1", "s1", "a1", "echo") == "ACCEPTED"  # duplicate
    ledger.mark_running("a1")
    assert ledger.claim("t1", "s1", "a1", "echo") == "RUNNING"
    assert ledger.claim("t1", "s2", "a2", "echo") is None  # other attempt is independent


def test_finished_results_requote_until_reported(ledger):
    ledger.claim("t1", "s1", "a1", "echo")
    ledger.mark_running("a1")
    ledger.mark_finished("a1", "SUCCESS", result={"message": "done"})

    pending = ledger.unreported_results()
    assert len(pending) == 1 and pending[0]["attempt_id"] == "a1"

    ledger.mark_reported("a1")
    assert ledger.unreported_results() == []


def test_fail_running_parks_orphans_at_startup(ledger):
    ledger.claim("t1", "s1", "a1", "echo")
    ledger.mark_running("a1")
    parked = ledger.fail_running()
    assert parked == 1
    row = ledger.get("a1")
    assert row["status"] == "FAILED" and row["error_code"] == "WORKER_RESTARTED"
    # terminal -> now reported via the normal re-report channel
    assert len(ledger.unreported_results()) == 1


class RecordingClient:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, envelope: dict) -> None:
        self.sent.append(envelope)


def test_manager_reports_finished_attempt_after_reconnect(tmp_path):
    """§110: result lost mid-disconnect -> re-sent on the next bind()."""

    led = ExecutionLedger(tmp_path / "worker.db")
    try:
        manager = TaskManager(ledger=led)
        # A finished-but-unreported execution left over from "last session".
        led.claim("task_x", "step_x", "attempt_x", "yingdao.audit")
        led.mark_running("attempt_x")
        led.mark_finished("attempt_x", "SUCCESS", result={"message": "审单执行完成"})

        ws = RecordingClient()

        async def reconnect():
            manager.bind(ws)
            for _ in range(100):
                if ws.sent:
                    return
                await asyncio.sleep(0.05)

        asyncio.run(reconnect())

        results = [e for e in ws.sent if e["type"] == "task.result"]
        assert len(results) == 1
        data = results[0]["data"]
        assert data["attempt_id"] == "attempt_x"
        assert data["status"] == "success"
        assert data["result"] == {"message": "审单执行完成"}
        assert led.unreported_results() == []  # marked reported after send
    finally:
        led.close()


def test_manager_never_reruns_finished_attempt(tmp_path):
    """§119: duplicate dispatch of the same attempt_id re-reports the stored
    result instead of executing again."""

    led = ExecutionLedger(tmp_path / "worker.db")
    try:
        manager = TaskManager(ledger=led)
        ws = RecordingClient()
        envelope = {
            "id": "m1", "type": "task.dispatch", "version": 1, "timestamp": 1,
            "data": {"task_id": "task_9", "step_id": "step_9", "attempt_id": "attempt_9",
                     "command": "echo", "params": {"message": "hi"}, "timeout": 30},
        }

        async def scenario():
            manager.bind(ws)
            await manager.on_dispatch(dict(envelope))
            await manager._queue.join()  # execution #1 completes
            accepted = len([e for e in ws.sent if e["type"] == "task.accept"])
            results_before = len([e for e in ws.sent if e["type"] == "task.result"])
            await manager.on_dispatch(dict(envelope))  # duplicate delivery
            await manager._queue.join()
            return accepted, results_before

        accepted, results_before = asyncio.run(scenario())
        assert accepted == 1  # executed exactly once
        assert manager._queue.qsize() == 0  # duplicate did not queue
        results = [e for e in ws.sent if e["type"] == "task.result"]
        assert len(results) == results_before + 1  # re-reported, not re-run
        assert results[-1]["data"]["status"] == "success"
    finally:
        led.close()
