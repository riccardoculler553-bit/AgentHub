"""Phase 8 regression: progress events are monotonic per attempt.

- worker progress envelopes carry a per-attempt monotonic seq
- server drops any progress whose seq would move the snapshot backwards
- legacy workers (no seq) keep payload-only updates
- the task detail API surfaces the latest snapshot on the attempt
"""

import uuid

import pytest
from sqlalchemy import select

import protocol
from app.db.database import SessionLocal
from app.task.db_models import Task, TaskAttempt, TaskStep
from app.task.service import TaskService
from app.db.models import utcnow

from worker.ledger import ExecutionLedger
from worker.manager import TaskManager

from .test_worker_capability import FakePuller, build_package
from .test_worker_capability_runtime import FakeWS, FakeUploader
from ._worker import register_device


def _plant_running_task(device_id: str, *, progress_seq: int | None, progress_json: dict | None):
    suffix = uuid.uuid4().hex[:10]
    task_id, step_id, attempt_id = f"task_{suffix}", f"step_{suffix}", f"attempt_{suffix}"
    with SessionLocal() as db:
        task = Task(
            task_id=task_id, name="phase8", target_device_id=device_id,
            status="RUNNING", execution_type="LEGACY_COMMAND", created_at=utcnow(),
            started_at=utcnow(),
        )
        step = TaskStep(
            step_id=step_id, task_id=task_id, command="echo", status="RUNNING",
            current_attempt_id=attempt_id, started_at=utcnow(),
        )
        attempt = TaskAttempt(
            attempt_id=attempt_id, task_id=task_id, step_id=step_id, device_id=device_id,
            status="RUNNING", created_at=utcnow(), started_at=utcnow(),
            progress_seq=progress_seq, progress_json=progress_json,
        )
        db.add_all([task, step, attempt])
        db.commit()
    return task_id, step_id, attempt_id


def _attempt(attempt_id: str) -> TaskAttempt:
    with SessionLocal() as db:
        row = db.scalars(select(TaskAttempt).where(TaskAttempt.attempt_id == attempt_id)).first()
        db.expunge(row)
        return row


def test_progress_monotonic_gate(client):
    device = register_device(client, "进度机A")
    task_id, step_id, attempt_id = _plant_running_task(
        device["device_id"], progress_seq=3, progress_json={"progress": 50, "message": "B", "seq": 3}
    )
    data = {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id}

    # stale event (seq 2 < 3): snapshot untouched
    TaskService(SessionLocal()).handle_device_event(
        device["device_id"], "task.progress", {**data, "progress": 10, "message": "stale", "seq": 2}
    )
    assert _attempt(attempt_id).progress_json["progress"] == 50

    # newer event (seq 4): snapshot advances
    TaskService(SessionLocal()).handle_device_event(
        device["device_id"], "task.progress", {**data, "progress": 60, "message": "newer", "seq": 4}
    )
    row = _attempt(attempt_id)
    assert row.progress_seq == 4
    assert row.progress_json["progress"] == 60

    # legacy event (no seq): payload updates, stored seq untouched
    TaskService(SessionLocal()).handle_device_event(
        device["device_id"], "task.progress", {**data, "progress": 70, "message": "legacy"}
    )
    row = _attempt(attempt_id)
    assert row.progress_seq == 4
    assert row.progress_json["progress"] == 70

    detail = client.get(f"/api/tasks/{task_id}").json()
    attempt_out = next(a for a in detail["attempts"] if a["attempt_id"] == attempt_id)
    assert attempt_out["progress"]["progress"] == 70


def test_progress_without_prior_snapshot_wins(client):
    device = register_device(client, "进度机B")
    task_id, step_id, attempt_id = _plant_running_task(
        device["device_id"], progress_seq=None, progress_json=None
    )
    TaskService(SessionLocal()).handle_device_event(
        device["device_id"], "task.progress",
        {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id, "progress": 15, "seq": 1},
    )
    row = _attempt(attempt_id)
    assert row.progress_seq == 1 and row.progress_json["progress"] == 15


@pytest.mark.anyio
async def test_worker_progress_seq_strictly_increases(tmp_path):
    """The worker stamps every progress envelope with a per-attempt monotonic
    seq (Phase 8 contract)."""
    package = build_package(
        "p8.cap", "1.0.0",
        files={"main.py": "print('{\"ok\": true}')\n"},
    )
    from worker.capability.cache import CapabilityCache, sha256_bytes
    from worker.capability.manager import CapabilityManager

    cap_manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), FakePuller(package))
    tm = TaskManager(
        ledger=ExecutionLedger(tmp_path / "worker.db"),
        capability_manager=cap_manager,
        artifact_uploader=FakeUploader(),
    )
    ws = FakeWS()
    tm.bind(ws)
    envelope = protocol.build_envelope("capability.execute", {
        "task_id": "task_1", "step_id": "step_1", "attempt_id": "attempt_1",
        "capability": "p8.cap", "version": "1.0.0", "params": {},
        "timeout": 60, "package_id": "pkg_1", "checksum": sha256_bytes(package),
    })
    await tm.on_capability_execute(envelope)
    await tm._queue.join()

    seqs = [
        e["data"]["seq"]
        for e in ws.sent
        if e["type"] == "capability.progress" and "seq" in e["data"]
    ]
    assert seqs, "expected progress envelopes with seq"
    assert seqs == sorted(seqs) and len(set(seqs)) == len(seqs)  # strictly increasing
