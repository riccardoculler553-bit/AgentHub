"""V1.1 reliability matrix (V1.1_PLAN.md §70): races, late events, recovery,
idempotency, security hardening. All timing-sensitive paths use the state
machine / CAS primitives directly so tests stay deterministic."""

import asyncio
import time
import uuid

from sqlalchemy import select

try:
    from agenthub._worker import FakeWorker, register_device, wait_for_capabilities, wait_until
except ImportError:  # pragma: no cover - depends on pytest import mode
    from _worker import FakeWorker, register_device, wait_for_capabilities, wait_until

from app.capability.db_models import DeviceCapability
from app.core.config import settings
from app.db.database import SessionLocal
from app.db.models import utcnow
from app.task.db_models import Task, TaskAttempt, TaskEvent, TaskStep
from app.task.dispatcher import TaskDispatcher
from app.task.monitor import TaskMonitor
from app.task.service import TaskService


# ----------------------------------------------------------------- helpers


def _create_task(client, device_name: str, device_id: str) -> dict:
    res = client.post(
        "/api/tasks",
        json={
            "name": f"reliability-{device_name}",
            "target_device_id": device_id,
            "steps": [{"command": "echo", "params": {"message": "v1.1"}}],
        },
    )
    assert res.status_code == 201, res.text
    return res.json()


def _load_task(task_id: str) -> Task:
    with SessionLocal() as db:
        task = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        assert task is not None
        db.expunge(task)
        return task


def _attempts_of(task_id: str) -> list[TaskAttempt]:
    with SessionLocal() as db:
        rows = db.scalars(
            select(TaskAttempt).where(TaskAttempt.task_id == task_id).order_by(TaskAttempt.id)
        ).all()
        db.expunge_all()
        return list(rows)


def _events_of(task_id: str) -> list[dict]:
    with SessionLocal() as db:
        rows = db.scalars(
            select(TaskEvent).where(TaskEvent.task_id == task_id).order_by(TaskEvent.id)
        ).all()
        return [{"type": r.event_type, "attempt_id": r.attempt_id, "payload": r.payload} for r in rows]


def _svc() -> TaskService:
    return TaskService(SessionLocal())


# ------------------------------------------------------- §10 dispatcher CAS


def test_cas_claim_admits_exactly_one_dispatcher(client):
    """Two dispatchers race for one PENDING task -> exactly one claim wins."""
    device = register_device(client, "cas机")
    # No worker will connect; plant the capability so task creation passes and
    # patch liveness so dispatch reaches the (patched) send stage.
    with SessionLocal() as db:
        db.add(DeviceCapability(device_id=device["device_id"], command_name="echo"))
        db.commit()
    task = _create_task(client, "cas机", device["device_id"])

    dispatcher = TaskDispatcher(client.app.state.hub)
    dispatcher.device_link.is_online = lambda device_id_: True

    async def unreachable(device_id_, envelope):
        await asyncio.sleep(0.05)  # force interleave between CAS and send
        return 0

    dispatcher.device_link.send_task = unreachable

    async def race():
        return await asyncio.gather(
            dispatcher.dispatch_task(task["task_id"]),
            dispatcher.dispatch_task(task["task_id"]),
        )

    asyncio.run(race())
    # The first dispatcher runs synchronously up to the send await; the second
    # then hits the CAS with status DISPATCHING -> 0 rows -> returns False.
    dispatching_events = [e for e in _events_of(task["task_id"]) if e["type"] == "task.dispatching"]
    assert len(dispatching_events) == 1
    assert _load_task(task["task_id"]).status == "PENDING"  # rolled back, still dispatchable
    assert _attempts_of(task["task_id"]) == []  # no attempt survived the rollback


def test_dispatch_to_online_device_creates_single_attempt(client):
    """Full path through the running monitor: exactly one attempt, one dispatch."""
    device = register_device(client, "单尝试机")
    worker = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="silent", max_dispatches=1)
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], names=("echo",))
        task = _create_task(client, "单尝试机", device["device_id"])
        assert wait_until(lambda: _load_task(task["task_id"]).status in ("SENT", "ACCEPTED", "RUNNING"), timeout=10)
        assert wait_until(lambda: len(worker.dispatches) >= 1, timeout=5)
        import time
        time.sleep(4)  # allow a full monitor sweep cycle to try re-dispatching
        attempts = _attempts_of(task["task_id"])
        assert len(attempts) == 1
        assert attempts[0].attempt_no == 1
        assert len(worker.dispatches) == 1
    finally:
        worker.stop()


# --------------------------------------------- §55.3/§55.6 late result races


def _dispatch_via_monitor_and_run(client, device_id, task_id, to_status="RUNNING"):
    """Dispatch through the monitor (silent worker keeps the task SENT), then
    force the attempt into RUNNING as if the worker had reported it."""
    assert wait_until(lambda: _load_task(task_id).status == "SENT", timeout=10), _load_task(task_id).status
    with SessionLocal() as db:
        task = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        attempt = db.scalars(select(TaskAttempt).where(TaskAttempt.task_id == task_id)).first()
        attempt.status = to_status
        attempt.started_at = attempt.started_at or utcnow()
        db.commit()
        db.expunge(task)
        db.expunge(attempt)
    return task, attempt


def test_late_success_after_timeout_stays_timeout(client):
    """§55.3: attempt 1 TIMEOUT -> its late SUCCESS may not reopen the task."""
    device = register_device(client, "晚到机A")
    worker = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="silent", max_dispatches=1)
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], names=("echo",))
        task = _create_task(client, "晚到机A", device["device_id"])
        task_row, attempt = _dispatch_via_monitor_and_run(client, device["device_id"], task["task_id"])

        svc = _svc()
        assert svc.timeout_running(task_row)["status"] == "TIMEOUT"
        assert _load_task(task["task_id"]).status == "TIMEOUT"

        result = svc.handle_device_event(
            device["device_id"], "task.result",
            {"task_id": task["task_id"], "step_id": attempt.step_id,
             "attempt_id": attempt.attempt_id, "status": "success", "result": {"late": True}},
        )
        assert result["advance"] is False
        assert _load_task(task["task_id"]).status == "TIMEOUT"  # NOT reopened
        events = _events_of(task["task_id"])
        late = [e for e in events if e["type"] == "task.late_result"]
        assert late and late[0]["payload"]["reason"] == "task_terminal"
        assert late[0]["attempt_id"] == attempt.attempt_id
    finally:
        worker.stop()


def test_late_success_after_cancel_keeps_cancelled(client):
    """§55.5: cancel wins the race -> the task stays CANCELLED forever."""
    device = register_device(client, "晚到机B")
    worker = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="silent", max_dispatches=1)
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], names=("echo",))
        task = _create_task(client, "晚到机B", device["device_id"])
        task_row, attempt = _dispatch_via_monitor_and_run(client, device["device_id"], task["task_id"])

        svc = _svc()
        svc.request_cancel(task["task_id"])
        assert _load_task(task["task_id"]).status == "CANCELLED"

        svc.handle_device_event(
            device["device_id"], "task.result",
            {"task_id": task["task_id"], "step_id": attempt.step_id,
             "attempt_id": attempt.attempt_id, "status": "success"},
        )
        assert _load_task(task["task_id"]).status == "CANCELLED"
        assert any(e["type"] == "task.late_result" for e in _events_of(task["task_id"]))
    finally:
        worker.stop()


def test_result_beats_watchdog_single_winner(client):
    """§55.4: result first -> task SUCCESS; the watchdog CAS must then no-op."""
    device = register_device(client, "竞速机")
    worker = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="silent", max_dispatches=1)
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], names=("echo",))
        task = _create_task(client, "竞速机", device["device_id"])
        task_row, attempt = _dispatch_via_monitor_and_run(client, device["device_id"], task["task_id"])

        svc = _svc()
        result = svc.handle_device_event(
            device["device_id"], "task.result",
            {"task_id": task["task_id"], "step_id": attempt.step_id,
             "attempt_id": attempt.attempt_id, "status": "success"},
        )
        assert result["advance"] is False
        assert _load_task(task["task_id"]).status == "SUCCESS"

        task_row2 = _load_task(task["task_id"])
        outcome = svc.timeout_running(task_row2)  # stale watchdog sweep
        assert outcome["notify_device"] is False
        assert outcome["status"] == "SUCCESS"  # no CAS win -> no state change
        assert _load_task(task["task_id"]).status == "SUCCESS"
    finally:
        worker.stop()


# ------------------------------------------------------ §7 stale attempt gate


def test_stale_attempt_event_is_audit_only(client):
    """§8: events naming a non-current attempt never mutate state."""
    device = register_device(client, "过期机")
    worker = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="silent", max_dispatches=1)
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], names=("echo",))
        task = _create_task(client, "过期机", device["device_id"])
        assert wait_until(lambda: _load_task(task["task_id"]).status == "SENT", timeout=10)
        attempt = _attempts_of(task["task_id"])[0]

        with SessionLocal() as db:
            from sqlalchemy import select
            from app.task.db_models import TaskStep
            step = db.scalars(select(TaskStep).where(TaskStep.step_id == attempt.step_id)).first()
            assert step.current_attempt_id == attempt.attempt_id
            step.current_attempt_id = "attempt_future"  # server already moved on
            db.commit()

        svc = _svc()
        svc.handle_device_event(
            device["device_id"], "task.running",
            {"task_id": task["task_id"], "step_id": attempt.step_id,
             "attempt_id": attempt.attempt_id},
        )
        row = _load_task(task["task_id"])
        assert row.status == "SENT"  # untouched
        events = _events_of(task["task_id"])
        stale = [e for e in events if e["type"] == "task.late_event"]
        assert stale and stale[0]["payload"]["reason"] == "stale_attempt"
        assert stale[0]["attempt_id"] == attempt.attempt_id
    finally:
        worker.stop()


# ------------------------------------------------------ §21 AgentRun 幂等


def test_agentrun_same_message_id_same_run(client):
    first = client.post("/api/agent/message", json={"text": "运行办公室电脑02的Text1", "channel": "api", "message_id": "dup-1"})
    assert first.status_code == 200, first.text
    second = client.post("/api/agent/message", json={"text": "运行办公室电脑02的Text1", "channel": "api", "message_id": "dup-1"})
    assert second.status_code == 200, second.text
    assert first.json()["run_id"] == second.json()["run_id"]


def test_agentrun_empty_message_id_never_collides(client):
    """API-created runs carry no message_id (stored NULL) - each is its own run."""
    runs = [
        client.post("/api/agent/message", json={"text": "你好", "channel": "api"}).json()["run_id"]
        for _ in range(3)
    ]
    assert len(set(runs)) == 3


# ------------------------------------------------------ §22 Device API 鉴权


def test_device_api_requires_admin(client, monkeypatch):
    monkeypatch.setattr(settings, "admin_token", "sekret")
    device = register_device(client, "鉴权机")

    assert client.get("/api/devices").status_code == 401
    assert client.get("/api/devices", headers={"X-Admin-Token": "wrong"}).status_code == 401
    assert client.get("/api/devices", headers={"X-Admin-Token": "sekret"}).status_code == 200
    assert (
        client.post(f"/api/devices/{device['device_id']}/revoke").status_code == 401
    )
    assert (
        client.post(
            f"/api/devices/{device['device_id']}/revoke", headers={"X-Admin-Token": "sekret"}
        ).status_code == 200
    )


def test_admin_fail_closed_without_token_on_public_bind(client, monkeypatch):
    """V1.6 P0 0.7 (audit H1): empty AGENTHUB_ADMIN_TOKEN must NOT open the
    management plane when the server binds a non-loopback address."""
    monkeypatch.setattr(settings, "admin_token", "")
    monkeypatch.setattr(settings, "host", "0.0.0.0")
    assert client.get("/api/devices").status_code == 401
    assert client.get("/api/artifacts").status_code == 401
    # loopback bind keeps the local-dev open mode
    monkeypatch.setattr(settings, "host", "127.0.0.1")
    assert client.get("/api/devices").status_code == 200


def test_rbac_role_tokens_are_hierarchical(client, monkeypatch):
    """V1.6 P0 0.18: admin > operator > viewer. Viewer reads, operator
    dispatches, admin revokes; lower roles cannot reach higher endpoints."""
    monkeypatch.setattr(settings, "admin_token", "adm-tok")
    monkeypatch.setattr(settings, "operator_token", "op-tok")
    monkeypatch.setattr(settings, "viewer_token", "view-tok")
    device = register_device(client, "RBAC机")

    # no token -> 401 (fail-closed with roles configured)
    assert client.get("/api/tasks").status_code == 401
    # viewer: reads pass...
    assert client.get("/api/tasks", headers={"X-Admin-Token": "view-tok"}).status_code == 200
    assert client.get("/api/devices", headers={"X-Admin-Token": "view-tok"}).status_code == 200
    # ...but mutations are forbidden (403, not 401: authenticated, unauthorized)
    res = client.post("/api/tasks", json={}, headers={"X-Admin-Token": "view-tok"})
    assert res.status_code == 403, res.text
    # operator: task mutation passes auth (payload may still be invalid -> 422)
    res = client.post("/api/tasks", json={}, headers={"X-Admin-Token": "op-tok"})
    assert res.status_code == 422
    # operator cannot revoke (admin-only)
    res = client.post(
        f"/api/devices/{device['device_id']}/revoke", headers={"X-Admin-Token": "op-tok"}
    )
    assert res.status_code == 403
    # admin passes everywhere
    res = client.post(
        f"/api/devices/{device['device_id']}/revoke", headers={"X-Admin-Token": "adm-tok"}
    )
    assert res.status_code == 200


# ---------------------------------------------------- §38 多连接聚合在线


def test_multi_connection_device_stays_online(client):
    device = register_device(client, "多连机")
    # "silent" keeps each session open until stop(); caps_only would hang up
    # right after reporting capabilities and the connections would be gone.
    w1 = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="silent")
    w2 = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="silent")
    w1.start()
    w2.start()
    try:
        assert wait_until(
            lambda: any(
                d["device_id"] == device["device_id"] and d["online"]
                for d in client.get("/api/devices").json()
            ),
            timeout=10,
        )
        w1.stop()
        # one connection dropped -> the device is STILL online
        assert wait_until(
            lambda: any(
                d["device_id"] == device["device_id"]
                and d["online"]
                and d["connection_count"] == 1
                for d in client.get("/api/devices").json()
            ),
            timeout=10,
        )
    finally:
        w2.stop()
        assert wait_until(
            lambda: any(
                d["device_id"] == device["device_id"] and not d["online"]
                for d in client.get("/api/devices").json()
            ),
            timeout=10,
        )


# ---------------------------------------------------- §42 server 重启恢复


def test_server_restart_recovers_stuck_dispatching(client):
    device = register_device(client, "重启机")
    # No worker will connect; plant the capability so task creation passes.
    # The monitor leaves PENDING tasks alone while the device is offline.
    with SessionLocal() as db:
        db.add(DeviceCapability(device_id=device["device_id"], command_name="echo"))
        db.commit()
    task = _create_task(client, "重启机", device["device_id"])

    # Simulate a crash between CAS claim and send: task DISPATCHING with a
    # DISPATCHING attempt, as a killed server process would leave them.
    with SessionLocal() as db:
        db_task = db.scalars(select(Task).where(Task.task_id == task["task_id"])).first()
        db_task.status = "DISPATCHING"
        step = db.scalars(select(TaskStep).where(TaskStep.task_id == task["task_id"])).first()
        attempt = TaskAttempt(
            attempt_id=f"attempt_{uuid.uuid4().hex[:12]}",
            task_id=task["task_id"],
            step_id=step.step_id,
            device_id=device["device_id"],
            attempt_no=1,
            status="DISPATCHING",
        )
        db.add(attempt)
        step.current_attempt_id = attempt.attempt_id
        db.commit()

    recovered = TaskMonitor(client.app.state.hub).recover_stuck_dispatching()
    assert recovered == 1
    row = _load_task(task["task_id"])
    assert row.status == "PENDING"  # dispatchable again, monitor takes over
    attempts = _attempts_of(task["task_id"])
    assert attempts[0].status == "FAILED"
    assert attempts[0].error_code == "SERVER_RESTART"
    events = _events_of(task["task_id"])
    assert any(e["type"] == "task.recovered" for e in events)
