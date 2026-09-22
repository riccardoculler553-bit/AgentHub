"""V1.7 Worker Environment Inventory tests (§22-§28).

FakeWorker reports a worker.environment snapshot on connect; the server
stores the latest per device, exposes GET /api/devices/{id}/environment and
flags fingerprint drift on a changed report.
"""

import uuid

from sqlalchemy import select

from app.db.database import SessionLocal
from app.worker.db_models import WorkerEnvironment, WorkerProcess
from app.worker.service import WorkerService

from ._worker import FakeWorker, register_device, wait_until

_ENV = {
    "hostname": "OFFICE-PC-02",
    "os": {"name": "Windows", "version": "11"},
    "arch": "AMD64",
    "cpu": {"cores": 16},
    "memory": {"total_gb": 32.0},
    "disk": {"free_gb": 380.0},
    "python": ["3.11", "3.13"],
    "yingdao": {"installed": True, "version": "6.4"},
    "worker": {"version": "1.7.0"},
    "fingerprint": "fp-a",
}


def test_environment_report_stored_and_served(client):
    device = register_device(client, "环境机A")
    worker = FakeWorker(client, device["device_token"], behaviour="caps_only", environment=dict(_ENV))
    worker.start()
    try:
        assert wait_until(lambda: worker.received and worker.received[-1]["type"] == "message_ack")
        res = client.get(f"/api/devices/{device['device_id']}/environment")
        assert res.status_code == 200, res.text
        body = res.json()
        assert body["hostname"] == "OFFICE-PC-02"
        assert body["fingerprint"] == "fp-a"
        assert body["worker_version"] == "1.7.0"
        assert body["environment"]["cpu"]["cores"] == 16
        assert body["environment"]["yingdao"]["installed"] is True
    finally:
        worker.stop()


def test_environment_drift_flagged_on_fingerprint_change(client):
    device = register_device(client, "环境机B")
    worker = FakeWorker(client, device["device_token"], behaviour="caps_only", environment=dict(_ENV))
    worker.start()
    try:
        assert wait_until(lambda: worker.received and worker.received[-1]["type"] == "message_ack")
        drifted_ack = worker.received[-1]
        # no prior report -> no drift
        assert drifted_ack["data"]["drift"] is False
    finally:
        worker.stop()

    # second connect with a changed fingerprint -> drift flagged
    env2 = dict(_ENV)
    env2["fingerprint"] = "fp-b"
    env2["python"] = ["3.11"]  # 3.13 disappeared overnight
    worker2 = FakeWorker(client, device["device_token"], behaviour="caps_only", environment=env2)
    worker2.start()
    try:
        assert wait_until(lambda: worker2.received and worker2.received[-1]["type"] == "message_ack")
        ack = worker2.received[-1]
        assert ack["data"]["drift"] is True
        res = client.get(f"/api/devices/{device['device_id']}/environment")
        assert res.json()["fingerprint"] == "fp-b"
    finally:
        worker2.stop()


def test_environment_never_reported_404(client):
    device = register_device(client, "环境机C")
    res = client.get(f"/api/devices/{device['device_id']}/environment")
    assert res.status_code == 404
    assert res.json()["detail"]["code"] == "environment_not_reported"


# ------------------------------------------------- process instance registry


def test_process_registry_lifecycle_states():
    device_id = f"dev_{uuid.uuid4().hex[:8]}"
    svc = WorkerService(SessionLocal())
    row = svc.create_process(device_id, "order.sync", "1.0.0", requested_by="tester")
    assert row.status == "STARTING"
    # idempotent create while live returns the same instance
    again = svc.create_process(device_id, "order.sync", "1.0.0")
    assert again.process_id == row.process_id
    # device reports running with an OS pid
    svc.update_process_status(row.process_id, "RUNNING", pid=4242)
    fetched = svc.get_process(row.process_id)
    assert fetched.status == "RUNNING" and fetched.pid == 4242 and fetched.started_at is not None
    # stop -> STOPPED with a stopped_at
    svc.update_process_status(row.process_id, "STOPPED")
    assert svc.get_process(row.process_id).status == "STOPPED"
    # restart path: create again reuses the row in STARTING
    reused = svc.create_process(device_id, "order.sync", "1.0.0")
    assert reused.process_id == row.process_id and reused.status == "STARTING"


def test_process_registry_lists_by_device_and_status():
    device_id = f"dev_{uuid.uuid4().hex[:8]}"
    svc = WorkerService(SessionLocal())
    a = svc.create_process(device_id, "a.b.c", "1.0.0")
    b = svc.create_process(device_id, "d.e.f", "2.0.0")
    svc.update_process_status(a.process_id, "RUNNING", pid=11)
    running = svc.list_processes(device_id=device_id, status="RUNNING")
    assert [p.process_id for p in running] == [a.process_id]
    all_rows = svc.list_processes(device_id=device_id)
    assert {p.process_id for p in all_rows} == {a.process_id, b.process_id}
    assert all(isinstance(p, WorkerProcess) for p in all_rows)
