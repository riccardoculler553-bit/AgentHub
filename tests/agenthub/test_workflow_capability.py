"""V1.4 Workflow x Capability Runtime integration tests (Phase 7, §24-§28).

Real chain: capability publish API -> WorkflowService -> WorkflowEngine
(CAPABILITY task) -> Dispatcher -> FakeWorker capability lifecycle ->
Artifact upload -> run context artifacts index.
"""

import pytest

from app.db.database import SessionLocal
from app.task.db_models import Task

from ._worker import FakeWorker, register_device, wait_for_worker_capabilities, wait_until
from .test_workflow_run import _make_workflow, _run, _start_run, _wait_terminal

CAP_NAME = "erp.order.export"
CAP_VERSION = "1.0.0"


def _publish_capability(client, name: str = CAP_NAME, version: str = CAP_VERSION) -> int:
    """Create capability + upload package + publish; returns the version id."""
    from .test_worker_capability import build_package

    res = client.post(
        "/api/capabilities",
        json={"name": name, "runtime_type": "PYTHON", "display_name": "ERP 订单导出"},
    )
    assert res.status_code == 201, res.text
    package = build_package(name, version, files={"main.py": "print('capability ok')"})
    res = client.post(
        f"/api/capabilities/{name}/versions",
        files={"file": (f"{name}-{version}.zip", package, "application/zip")},
    )
    assert res.status_code == 201, res.text
    version_id = res.json()["id"]
    res = client.post(f"/api/capability-versions/{version_id}/publish")
    assert res.status_code == 200, res.text
    return version_id


@pytest.fixture()
def device(client):
    payload = register_device(client, "能力流程测试机")
    from app.capability.service import CapabilityService

    with SessionLocal() as db:
        CapabilityService(db).replace_device_capabilities(
            payload["device_id"], [{"name": "echo", "version": "1.0"}]
        )
    return payload


# ------------------------------------------------------------------ definition


def test_capability_step_rejects_unknown_capability(client, device):
    res = client.post(
        "/api/workflows",
        json={
            "name": "bad_wf",
            "version": "1.0.0",
            "steps": [{
                "name": "export", "command": CAP_NAME, "capability_version": CAP_VERSION,
            }],
        },
    )
    assert res.status_code == 422, res.text
    assert CAP_NAME in res.text


def test_capability_step_rejects_unpublished_version(client, device):
    from .test_worker_capability import build_package

    res = client.post(
        "/api/capabilities",
        json={"name": CAP_NAME, "runtime_type": "PYTHON"},
    )
    assert res.status_code == 201, res.text
    package = build_package(CAP_NAME, "2.0.0", files={"main.py": "print('x')"})
    res = client.post(
        f"/api/capabilities/{CAP_NAME}/versions",
        files={"file": ("pkg.zip", package, "application/zip")},
    )
    assert res.status_code == 201, res.text  # DRAFT, never published

    res = client.post(
        "/api/workflows",
        json={
            "name": "draft_dep_wf",
            "version": "1.0.0",
            "steps": [{"name": "export", "command": CAP_NAME, "capability_version": "2.0.0"}],
        },
    )
    assert res.status_code == 422, res.text
    assert "2.0.0" in res.text


# --------------------------------------------------------------------- run


def test_capability_workflow_end_to_end_with_artifact(client, device):
    _publish_capability(client)
    worker = FakeWorker(
        client, device["device_token"], behaviour="success", capability_artifact=b"order export v1",
        # V1.6 P0 0.10: the resolver only selects workers advertising the
        # capability - the arbitrary-online fallback is gone.
        installed_capabilities=[{"name": CAP_NAME, "version": CAP_VERSION}],
    )
    worker.start()
    assert wait_for_worker_capabilities(client, device["device_id"], (CAP_NAME,))
    try:
        workflow_id = _make_workflow(
            client, device,
            steps=[{
                "name": "export_orders", "command": CAP_NAME, "capability_version": CAP_VERSION,
                "params": {"start_date": "2026-09-01"},
            }],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        assert run["status"] == "SUCCESS", run
        assert not worker.errors, worker.errors

        step = run["steps"][0]
        assert step["status"] == "SUCCESS"
        assert step["capability_version"] == CAP_VERSION  # §24 version snapshot

        # the engine created a CAPABILITY execution-type task
        from sqlalchemy import select

        with SessionLocal() as db:
            task = db.scalars(select(Task).where(Task.task_id == step["task_id"])).first()
            assert task is not None
            assert task.execution_type == "CAPABILITY"
            assert task.capability_name == CAP_NAME
            assert task.capability_version == CAP_VERSION

        # the dispatcher sent a capability.execute envelope with package identity
        dispatches = [m for m in worker.received if m.get("type") == "capability.execute"]
        assert len(dispatches) == 1
        data = dispatches[0]["data"]
        assert data["capability"] == CAP_NAME
        assert data["version"] == CAP_VERSION
        assert data["package_id"]
        assert data["workflow_run_id"] == run_id
        assert data["params"] == {"start_date": "2026-09-01"}

        # worker-uploaded artifact joined the run context (§27/§28)
        artifacts = run["context"].get("artifacts") or []
        assert len(artifacts) == 1
        assert artifacts[0]["name"] == "report.txt"
        assert artifacts[0]["step"] == "export_orders"
        assert artifacts[0]["artifact_id"]
    finally:
        worker.stop()


def test_capability_workflow_step_result_reaches_context(client, device):
    _publish_capability(client)
    worker = FakeWorker(
        client, device["device_token"], behaviour="success",
        installed_capabilities=[{"name": CAP_NAME, "version": CAP_VERSION}],
    )
    worker.start()
    assert wait_for_worker_capabilities(client, device["device_id"], (CAP_NAME,))
    try:
        workflow_id = _make_workflow(
            client, device,
            steps=[{"name": "export", "command": CAP_NAME, "capability_version": CAP_VERSION}],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        assert run["status"] == "SUCCESS"
        step_entry = run["context"]["steps"]["export"]
        assert step_entry["status"] == "SUCCESS"
        assert step_entry["result"]["capability"] == CAP_NAME
    finally:
        worker.stop()


def test_capability_workflow_failure_fails_run(client, device):
    _publish_capability(client)
    worker = FakeWorker(
        client, device["device_token"], behaviour="fail",
        installed_capabilities=[{"name": CAP_NAME, "version": CAP_VERSION}],
    )
    worker.start()
    assert wait_for_worker_capabilities(client, device["device_id"], (CAP_NAME,))
    try:
        workflow_id = _make_workflow(
            client, device,
            steps=[{"name": "export", "command": CAP_NAME, "capability_version": CAP_VERSION}],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        assert run["status"] == "FAILED"
        assert run["steps"][0]["status"] == "FAILED"
        assert run["steps"][0]["error_code"] == "CAPABILITY_EXECUTION_FAILED"
    finally:
        worker.stop()


def test_capability_workflow_stays_pending_without_online_worker(client, device):
    """No worker online -> task stays PENDING (resolver retry policy §65)."""
    _publish_capability(client)
    workflow_id = _make_workflow(
        client, device,
        steps=[{"name": "export", "command": CAP_NAME, "capability_version": CAP_VERSION}],
    )
    run_id = _start_run(client, workflow_id)
    assert wait_until(lambda: _run(client, run_id)["steps"][0]["task_id"], 10)
    step = _run(client, run_id)["steps"][0]
    with SessionLocal() as db:
        from sqlalchemy import select

        task = db.scalars(select(Task).where(Task.task_id == step["task_id"])).first()
        assert task is not None
        assert task.status == "PENDING"  # transient: no worker resolved yet
    # cleanup: cancel the run so it does not linger
    res = client.post(f"/api/workflow-runs/{run_id}/cancel")
    assert res.status_code == 200, res.text
