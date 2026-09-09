"""V1.4 acceptance tests (§85 matrix): capability lifecycle over the real
Server <-> FakeWorker chain, scenario by scenario.

Covered here (the rest live in unit/integration files):
  1/2  注册 Capability + 发布版本 (+ version immutability)
  3    Worker 上报已安装能力 (worker.capabilities -> worker_capabilities)
  10   Capability Retry
  11   Capability Cancel
  13   Workflow 跨 Runtime（命令步骤 + 能力步骤混排）
  14   Agent 调用 Capability (test_agent_capability_tools.py)
  18   Capability Version 回滚（pinned 旧版本仍可执行）
"""

import pytest
from sqlalchemy import select

from app.db.database import SessionLocal
from app.task.db_models import Task
from app.workflow.db_models import WorkflowStepRun

from ._worker import FakeWorker, register_device, wait_until
from .test_workflow_capability import CAP_NAME, CAP_VERSION, _publish_capability
from .test_workflow_run import _make_workflow, _run, _start_run, _wait_terminal


@pytest.fixture()
def device(client):
    payload = register_device(client, "验收能力测试机")
    from app.capability.service import CapabilityService

    with SessionLocal() as db:
        CapabilityService(db).replace_device_capabilities(
            payload["device_id"], [{"name": "echo", "version": "1.0"}]
        )
    return payload


def _task(task_id: str) -> Task | None:
    with SessionLocal() as db:
        return db.scalars(select(Task).where(Task.task_id == task_id)).first()


# ------------------------------------------------------- scenario 1/2: publish


def test_capability_register_publish_and_immutability(client):
    version_id = _publish_capability(client)  # create + upload + publish

    res = client.get("/api/capabilities/erp.order.export")
    assert res.status_code == 200
    assert res.json()["current_version"] == CAP_VERSION

    # re-publish is idempotent
    res = client.post(f"/api/capability-versions/{version_id}/publish")
    assert res.status_code == 200

    # version identity is immutable: same version re-upload -> 409
    from .test_worker_capability import build_package

    package = build_package(CAP_NAME, CAP_VERSION, files={"main.py": "print('dup')"})
    res = client.post(
        f"/api/capabilities/{CAP_NAME}/versions",
        files={"file": ("dup.zip", package, "application/zip")},
    )
    assert res.status_code == 409, res.text


# ------------------------------------------------ scenario 3: worker caps report


def test_worker_reports_installed_capabilities(client, device):
    worker = FakeWorker(
        client, device["device_token"], behaviour="caps_only",
        installed_capabilities=[{"name": CAP_NAME, "version": CAP_VERSION}],
    )
    worker.start()
    try:
        def reported() -> bool:
            workers = client.get("/api/worker-capabilities").json()
            return any(
                any(c["name"] == CAP_NAME and c["version"] == CAP_VERSION for c in w["capabilities"])
                for w in workers
            )

        assert wait_until(reported, 10)
    finally:
        worker.stop()


# ---------------------------------------------------- scenario 10: retry


def test_capability_task_retry(client, device):
    _publish_capability(client)
    failing = FakeWorker(client, device["device_token"], behaviour="fail")
    failing.start()
    res = client.post(
        "/api/tasks",
        json={
            "name": "验收重试",
            "steps": [{"command": CAP_NAME, "params": {}}],
            "execution_type": "CAPABILITY",
        },
    )
    assert res.status_code == 201, res.text
    task_id = res.json()["task_id"]
    assert wait_until(lambda: (_task(task_id)).status == "FAILED", 20), "task never failed"
    failing.stop()

    # swap in a healthy worker, then retry through the API
    healthy = FakeWorker(client, device["device_token"], behaviour="success")
    healthy.start()
    try:
        res = client.post(f"/api/tasks/{task_id}/retry")
        assert res.status_code == 200, res.text
        assert wait_until(lambda: (_task(task_id)).status == "SUCCESS", 20)
    finally:
        healthy.stop()


# ---------------------------------------------------- scenario 11: cancel


def test_capability_task_cancel(client, device):
    _publish_capability(client)
    worker = FakeWorker(client, device["device_token"], behaviour="silent")
    worker.start()
    try:
        res = client.post(
            "/api/tasks",
            json={
                "name": "验收取消",
                "steps": [{"command": CAP_NAME, "params": {}}],
                "execution_type": "CAPABILITY",
            },
        )
        task_id = res.json()["task_id"]
        assert wait_until(lambda: (_task(task_id)).status == "SENT", 20), "task never dispatched"
        res = client.post(f"/api/tasks/{task_id}/cancel")
        assert res.status_code == 200, res.text
        assert wait_until(lambda: (_task(task_id)).status == "CANCELLED", 20)
    finally:
        worker.stop()


# --------------------------------------- scenario 13: workflow across runtimes


def test_workflow_mixed_command_and_capability_steps(client, device):
    _publish_capability(client)
    worker = FakeWorker(client, device["device_token"], behaviour="success")
    worker.start()
    try:
        assert wait_until(lambda: client.get(
            f"/api/devices/{device['device_id']}/capabilities").json().get("capabilities"), 10)
        workflow_id = _make_workflow(
            client, device,
            steps=[
                {"name": "ping", "command": "echo", "params": {"message": "wake"}},
                {"name": "export", "command": CAP_NAME, "capability_version": CAP_VERSION},
            ],
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        assert run["status"] == "SUCCESS", run
        assert [s["status"] for s in run["steps"]] == ["SUCCESS", "SUCCESS"]
        # the two steps ran on two different execution engines
        with SessionLocal() as db:
            statuses = {
                s["name"]: db.scalars(
                    select(Task).where(Task.task_id == s["task_id"])
                ).first()
                for s in run["steps"]
            }
        assert statuses["ping"].execution_type == "LEGACY_COMMAND"
        assert statuses["export"].execution_type == "CAPABILITY"
    finally:
        worker.stop()


# ------------------------------------------- scenario 18: version pin / rollback


def test_capability_version_rollback_via_pin(client, device):
    """Publish 1.0.0 then 1.1.0: unpinned tasks follow current (1.1.0) while a
    step pinned to 1.0.0 keeps dispatching exactly 1.0.0 (§24 reproducibility)."""
    _publish_capability(client)  # 1.0.0 current
    from .test_worker_capability import build_package

    package = build_package(CAP_NAME, "1.1.0", files={"main.py": "print('v11')"})
    res = client.post(
        f"/api/capabilities/{CAP_NAME}/versions",
        files={"file": ("v11.zip", package, "application/zip")},
    )
    assert res.status_code == 201, res.text
    res = client.post(f"/api/capability-versions/{res.json()['id']}/publish")
    assert res.status_code == 200
    assert client.get(f"/api/capabilities/{CAP_NAME}").json()["current_version"] == "1.1.0"

    worker = FakeWorker(client, device["device_token"], behaviour="success")
    worker.start()
    try:
        workflow_id = _make_workflow(
            client, device,
            steps=[{"name": "export", "command": CAP_NAME, "capability_version": CAP_VERSION}],
            name="pinned_export",
        )
        run_id = _start_run(client, workflow_id)
        run = _wait_terminal(client, run_id)
        assert run["status"] == "SUCCESS", run

        dispatched = [m for m in worker.received if m.get("type") == "capability.execute"]
        assert len(dispatched) == 1
        assert dispatched[0]["data"]["version"] == CAP_VERSION  # pinned, not current

        step = db_step_run(run_id, "export")
        assert step.capability_version == CAP_VERSION
    finally:
        worker.stop()


def db_step_run(run_id: str, name: str) -> WorkflowStepRun:
    with SessionLocal() as db:
        row = db.scalars(
            select(WorkflowStepRun).where(
                WorkflowStepRun.run_id == run_id, WorkflowStepRun.name == name
            )
        ).first()
        assert row is not None
        db.expunge(row)
        return row
