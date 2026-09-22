"""V1.4 Capability Agent tools integration tests (Phase 8, §33/§36-§37).

Direct tool invocation (no LLM): registry -> tool.run(db, args). run_capability
drives the full chain: TaskService (CAPABILITY) -> Dispatcher -> FakeWorker
capability lifecycle -> artifacts -> ToolResult.
"""

import pytest
from sqlalchemy import select

from app.agent.tools.base import ToolErrorCodes
from app.agent.tools.registry import build_default_registry
from app.core.config import settings
from app.db.database import SessionLocal
from app.artifact.db_models import Artifact

from ._worker import FakeWorker, register_device, wait_for_capabilities, wait_for_worker_capabilities
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
    payload = register_device(client, "能力工具测试机")
    from app.capability.service import CapabilityService

    with SessionLocal() as db:
        CapabilityService(db).replace_device_capabilities(
            payload["device_id"], [{"name": "echo", "version": "1.0"}]
        )
    return payload


async def _call(registry, name: str, args: dict):
    tool = registry.get(name)
    assert tool is not None, f"tool {name} missing"
    with SessionLocal() as db:
        return await tool.run(db, args)


# ----------------------------------------------------------------------- read


@pytest.mark.anyio
async def test_list_capabilities(client, registry):
    _publish_capability(client)
    r = await _call(registry, "list_capabilities", {})
    assert r.success
    names = [c["name"] for c in r.data["capabilities"]]
    assert "erp.order.export" in names
    entry = next(c for c in r.data["capabilities"] if c["name"] == "erp.order.export")
    assert entry["current_version"] == "1.0.0"
    assert entry["runtime_type"] == "PYTHON"


@pytest.mark.anyio
async def test_get_capability_details_and_versions(client, registry):
    _publish_capability(client)
    r = await _call(registry, "get_capability", {"capability": "erp.order.export"})
    assert r.success
    assert r.data["current_version"] == "1.0.0"
    assert any(v["version"] == "1.0.0" and v["status"] == "PUBLISHED" for v in r.data["versions"])


@pytest.mark.anyio
async def test_get_capability_unknown(registry):
    r = await _call(registry, "get_capability", {"capability": "no.such.thing"})
    assert not r.success and r.error_code == "CAPABILITY_NOT_FOUND"


# ------------------------------------------------------------------------ run


@pytest.mark.anyio
async def test_run_capability_end_to_end(client, registry, device):
    _publish_capability(client)
    worker = FakeWorker(
        client, device["device_token"], behaviour="success", capability_artifact=b"tool artifact",
        # V1.6 P0 0.10: the resolver only selects workers advertising the
        # capability - the arbitrary-online fallback is gone.
        installed_capabilities=[{"name": "erp.order.export", "version": "1.0.0"}],
    )
    worker.start()
    assert wait_for_worker_capabilities(client, device["device_id"], ("erp.order.export",))
    try:
        r = await _call(
            registry, "run_capability",
            {"capability": "erp.order.export", "params": {"start_date": "2026-09-09"}, "wait": True},
        )
        assert r.success, r
        data = r.data
        assert data["capability"] == "erp.order.export"
        assert data["version"] == "1.0.0"
        assert data["status"] == "SUCCESS"
        assert data["result"]["capability"] == "erp.order.export"
        assert len(data["artifacts"]) == 1
        assert data["artifacts"][0]["name"] == "report.txt"

        with SessionLocal() as db:
            row = db.scalars(
                select(Artifact).where(Artifact.artifact_id == data["artifacts"][0]["artifact_id"])
            ).first()
            assert row is not None
            assert row.task_id == data["task_id"]
    finally:
        worker.stop()


@pytest.mark.anyio
async def test_run_capability_unknown_fails_validation(registry):
    r = await _call(registry, "run_capability", {"capability": "no.such.thing"})
    assert not r.success
    assert r.error_code == "TASK_VALIDATION_FAILED"


@pytest.mark.anyio
async def test_run_capability_unpinned_version_resolves_current(client, registry, device):
    """version=None -> TaskService pins the capability's current PUBLISHED one."""
    _publish_capability(client)
    worker = FakeWorker(
        client, device["device_token"], behaviour="success",
        installed_capabilities=[{"name": "erp.order.export", "version": "1.0.0"}],
    )
    worker.start()
    assert wait_for_worker_capabilities(client, device["device_id"], ("erp.order.export",))
    try:
        r = await _call(registry, "run_capability", {"capability": "erp.order.export", "wait": True})
        assert r.success, r
        assert r.data["version"] == "1.0.0"
    finally:
        worker.stop()


# ---------------------------------------------------- V1.5 §11 device / §15 inputs


@pytest.mark.anyio
async def test_run_capability_resolves_device_name(client, registry, device):
    """§11: the Agent speaks device NAMES; only the resolved id reaches the
    execution layer (persisted on the task by the dispatcher)."""
    from app.task.db_models import Task

    _publish_capability(client)
    worker = FakeWorker(client, device["device_token"], behaviour="success")
    worker.start()
    try:
        r = await _call(registry, "run_capability", {
            "capability": "erp.order.export", "device": "能力工具测试机", "wait": True,
        })
        assert r.success, r
        with SessionLocal() as db:
            task = db.scalars(
                select(Task).where(Task.task_id == r.data["task_id"])
            ).first()
            assert task is not None
            assert task.target_device_id == device["device_id"]
    finally:
        worker.stop()


@pytest.mark.anyio
async def test_run_capability_unknown_device_fails(client, registry):
    r = await _call(
        registry, "run_capability",
        {"capability": "erp.order.export", "device": "不存在的电脑"},
    )
    assert not r.success
    assert r.error_code == "DEVICE_NOT_FOUND"


@pytest.mark.anyio
async def test_run_capability_with_input_artifacts(client, registry, device):
    """§15/§26: tool inputs -> task artifact references -> dispatch refs."""
    _publish_capability(client)
    worker = FakeWorker(client, device["device_token"], behaviour="success")
    worker.start()
    try:
        res = client.post(
            "/api/artifacts",
            files={"file": ("data.xlsx", b"INPUT-BYTES", "application/octet-stream")},
            data={"name": "data.xlsx", "type": "file"},
            headers={"Authorization": f"Bearer {device['device_token']}"},
        )
        assert res.status_code == 201, res.text
        artifact_id = res.json()["artifact_id"]

        r = await _call(registry, "run_capability", {
            "capability": "erp.order.export",
            "device": "能力工具测试机",
            "inputs": {"data_dir": [artifact_id]},
            "wait": True,
        })
        assert r.success, r
        execute = next(m for m in worker.received if m.get("type") == "capability.execute")
        refs = execute["data"]["input_artifacts"]
        assert len(refs) == 1
        assert refs[0]["artifact_id"] == artifact_id
        assert refs[0]["name"] == "data.xlsx"
        assert refs[0]["role"] == "data_dir"
        assert refs[0]["checksum"]  # worker verifies against this
    finally:
        worker.stop()
