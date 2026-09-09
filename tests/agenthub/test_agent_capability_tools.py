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

from ._worker import FakeWorker, register_device, wait_for_capabilities
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
    )
    worker.start()
    try:
        r = await _call(
            registry, "run_capability",
            {"capability": "erp.order.export", "params": {"start_date": "2026-09-09"}},
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
    worker = FakeWorker(client, device["device_token"], behaviour="success")
    worker.start()
    try:
        r = await _call(registry, "run_capability", {"capability": "erp.order.export"})
        assert r.success, r
        assert r.data["version"] == "1.0.0"
    finally:
        worker.stop()
