"""V1.2 core tools integration tests (PDF Phase 3 / §169).

Every tool must be callable WITHOUT the LLM: registry -> tool.run(db, args).
Full-loop execute_command runs against FakeWorker; the rest use direct DB
fixtures (V1.1 services stay the source of truth). Confirmation-gated calls
go through ToolPolicy exactly like the execute_tool node will.
"""

import pytest
from sqlalchemy import select

from app.agent.tools.base import ToolErrorCodes
from app.agent.tools.registry import build_default_registry
from app.agent.core.policies import ToolPolicy
from app.capability.service import CapabilityService
from app.core.config import settings
from app.db.database import SessionLocal
from app.task import models as schemas
from app.task.db_models import Task
from app.task.service import TaskService

from ._worker import FakeWorker, register_device, wait_for_capabilities


@pytest.fixture()
def hub(client):
    return client.app.state.hub


@pytest.fixture()
def registry(hub, monkeypatch):
    # keep offline-wait tool calls snappy in tests
    monkeypatch.setattr(settings, "agent_tool_wait_max", 1, raising=False)
    return build_default_registry(hub)


@pytest.fixture()
def policy(registry):
    return ToolPolicy(registry)


@pytest.fixture()
def device(client, monkeypatch):
    """A registered device named 工具测试机 with the echo capability."""
    monkeypatch.setattr(settings, "agent_tool_wait_max", 1, raising=False)
    payload = register_device(client, "工具测试机")
    with SessionLocal() as db:
        CapabilityService(db).replace_device_capabilities(
            payload["device_id"], [{"name": "echo", "version": "1.0"}]
        )
    return payload


def _create_echo_task(device_id: str, created_by: str = "tool_agent") -> str:
    with SessionLocal() as db:
        task = TaskService(db).create(
            schemas.TaskCreateIn(
                name="tool-test",
                target_device_id=device_id,
                steps=[schemas.StepIn(command="echo", params={"message": "hi"})],
            ),
            created_by=created_by,
        )
        return task.task_id


def _get_task(task_id: str) -> Task | None:
    with SessionLocal() as db:
        return db.scalars(select(Task).where(Task.task_id == task_id)).first()


def _force_failed(task_id: str) -> None:
    """Test surgery: no worker needed to reach a retryable state."""
    with SessionLocal() as db:
        task = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        assert task is not None
        task.status = "FAILED"
        db.commit()


async def _call(registry, name: str, args: dict):
    """Direct tool invocation (no policy, no LLM) - Phase 3 contract (§169)."""
    tool = registry.get(name)
    assert tool is not None, f"tool {name} missing"
    with SessionLocal() as db:
        return await tool.run(db, args)


async def _call_policy(policy, name: str, args: dict, confirmed: bool = False):
    """Policy-mediated invocation - the exact path the execute_tool node uses."""
    with SessionLocal() as db:
        return await policy.execute(db, tool_name=name, args=args, confirmed=confirmed)


# ------------------------------------------------------------------ catalogue


def test_default_registry_has_standard_tools(registry):
    """V1.5: nine V1.2 tools + five workflow tools + three capability tools
    + one artifact tool (18)."""
    expected = {
        "list_devices": ("READ", False),
        "get_device_status": ("READ", False),
        "get_device_capabilities": ("READ", False),
        "get_recent_tasks": ("READ", False),
        "get_task_detail": ("READ", False),
        "get_task_events": ("READ", False),
        "retry_task": ("WRITE", False),
        "cancel_task": ("WRITE", True),
        "execute_command": ("ACTION", settings.agent_confirm_actions),
        # V1.3 workflow tools (§115-§117)
        "list_workflows": ("READ", False),
        "get_workflow": ("READ", False),
        "get_workflow_run": ("READ", False),
        "run_workflow": ("ACTION", settings.agent_confirm_actions),
        "cancel_workflow_run": ("WRITE", True),
        # V1.4 capability tools (§33)
        "list_capabilities": ("READ", False),
        "get_capability": ("READ", False),
        "run_capability": ("ACTION", settings.agent_confirm_actions),
        # V1.5 artifact tool (结果落盘)
        "save_artifact": ("WRITE", False),
    }
    assert {t.name for t in registry.all()} == set(expected)
    for name, (risk, confirm) in expected.items():
        tool = registry.get(name)
        assert tool.risk_level.value == risk, name
        assert tool.requires_confirmation is confirm, name
    # wait-type tools never let the LLM poll (PDF §44)
    assert registry.get("retry_task").waits_task
    assert registry.get("execute_command").waits_task
    assert registry.get("run_workflow").waits_task
    assert registry.get("run_capability").waits_task


# --------------------------------------------------------------------- device


@pytest.mark.anyio
async def test_list_devices_reports_busy(registry, device):
    _create_echo_task(device["device_id"])  # PENDING = live = busy
    r = await _call(registry, "list_devices", {})
    assert r.success
    entry = next(d for d in r.data["devices"] if d["name"] == "工具测试机")
    assert entry["device_id"] == device["device_id"]
    assert entry["busy"] is True  # offline in hub but has a live task
    assert entry["online"] is False


@pytest.mark.anyio
async def test_get_device_status_unknown_device(registry):
    r = await _call(registry, "get_device_status", {"device_name": "不存在的电脑"})
    assert not r.success and r.error_code == ToolErrorCodes.DEVICE_NOT_FOUND


@pytest.mark.anyio
async def test_get_device_capabilities(registry, device):
    r = await _call(registry, "get_device_capabilities", {"device_name": "工具测试机"})
    assert r.success
    assert {"command": "echo", "version": "1.0", "enabled": True} in r.data["capabilities"]


# ----------------------------------------------------------------------- task


@pytest.mark.anyio
async def test_get_recent_tasks_filters_by_status(registry, device):
    t1 = _create_echo_task(device["device_id"])
    t2 = _create_echo_task(device["device_id"])
    _force_failed(t2)

    r_all = await _call(registry, "get_recent_tasks", {"device_name": "工具测试机", "limit": 10})
    ids = [t["task_id"] for t in r_all.data["tasks"]]
    assert {t1, t2} <= set(ids)

    r_failed = await _call(
        registry, "get_recent_tasks", {"device_name": "工具测试机", "status": "FAILED"}
    )
    assert [t["task_id"] for t in r_failed.data["tasks"]] == [t2]


@pytest.mark.anyio
async def test_get_task_detail_and_events(registry, device):
    tid = _create_echo_task(device["device_id"])
    r = await _call(registry, "get_task_detail", {"task_id": tid})
    assert r.success
    assert r.data["status"] == "PENDING"
    assert r.data["commands"] == ["echo"]
    assert r.data["steps"][0]["command"] == "echo"

    r_events = await _call(registry, "get_task_events", {"task_id": tid})
    types = [e["event_type"] for e in r_events.data["events"]]
    assert "task.created" in types


@pytest.mark.anyio
async def test_get_task_detail_unknown(registry):
    r = await _call(registry, "get_task_detail", {"task_id": "task-nope"})
    assert not r.success and r.error_code == ToolErrorCodes.TASK_NOT_FOUND


# --------------------------------------------------------------------- write


@pytest.mark.anyio
async def test_retry_does_not_require_confirmation(policy, device):
    """V1.5: 用户消息本身就是重试指令，retry_task 不再走确认门 ——
    之前的确认流程会造成"提示确认 → 非'是'回复取消 → 再要求确认"死循环。"""
    tid = _create_echo_task(device["device_id"])
    r = await _call_policy(policy, "retry_task", {"task_id": tid})
    assert r.error_code != ToolErrorCodes.CONFIRMATION_REQUIRED


@pytest.mark.anyio
async def test_retry_failed_task_requeues_and_waits(policy, device):
    tid = _create_echo_task(device["device_id"])
    _force_failed(tid)
    r = await _call_policy(policy, "retry_task", {"task_id": tid}, confirmed=True)
    # device offline -> dispatch False -> stays PENDING for TaskMonitor
    assert r.success, r
    assert r.data["task_id"] == tid
    assert r.data["status"] == "PENDING"


@pytest.mark.anyio
async def test_retry_rejects_pending_task(policy, device):
    """PENDING is not retryable: TaskService policy decides, not the LLM (§55)."""
    tid = _create_echo_task(device["device_id"])
    r = await _call_policy(policy, "retry_task", {"task_id": tid}, confirmed=True)
    assert not r.success and r.error_code == ToolErrorCodes.INVALID_TASK_STATE


@pytest.mark.anyio
async def test_retry_task_not_found(policy):
    r = await _call_policy(policy, "retry_task", {"task_id": "task-x"}, confirmed=True)
    assert r.error_code == ToolErrorCodes.TASK_NOT_FOUND


@pytest.mark.anyio
async def test_cancel_pending_task(policy, device):
    tid = _create_echo_task(device["device_id"])
    r = await _call_policy(policy, "cancel_task", {"task_id": tid}, confirmed=True)
    assert r.success
    assert r.data["status"] == "CANCELLED"
    assert _get_task(tid).status == "CANCELLED"


# ------------------------------------------------------------------- command


@pytest.mark.anyio
async def test_execute_command_full_loop_with_worker(registry, client, device):
    worker = FakeWorker(client, device["device_token"], capabilities=("echo",), behaviour="success")
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"], ("echo",))
        r = await _call(
            registry,
            "execute_command",
            {"command": "echo", "device_name": "工具测试机", "params": {"message": "v1.2"}},
        )
        assert r.success, r
        assert r.data["status"] == "SUCCESS"
        assert r.data["device_name"] == "工具测试机"
        assert r.data["result"]["payload"]["result"]["echo"] == "v1.2"
    finally:
        worker.stop()


@pytest.mark.anyio
async def test_execute_command_busy_device(registry, device):
    _create_echo_task(device["device_id"])  # live task -> busy
    r = await _call(
        registry, "execute_command", {"command": "echo", "device_name": "工具测试机"}
    )
    assert not r.success and r.error_code == ToolErrorCodes.DEVICE_BUSY


@pytest.mark.anyio
async def test_execute_command_unknown_command(registry, device):
    r = await _call(
        registry,
        "execute_command",
        {"command": "totally.unknown", "device_name": "工具测试机"},
    )
    assert not r.success
    assert r.error_code in (ToolErrorCodes.COMMAND_NOT_FOUND, ToolErrorCodes.VALIDATION_FAILED)


@pytest.mark.anyio
async def test_execute_command_rejects_injected_script_path(registry, device):
    """PDF §150: the schema must leave nowhere for a hallucinated path."""
    r = await _call(
        registry,
        "execute_command",
        {"command": "echo", "device_name": "工具测试机", "script_path": "D:\\evil.py"},
    )
    assert not r.success and r.error_code == ToolErrorCodes.INVALID_ARGS
    assert "script_path" in r.message


@pytest.mark.anyio
async def test_execute_command_offline_device_stays_pending(registry, device):
    r = await _call(
        registry,
        "execute_command",
        {"command": "echo", "device_name": "工具测试机", "params": {"message": "offline"}},
    )
    # no worker connected: dispatch fails, task waits for TaskMonitor
    assert r.success
    assert r.data["status"] == "PENDING"
    task = _get_task(r.data["task_id"])
    assert task is not None and task.created_by == "tool_agent"
