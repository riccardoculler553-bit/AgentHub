"""V1.2 acceptance (PDF §175): the ten business scenarios over the real graph.

Each scenario drives AgentRunner + build_default_registry (the nine standard
tools) with a scripted LLM against live task machinery (FakeWorker / manual
WS sessions). No tool is mocked: this is the full
LLM -> policy -> tool -> TaskService -> dispatcher -> worker -> observe loop.
"""

import threading
import time

import pytest
from sqlalchemy import func, select

from app.agent.core.policies import ToolPolicy
from app.agent.core.runner import AgentRunner
from app.agent.llm.schemas import AgentDecision
from app.agent.runs import AgentRunService
from app.agent.tools.base import AgentTool
from app.agent.tools.registry import ToolRegistry, build_default_registry
from app.capability.service import CapabilityService
from app.core.config import settings
from app.db.database import SessionLocal
from app.task import models as schemas
from app.task.db_models import Task, TaskEvent
from app.task.dispatcher import TaskDispatcher
from app.task.service import TaskService

try:
    from agenthub._worker import FakeWorker, register_device, wait_for_capabilities
except ImportError:  # pragma: no cover - depends on pytest import mode
    from _worker import FakeWorker, register_device, wait_for_capabilities


# ----------------------------------------------------------------- scaffolding

class ScriptedLLM:
    """Pops one AgentDecision per decide() call; the queue spans run+resume."""

    def __init__(self, *decisions) -> None:
        self.decisions = list(decisions)

    async def decide(self, messages):  # noqa: ARG001
        return self.decisions.pop(0)


def dec(action, **kw) -> AgentDecision:
    return AgentDecision(action=action, **kw)


@pytest.fixture()
def hub(client):
    return client.app.state.hub


@pytest.fixture()
def registry(hub, monkeypatch):
    monkeypatch.setattr(settings, "agent_tool_wait_max", 3, raising=False)
    return build_default_registry(hub)


@pytest.fixture()
def device(client):
    """Registered offline device named 验收机A with the echo capability."""
    payload = register_device(client, "验收机A")
    with SessionLocal() as db:
        CapabilityService(db).replace_device_capabilities(
            payload["device_id"], [{"name": "echo", "version": "1.0"}]
        )
    return payload


def _runner(registry, *decisions) -> AgentRunner:
    return AgentRunner(registry=registry, llm=ScriptedLLM(*decisions))


def _create_echo_task(device_id: str) -> str:
    with SessionLocal() as db:
        task = TaskService(db).create(
            schemas.TaskCreateIn(
                name="acceptance",
                target_device_id=device_id,
                steps=[schemas.StepIn(command="echo", params={"message": "验收"})],
            ),
            created_by="tool_agent",
        )
        return task.task_id


def _dispatch(hub, task_id: str) -> None:
    import asyncio

    asyncio.get_event_loop().run_until_complete(TaskDispatcher(hub).dispatch_task(task_id))


def _task(task_id: str) -> Task:
    with SessionLocal() as db:
        row = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        assert row is not None
        db.refresh(row)
        return row


def _task_count() -> int:
    with SessionLocal() as db:
        return db.scalar(select(func.count()).select_from(Task)) or 0


def _force_failed(task_id: str) -> None:
    with SessionLocal() as db:
        task = db.scalars(select(Task).where(Task.task_id == task_id)).first()
        assert task is not None
        task.status = "FAILED"
        db.commit()


# ------------------------------------------------- 1. 运行审单 (real execution)

@pytest.mark.anyio
async def test_scenario_1_run_command(client, hub, registry, device):
    worker = FakeWorker(client, device["device_token"])
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"])
        final = await _runner(
            registry,
            dec(
                "tool_call",
                tool_name="execute_command",
                tool_args={
                    "command": "echo",
                    "device_name": "验收机A",
                    "params": {"message": "验收通过"},
                },
            ),
            dec("finish", answer="已在验收机A执行完成，结果：验收通过。"),
        ).run("帮我在验收机A上执行 echo 验收通过")
    finally:
        worker.stop()

    assert final["error"] is None
    assert final["reply"] == "已在验收机A执行完成，结果：验收通过。"
    result = final["tool_result"]
    assert result["success"] is True and result["data"]["status"] == "SUCCESS"
    with SessionLocal() as db:
        task = db.scalars(
            select(Task).where(Task.task_id == result["data"]["task_id"])
        ).first()
        assert task is not None and task.status == "SUCCESS"


# ------------------------------------------------------- 2. 查询审单结果 (READ)

@pytest.mark.anyio
async def test_scenario_2_query_task_result(client, hub, registry, device):
    worker = FakeWorker(client, device["device_token"])
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"])
        task_id = _create_echo_task(device["device_id"])
        await TaskDispatcher(hub).dispatch_task(task_id)
        assert _wait_status(task_id, "SUCCESS")
    finally:
        worker.stop()

    final = await _runner(
        registry,
        dec("tool_call", tool_name="get_task_detail", tool_args={"task_id": task_id}),
        dec("finish", answer=f"任务 {task_id} 已成功完成。"),
    ).run(f"查一下任务 {task_id} 的结果")
    assert "SUCCESS" in final["observations"][0]["fact"]
    assert final["reply"] == f"任务 {task_id} 已成功完成。"
    assert final["tool_call_count"] == 1


# ------------------------------------------------------- 3. 查询失败原因 (诊断)

@pytest.mark.anyio
async def test_scenario_3_diagnose_failure(client, hub, registry, device):
    worker = FakeWorker(client, device["device_token"], behaviour="fail")
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"])
        task_id = _create_echo_task(device["device_id"])
        await TaskDispatcher(hub).dispatch_task(task_id)
        assert _wait_status(task_id, "FAILED")
    finally:
        worker.stop()

    final = await _runner(
        registry,
        dec("tool_call", tool_name="get_task_detail", tool_args={"task_id": task_id}),
        dec("finish", answer="失败原因：执行器报错 EXECUTOR_FAILED（boom）。"),
    ).run(f"任务 {task_id} 为什么失败？")
    assert "FAILED" in final["observations"][0]["fact"]
    assert "EXECUTOR_FAILED" in final["reply"]


# ------------------------------------------------- 4. 符合条件自动 Retry (确认)

@pytest.mark.anyio
async def test_scenario_4_auto_retry_with_confirmation(client, hub, registry, device):
    task_id = _create_echo_task(device["device_id"])
    _force_failed(task_id)  # retryable state without a worker

    worker = FakeWorker(client, device["device_token"])
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"])
        runner = _runner(
            registry,
            dec("tool_call", tool_name="retry_task", tool_args={"task_id": task_id}),
            dec("finish", answer="已重试成功。"),
        )
        parked = await runner.run(f"把任务 {task_id} 再跑一次")
        assert parked["paused"] is True  # WRITE -> confirmation (§56)
        final = await runner.resume(parked, "是")
    finally:
        worker.stop()

    assert final["confirmed"] is True
    assert final["reply"] == "已重试成功。"
    assert _task(task_id).status == "SUCCESS"


# ----------------------------------------------------- 5. 取消正在运行任务

@pytest.mark.anyio
async def test_scenario_5_cancel_running_task(client, hub, registry, device):
    worker = FakeWorker(client, device["device_token"], behaviour="silent")
    worker.start()
    try:
        assert wait_for_capabilities(client, device["device_id"])
        task_id = _create_echo_task(device["device_id"])
        await TaskDispatcher(hub).dispatch_task(task_id)
        # silent worker: the dispatch envelope lands but the worker never
        # accepts, so the task parks in SENT (an open, cancellable state;
        # DISPATCHING is only the dispatcher's transient CAS state)
        assert _wait_status(task_id, "SENT")

        runner = _runner(
            registry,
            dec("tool_call", tool_name="cancel_task", tool_args={"task_id": task_id}),
            dec("finish", answer="已取消该任务。"),
        )
        parked = await runner.run(f"取消任务 {task_id}")
        assert parked["paused"] is True
        final = await runner.resume(parked, "确认")
    finally:
        worker.stop()

    assert final["reply"] == "已取消该任务。"
    assert _task(task_id).status == "CANCELLED"


# ----------------------------------------------------- 6. 设备不存在 → Ask User

@pytest.mark.anyio
async def test_scenario_6_unknown_device_asks_user(registry):
    final = await _runner(
        registry,
        dec(
            "tool_call",
            tool_name="execute_command",
            tool_args={"command": "echo", "device_name": "不存在的电脑"},
        ),
        dec("ask_user", answer="没有找到这台电脑，请问要操作哪台设备？"),
    ).run("在不存在的电脑上跑 echo")

    assert final["paused"] is True
    assert "DEVICE_NOT_FOUND" in final["observations"][0]["fact"]
    assert final["reply"] == "没有找到这台电脑，请问要操作哪台设备？"
    assert final["error"] is None  # asking is not an error (§47)
    assert final["tool_call_count"] == 1
    assert _task_count() == 0  # no doomed task was created


# ----------------------------------------------------- 7. 设备离线 → Replan

@pytest.mark.anyio
async def test_scenario_7_offline_device_replans(registry, device):
    final = await _runner(
        registry,
        dec(
            "tool_call",
            tool_name="execute_command",
            tool_args={"command": "echo", "device_name": "验收机A", "params": {"message": "hi"}},
        ),
        dec("tool_call", tool_name="get_device_status", tool_args={"device_name": "验收机A"}),
        dec("finish", answer="验收机A当前离线，任务已排队，设备上线后会自动执行。"),
    ).run("在验收机A上跑 echo")

    assert "PENDING" in final["observations"][0]["fact"]  # queued, not lost
    assert '"online"' in final["observations"][1]["fact"]  # LLM saw the live status
    assert final["reply"].startswith("验收机A当前离线")
    # the agent-created task must be queued (tool_result is the LAST tool's
    # result - get_device_status - so the task is read from the DB instead)
    with SessionLocal() as db:
        row = db.scalars(
            select(Task).where(Task.target_device_id == device["device_id"])
        ).first()
        assert row is not None and row.status == "PENDING"


# ------------------------------------------------- 8. Task Result Late Event

@pytest.mark.anyio
async def test_scenario_8_late_result_never_overwrites(client, hub, registry, device):
    """A success result arriving after CANCELLED is recorded as a late event;
    the agent later reads the truthful CANCELLED status."""
    box: dict = {}

    def worker_flow():
        try:
            with client.websocket_connect(
                "/api/ws/device", headers={"Authorization": f"Bearer {device['device_token']}"}
            ) as ws:
                ws.receive_json()  # device.connected
                ws.send_json(
                    {
                        "id": "cap_1",
                        "type": "device.capabilities",
                        "version": 1,
                        "timestamp": 1,
                        "data": {"capabilities": [{"name": "echo", "version": "1.0"}]},
                    }
                )
                ws.receive_json()  # message_ack
                box["caps"] = True
                # the main thread signals right before dispatching, so this
                # receive blocks only for the (guaranteed) dispatch envelope
                if not wait_until(lambda: box.get("go"), timeout=15):
                    box["error"] = "main thread never dispatched"
                    return
                dispatch = ws.receive_json()  # task.dispatch
                d = dispatch["data"]
                ws.send_json({"id": "a", "type": "task.accept", "version": 1, "timestamp": 1, "data": {"task_id": d["task_id"], "step_id": d["step_id"], "attempt_id": d.get("attempt_id", "")}})
                ws.receive_json()  # ack
                ws.send_json({"id": "r", "type": "task.running", "version": 1, "timestamp": 1, "data": {"task_id": d["task_id"], "step_id": d["step_id"], "attempt_id": d.get("attempt_id", "")}})
                ws.receive_json()  # ack
                box["ready"] = True
                if not wait_until(lambda: box.get("cancel_done"), timeout=20):
                    box["error"] = "task was never cancelled"
                    return
                # late result: arrives AFTER the task was cancelled
                ws.send_json({"id": "x", "type": "task.result", "version": 1, "timestamp": 1, "data": {"task_id": d["task_id"], "step_id": d["step_id"], "attempt_id": d.get("attempt_id", ""), "status": "success", "result": {"echo": "late"}}})
                ws.receive_json()  # ack
        except Exception as exc:  # surface thread errors to the main thread
            box["error"] = repr(exc)

    thread = threading.Thread(target=worker_flow, daemon=True)
    thread.start()
    assert wait_until(lambda: box.get("caps"), timeout=10), box.get("error")
    assert wait_for_capabilities(client, device["device_id"])

    task_id = _create_echo_task(device["device_id"])
    box["go"] = True
    assert await TaskDispatcher(hub).dispatch_task(task_id), "dispatch failed"
    assert _wait_status(task_id, "RUNNING")

    with SessionLocal() as db:
        TaskService(db).request_cancel(task_id)
    box["cancel_done"] = True
    thread.join(timeout=10)
    assert not thread.is_alive(), f"worker thread stuck: {box.get('error')}"

    # the late result must NOT resurrect the cancelled task
    assert _task(task_id).status == "CANCELLED"
    with SessionLocal() as db:
        event = db.scalars(
            select(TaskEvent).where(
                TaskEvent.task_id == task_id, TaskEvent.event_type == "task.late_result"
            )
        ).first()
        assert event is not None  # late result recorded as an audit event

    final = await _runner(
        registry,
        dec("tool_call", tool_name="get_task_detail", tool_args={"task_id": task_id}),
        dec("finish", answer="任务已取消，迟到的结果被记录为滞后事件，不影响状态。"),
    ).run(f"任务 {task_id} 现在什么状态？")
    assert "CANCELLED" in final["observations"][0]["fact"]
    assert "已取消" in final["reply"]


# ------------------------------------------------- 9. Agent 重复 Tool Call

@pytest.mark.anyio
async def test_scenario_9_repeated_tool_call_rejected(registry):
    from pydantic import BaseModel

    executed: list = []

    class _N(BaseModel):
        model_config = {"extra": "forbid"}

    async def dup_handler(db, args):  # noqa: ARG001
        executed.append(dict(args))
        return ToolResult.ok({"n": len(executed)})

    reg: ToolRegistry = registry
    reg.register(
        AgentTool(
            name="probe_dup",
            description="dup probe",
            handler=dup_handler,
            args_schema=_N,
            max_calls=2,
        )
    )
    final = await _runner(
        reg,
        dec("tool_call", tool_name="probe_dup"),
        dec("tool_call", tool_name="probe_dup"),
        dec("tool_call", tool_name="probe_dup"),  # 3rd: per-tool limit -> rejected
        dec("finish", answer="已两次调用，第三次被策略拒绝。"),
    ).run("重复探测")

    assert len(executed) == 2
    assert "MAX_CALLS_EXCEEDED" in final["observations"][-1]["fact"]
    assert final["reply"] == "已两次调用，第三次被策略拒绝。"


# --------------------------------------------- 10. Tool Policy 拒绝危险调用

@pytest.mark.anyio
async def test_scenario_10_policy_rejects_dangerous_calls(registry, device):
    before = _task_count()
    final = await _runner(
        registry,
        # parameter smuggling: script_path is not in the schema (extra=forbid)
        dec(
            "tool_call",
            tool_name="execute_command",
            tool_args={
                "command": "echo",
                "device_name": "验收机A",
                "params": {"message": "x", "script_path": "D:/evil.py"},
            },
        ),
        # unregistered command
        dec("tool_call", tool_name="execute_command", tool_args={"command": "rm.rf", "device_name": "验收机A"}),
        dec("finish", answer="已拒绝：参数不合法且命令未注册。"),
    ).run("执行这个脚本")

    facts = " | ".join(o["fact"] for o in final["observations"])
    assert "INVALID_ARGS" in facts
    assert "COMMAND_NOT_FOUND" in facts
    assert final["reply"] == "已拒绝：参数不合法且命令未注册。"
    assert _task_count() == before  # nothing executed


# ------------------------------------------------------------------- helpers

def _wait_status(task_id: str, status: str, timeout: float = 10) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _task(task_id).status == status:
            return True
        time.sleep(0.1)
    return False


def wait_until(predicate, timeout: float = 10, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False
