"""AgentHub Main Agent: rule planner + end-to-end agent run (MVP chain)."""

import threading

import pytest

from app.agent.legacy.planner import PlanError, build_plan
from app.core.config import settings


SEED_LIKE_COMMANDS = [
    {
        "command_name": "echo",
        "version": "1.0",
        "description": "回显测试命令：原样返回 message",
        "params_schema": {"message": "string"},
        "timeout": 30,
        "enabled": True,
    },
    {
        "command_name": "python.demo",
        "version": "1.0",
        "description": "在目标电脑上运行预注册的演示 Python 脚本",
        "params_schema": {},
        "timeout": 120,
        "enabled": True,
    },
]


@pytest.fixture(autouse=True)
def force_rule_planner():
    """Keep planner tests deterministic: no LLM even if .env defines a key."""
    original = settings.openai_api_key
    settings.openai_api_key = None
    yield
    settings.openai_api_key = original


def _context(devices=None, commands=None, capabilities=None):
    return {
        "devices": devices or [],
        "commands": commands or SEED_LIKE_COMMANDS,
        "capabilities": capabilities or [],
    }


# ---------------------------------------------------------------- planner


def test_rule_planner_echo_quoted_message_and_device_hint():
    devices = [{"device_id": "dev-1", "name": "办公室电脑01", "online": True}]
    plan = build_plan(None, "帮我 echo '部署完成' 在 办公室电脑01", _context(devices=devices))
    assert plan["steps"] == [{"command": "echo", "params": {"message": "部署完成"}}]
    assert plan["device_hint"] == "办公室电脑01"


def test_rule_planner_python_demo_code_fence():
    request = "运行 python.demo，代码：\n```python\nprint('agent ok')\n```"
    plan = build_plan(None, request, _context())
    assert plan["steps"][0]["command"] == "python.demo"
    assert plan["steps"][0]["params"]["code"] == "print('agent ok')"


def test_rule_planner_raises_without_match():
    with pytest.raises(PlanError):
        build_plan(None, "随便做点不相干的事情", _context())


# ------------------------------------------------------------------ API


def test_agent_run_replans_and_finishes(client):
    res = client.post("/api/agent/run", json={"request": "随便做点不相干的事情"})
    assert res.status_code == 200
    body = res.json()
    assert body["decision"] == "finish"
    assert body["replan_count"] == 2
    assert body["error"]["kind"] == "plan_invalid"
    assert body["task_id"] is None


def test_agent_run_full_chain_success(client, registered_device):
    """Fake worker: connect -> report capability -> answer task.dispatch."""
    token = registered_device["device_token"]
    box: dict = {}

    with client.websocket_connect(
        "/api/ws/device", headers={"Authorization": f"Bearer {token}"}
    ) as ws:
        welcome = ws.receive_json()
        assert welcome["type"] == "device.connected"

        ws.send_json(
            {
                "id": "cap_1",
                "type": "device.capabilities",
                "version": 1,
                "timestamp": 1,
                "data": {"capabilities": [{"name": "echo", "version": "1.0"}]},
            }
        )
        cap_ack = ws.receive_json()
        assert cap_ack["type"] == "message_ack"

        runner = threading.Thread(
            target=lambda: box.update(res=client.post("/api/agent/run", json={"request": "echo 'hello agent' 测试设备A"}))
        )
        runner.start()

        dispatch = ws.receive_json()
        assert dispatch["type"] == "task.dispatch"
        task_id = dispatch["data"]["task_id"]
        step_id = dispatch["data"]["step_id"]
        assert dispatch["data"]["command"] == "echo"
        assert dispatch["data"]["params"] == {"message": "hello agent"}

        ws.send_json({"id": "a1", "type": "task.accept", "version": 1, "timestamp": 1, "data": {"task_id": task_id, "step_id": step_id}})
        ws.receive_json()  # message_ack
        ws.send_json({"id": "r1", "type": "task.running", "version": 1, "timestamp": 1, "data": {"task_id": task_id, "step_id": step_id}})
        ws.receive_json()  # message_ack
        ws.send_json(
            {
                "id": "res1",
                "type": "task.result",
                "version": 1,
                "timestamp": 1,
                "data": {"task_id": task_id, "step_id": step_id, "status": "success", "result": {"message": "hello agent"}},
            }
        )
        ws.receive_json()  # message_ack

        runner.join(timeout=30)
        assert runner.is_alive() is False

    assert box["res"].status_code == 200
    body = box["res"].json()
    assert body["decision"] == "finish"
    assert body["task_id"] == task_id
    assert body["task_status"] == "SUCCESS"
    assert body["task_result"]["payload"]["result"] == {"message": "hello agent"}
    assert body["retry_count"] == 0
