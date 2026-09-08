"""V1.2 agent run APIs (PDF §91): resume / cancel / detail over HTTP.

Runs are driven by an AgentService wired to a scripted LLM, installed on
app.state (AGENT_MODE=tool_agent shape) without touching real config.
"""

import pytest

from app.agent.service import AgentService
from app.agent.tools.base import AgentTool
from app.agent.tools.registry import ToolRegistry
from app.main import app as fastapi_app
from pydantic import BaseModel, Field

try:
    from agenthub._worker import wait_until
except ImportError:  # pragma: no cover - depends on pytest import mode
    from _worker import wait_until


class _Text(BaseModel):
    model_config = {"extra": "forbid"}

    text: str = Field(default="")


async def _echo(db, args):  # noqa: ARG001
    return {"echo": args.get("text", "")}


def _dec(action, **kw):
    from app.agent.llm.schemas import AgentDecision

    return AgentDecision(action=action, **kw)


class _ScriptedLLM:
    def __init__(self, *decisions) -> None:
        self.decisions = list(decisions)

    async def decide(self, messages):  # noqa: ARG001
        return self.decisions.pop(0)


def _install_service(*decisions) -> None:
    reg = ToolRegistry()
    reg.register(
        AgentTool(
            name="probe",
            description="probe tool",
            handler=_echo,
            args_schema=_Text,
        )
    )
    fastapi_app.state.agent_service = AgentService(
        fastapi_app.state.hub, registry=reg, llm=_ScriptedLLM(*decisions)
    )


def _park(client, *, install: bool = True) -> str:
    """Message in -> agent asks a question -> run WAITING_USER."""
    if install:
        _install_service(_dec("ask_user", answer="请问要操作哪台电脑？"))
    res = client.post("/api/agent/message", json={"text": "帮我重新运行", "channel": "api"})
    assert res.status_code == 200, res.text
    run_id = res.json()["run_id"]
    assert wait_until(
        lambda: client.get(f"/api/agent/runs/{run_id}").json()["status"] == "WAITING_USER"
    )
    return run_id


def test_run_detail_includes_tool_call_count(client):
    run_id = _park(client)
    body = client.get(f"/api/agent/runs/{run_id}").json()
    assert body["tool_call_count"] == 0
    assert body["status"] == "WAITING_USER"


def test_resume_endpoint_completes_waiting_run(client):
    _install_service(
        _dec("ask_user", answer="请问要操作哪台电脑？"),
        _dec("tool_call", tool_name="probe", tool_args={"text": "办公室02"}),
        _dec("finish", answer="已在办公室02执行完成。"),
    )
    run_id = _park(client, install=False)

    res = client.post(f"/api/agent/runs/{run_id}/message", json={"text": "办公室02"})
    assert res.status_code == 200, res.text

    assert wait_until(
        lambda: client.get(f"/api/agent/runs/{run_id}").json()["status"] == "SUCCESS"
    )
    body = client.get(f"/api/agent/runs/{run_id}").json()
    assert body["final_reply"] == "已在办公室02执行完成。"
    assert body["tool_call_count"] == 1


def test_resume_rejects_run_not_waiting(client):
    _install_service(_dec("finish", answer="done"))
    res = client.post("/api/agent/message", json={"text": "直接完成", "channel": "api"})
    run_id = res.json()["run_id"]
    assert wait_until(
        lambda: client.get(f"/api/agent/runs/{run_id}").json()["status"] == "SUCCESS"
    )
    res = client.post(f"/api/agent/runs/{run_id}/message", json={"text": "继续"})
    assert res.status_code == 409


def test_resume_unknown_run_404(client):
    res = client.post("/api/agent/runs/does-not-exist/message", json={"text": "hi"})
    assert res.status_code == 404


def test_cancel_endpoint_closes_open_run(client):
    run_id = _park(client)
    res = client.post(f"/api/agent/runs/{run_id}/cancel")
    assert res.status_code == 200, res.text
    assert res.json()["status"] == "CANCELLED"
    assert res.json()["finished_at"] is not None

    # idempotent: cancelling again returns the same closed row
    res = client.post(f"/api/agent/runs/{run_id}/cancel")
    assert res.status_code == 200
    assert res.json()["status"] == "CANCELLED"


def test_cancel_unknown_run_404(client):
    res = client.post("/api/agent/runs/does-not-exist/cancel")
    assert res.status_code == 404


@pytest.mark.parametrize("payload", [{"text": ""}, {}])
def test_resume_validates_text(client, payload):
    run_id = _park(client)
    res = client.post(f"/api/agent/runs/{run_id}/message", json=payload)
    assert res.status_code == 422
