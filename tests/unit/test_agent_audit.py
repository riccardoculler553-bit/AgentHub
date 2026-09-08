"""agent_tool_calls audit tests (PDF §122-§124).

Every call that walks the policy chain lands in the audit table when the
runner hands over a run_id: REJECTED for policy turn-aways, SUCCESS/FAILED
for executed tools. Without a run_id (bare policies, unit tests) nothing is
written, and audit failures never break the tool call itself.
"""

import pytest
from sqlalchemy import func, select

from _agentloop import ScriptedLLM, dec, probe_tool

from app.agent.core.policies import ToolPolicy
from app.agent.core.runner import AgentRunner
from app.agent.db_models import AgentToolCall
from app.agent.tools.base import AgentTool
from app.agent.tools.registry import ToolRegistry
from app.db.database import SessionLocal


def _calls(run_id: str):
    with SessionLocal() as db:
        rows = db.scalars(
            select(AgentToolCall).where(AgentToolCall.run_id == run_id).order_by(AgentToolCall.id)
        ).all()
        return [
            (r.tool_call_id, r.tool_name, r.status, r.error_code, r.arguments, r.result)
            for r in rows
        ]


def _total_calls() -> int:
    with SessionLocal() as db:
        return db.scalar(select(func.count()).select_from(AgentToolCall)) or 0


@pytest.mark.anyio
async def test_runner_audits_rejection_and_success():
    reg = ToolRegistry()
    reg.register(probe_tool())
    llm = ScriptedLLM(
        dec(action="tool_call", tool_name="ghost_tool"),  # policy REJECTED
        dec(action="tool_call", tool_name="probe", tool_args={"text": "x"}),
        dec(action="finish", answer="done"),
    )
    final = await AgentRunner(registry=reg, llm=llm).run("看看")

    rows = _calls(final["run_id"])
    assert [r[2] for r in rows] == ["REJECTED", "SUCCESS"]
    assert rows[0][3] == "TOOL_NOT_FOUND"
    assert rows[1][2] == "SUCCESS" and rows[1][3] is None
    assert "x" in rows[1][4]  # validated arguments recorded
    assert rows[1][5] and "echo" in rows[1][5]  # bounded result summary


@pytest.mark.anyio
async def test_runner_audits_failed_tool_and_confirmation():
    reg = ToolRegistry()
    reg.register(probe_tool(fail=("TASK_NOT_FOUND", "任务不存在")))

    async def unexecuted(db, args):  # noqa: ARG001 - never runs: confirmation gate first
        raise AssertionError("risky must not execute without confirmation")

    from pydantic import BaseModel, Field

    class Text(BaseModel):
        model_config = {"extra": "forbid"}
        text: str = Field(default="")

    reg.register(
        AgentTool(
            name="risky",
            description="needs confirmation",
            handler=unexecuted,
            args_schema=Text,
            requires_confirmation=True,
        )
    )
    llm = ScriptedLLM(
        dec(action="tool_call", tool_name="probe"),
        dec(action="tool_call", tool_name="risky", tool_args={"text": "a"}),
        dec(action="finish", answer="stopped"),
    )
    final = await AgentRunner(registry=reg, llm=llm).run("试试")

    rows = _calls(final["run_id"])
    assert [r[2] for r in rows] == ["FAILED", "REJECTED"]
    assert rows[0][3] == "TASK_NOT_FOUND"
    assert rows[1][3] == "CONFIRMATION_REQUIRED"


@pytest.mark.anyio
async def test_bare_policy_writes_no_audit_rows():
    reg = ToolRegistry()
    reg.register(probe_tool())
    before = _total_calls()
    policy = ToolPolicy(reg, max_total_calls=4)  # no run_id -> audit off
    ok = await policy.execute(None, tool_name="probe", args={"text": ""})
    ghost = await policy.execute(None, tool_name="ghost", args={})
    assert ok.success and ghost.error_code == "TOOL_NOT_FOUND"
    assert _total_calls() == before


@pytest.mark.anyio
async def test_service_and_run_share_one_identity():
    """AgentService pre-creates the run row; runner + audit reuse its id."""
    import asyncio

    from app.agent.runs import AgentRunService
    from app.agent.service import AgentService

    reg = ToolRegistry()
    reg.register(probe_tool())
    svc = AgentService(
        hub=None, registry=reg, llm=ScriptedLLM(dec(action="finish", answer="ok"))
    )
    run_id = svc.start_run(text="hello")

    async def _wait():
        for _ in range(200):
            with SessionLocal() as db:
                if AgentRunService(db).get(run_id).status == "SUCCESS":
                    return True
            await asyncio.sleep(0.02)
        return False

    assert await _wait()
    assert _calls(run_id) == []  # no tool executed -> no audit rows
    with SessionLocal() as db:
        assert AgentRunService(db).get(run_id).tool_call_count == 0
