"""Unit tests for the ToolPolicy admission chain (PDF Phase 2 / §143 / §25).

Chain order: exists -> enabled -> args -> permission -> confirmation -> limits.
"""

import pytest
from pydantic import BaseModel

from app.agent.core.policies import ToolPolicy
from app.agent.tools.base import AgentTool, EmptyArgs, RiskLevel, ToolErrorCodes, ToolResult
from app.agent.tools.registry import ToolRegistry


class EchoArgs(BaseModel):
    model_config = {"extra": "forbid"}

    text: str


async def _echo(db, args: dict) -> ToolResult:
    return ToolResult.ok({"echo": args["text"]})


async def _noop(db, args: dict) -> ToolResult:
    return ToolResult.ok({})


def _registry() -> ToolRegistry:
    reg = ToolRegistry()
    reg.register(
        AgentTool(
            name="get_task_detail",
            description="query task",
            handler=_echo,
            args_schema=EchoArgs,
            risk_level=RiskLevel.READ,
        )
    )
    reg.register(
        AgentTool(
            name="retry_task",
            description="retry",
            handler=_echo,
            args_schema=EchoArgs,
            risk_level=RiskLevel.WRITE,
            requires_confirmation=True,
            max_calls=1,
        )
    )
    reg.register(
        AgentTool(
            name="disabled_tool",
            description="off",
            handler=_echo,
            args_schema=EchoArgs,
            enabled=False,
        )
    )
    return reg


# ------------------------------------------------------------------ chain gates

@pytest.mark.anyio
async def test_unknown_tool_rejected():
    policy = ToolPolicy(_registry())
    r = await policy.execute(None, tool_name="nope", args={})
    assert not r.success and r.error_code == ToolErrorCodes.TOOL_NOT_FOUND


@pytest.mark.anyio
async def test_disabled_tool_is_not_found_for_llm():
    policy = ToolPolicy(_registry())
    r = await policy.execute(None, tool_name="disabled_tool", args={"text": "x"})
    assert r.error_code == ToolErrorCodes.TOOL_NOT_FOUND


@pytest.mark.anyio
async def test_invalid_args_rejected():
    policy = ToolPolicy(_registry())
    r = await policy.execute(None, tool_name="get_task_detail", args={"wrong": 1})
    assert r.error_code == ToolErrorCodes.INVALID_ARGS


@pytest.mark.anyio
async def test_extra_field_smuggling_rejected():
    """PDF §150: LLM adds script_path to execute_command -> schema refuses."""
    policy = ToolPolicy(_registry())
    r = await policy.execute(
        None, tool_name="get_task_detail", args={"text": "x", "script_path": "D:\\evil.py"}
    )
    assert r.error_code == ToolErrorCodes.INVALID_ARGS
    assert "script_path" in (r.message or "")


@pytest.mark.anyio
async def test_permission_denied():
    async def _deny(tool, args):
        return False

    policy = ToolPolicy(_registry(), permission_fn=_deny)
    r = await policy.execute(None, tool_name="get_task_detail", args={"text": "x"})
    assert r.error_code == ToolErrorCodes.PERMISSION_DENIED


@pytest.mark.anyio
async def test_confirmation_required_blocks_unconfirmed_write():
    policy = ToolPolicy(_registry())
    r = await policy.execute(None, tool_name="retry_task", args={"text": "t1"})
    assert r.error_code == ToolErrorCodes.CONFIRMATION_REQUIRED
    assert r.data == {"pending_args": {"text": "t1"}}


@pytest.mark.anyio
async def test_confirmed_write_executes_and_counts():
    policy = ToolPolicy(_registry())
    r = await policy.execute(None, tool_name="retry_task", args={"text": "t1"}, confirmed=True)
    assert r.success and r.data == {"echo": "t1"}
    assert r.tool_call_id == "call_001"
    assert policy.calls_of("retry_task") == 1


# --------------------------------------------------------------------- limits

@pytest.mark.anyio
async def test_per_tool_max_calls_enforced():
    reg = ToolRegistry()
    reg.register(
        AgentTool(name="get_device_status", description="d", handler=_noop, args_schema=EmptyArgs, max_calls=2)
    )
    policy = ToolPolicy(reg)
    for _ in range(2):
        r = await policy.execute(None, tool_name="get_device_status", args={})
        assert r.success
    r = await policy.execute(None, tool_name="get_device_status", args={})
    assert r.error_code == ToolErrorCodes.MAX_CALLS_EXCEEDED
    assert "get_device_status" in r.message


@pytest.mark.anyio
async def test_run_total_max_calls_enforced():
    reg = ToolRegistry()
    reg.register(AgentTool(name="t1", description="d", handler=_noop, args_schema=EmptyArgs))
    reg.register(AgentTool(name="t2", description="d", handler=_noop, args_schema=EmptyArgs))
    policy = ToolPolicy(reg, max_total_calls=1)
    assert (await policy.execute(None, tool_name="t1", args={})).success
    r = await policy.execute(None, tool_name="t2", args={})
    assert r.error_code == ToolErrorCodes.MAX_CALLS_EXCEEDED


@pytest.mark.anyio
async def test_confirmation_does_not_consume_quota_but_execution_does():
    policy = ToolPolicy(_registry())
    # unconfirmed attempt: rejected, quota untouched
    r1 = await policy.execute(None, tool_name="retry_task", args={"text": "t1"})
    assert r1.error_code == ToolErrorCodes.CONFIRMATION_REQUIRED
    assert policy.total_calls == 0
    # confirmed execution consumes the single retry_task quota
    r2 = await policy.execute(None, tool_name="retry_task", args={"text": "t1"}, confirmed=True)
    assert r2.success
    r3 = await policy.execute(None, tool_name="retry_task", args={"text": "t1"}, confirmed=True)
    assert r3.error_code == ToolErrorCodes.MAX_CALLS_EXCEEDED
    # duplicate-call protection (PDF §88): policy layer refuses; Task Engine
    # idempotency stays the second line of defence.


@pytest.mark.anyio
async def test_call_ids_are_sequential():
    policy = ToolPolicy(_registry())
    assert policy.next_call_id() == "call_001"
    assert policy.next_call_id() == "call_002"


@pytest.mark.anyio
async def test_handler_crash_becomes_internal_error_not_exception():
    async def _boom(db, args):
        raise RuntimeError("db exploded")

    reg = ToolRegistry()
    reg.register(AgentTool(name="boom", description="d", handler=_boom, args_schema=EmptyArgs))
    policy = ToolPolicy(reg)
    r = await policy.execute(None, tool_name="boom", args={})
    assert not r.success and r.error_code == ToolErrorCodes.INTERNAL_ERROR
    assert "db exploded" in r.message
