"""Unit tests for ToolRegistry (PDF Phase 2 / §142).

register / lookup / duplicate / disable / describe round-trips; no DB needed.
"""

import pytest
from pydantic import BaseModel

from app.agent.tools.base import AgentTool, RiskLevel, ToolResult
from app.agent.tools.registry import ToolRegistry


class HelloArgs(BaseModel):
    name: str


async def _hello(db, args: dict) -> ToolResult:
    return ToolResult.ok({"hello": args["name"]})


def _make_tool(name: str = "hello", *, enabled: bool = True, **kwargs) -> AgentTool:
    kwargs.setdefault("description", "says hello")
    return AgentTool(
        name=name,
        handler=_hello,
        args_schema=HelloArgs,
        enabled=enabled,
        **kwargs,
    )


def test_register_and_get():
    reg = ToolRegistry()
    tool = _make_tool()
    reg.register(tool)
    assert reg.get("hello") is tool
    assert reg.get("nope") is None
    assert reg.all() == [tool]


def test_duplicate_registration_rejected():
    reg = ToolRegistry()
    reg.register(_make_tool())
    with pytest.raises(ValueError, match="already registered"):
        reg.register(_make_tool())
    # replace=True is allowed for hot-reload scenarios
    reg.register(_make_tool(description="v2"), replace=True)
    assert reg.get("hello").description == "v2"


def test_enabled_whitelist_filters_disabled():
    reg = ToolRegistry()
    reg.register(_make_tool("on"))
    reg.register(_make_tool("off", enabled=False))
    assert [t.name for t in reg.enabled()] == ["on"]
    # disabled tools are invisible to the LLM but still resolvable by name
    assert reg.get("off") is not None
    assert [d["name"] for d in reg.enabled_descriptions()] == ["on"]


def test_describe_exposes_contract_fields():
    tool = AgentTool(
        name="retry_task",
        description="重试任务",
        handler=_hello,
        args_schema=HelloArgs,
        risk_level=RiskLevel.WRITE,
        requires_confirmation=True,
        max_calls=1,
    )
    d = tool.describe()
    assert d["name"] == "retry_task"
    assert d["risk_level"] == "WRITE"
    assert d["requires_confirmation"] is True
    assert "properties" in d["args"]


def test_clear():
    reg = ToolRegistry()
    reg.register(_make_tool())
    reg.clear()
    assert reg.all() == []
