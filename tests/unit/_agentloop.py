"""Shared in-memory fixtures for agent loop / resume tests.

Kept importable (tests/unit is a flat pytest root) so test_agent_loop and
test_agent_resume script the same probe tool + FakeLLM pair.
"""

from pydantic import BaseModel, Field

from app.agent.llm.schemas import AgentDecision
from app.agent.tools.base import AgentTool, ToolResult


class ProbeArgs(BaseModel):
    model_config = {"extra": "forbid"}

    text: str = Field(default="")


def probe_tool(name: str = "probe", *, fail: tuple[str, str] | None = None) -> AgentTool:
    async def handler(db, args):  # noqa: ARG001 - db unused in unit probes
        if fail is not None:
            return ToolResult.fail(fail[0], fail[1])
        return ToolResult.ok({"echo": args.get("text", "")})

    return AgentTool(
        name=name,
        description="probe tool for loop tests",
        handler=handler,
        args_schema=ProbeArgs,
    )


class ScriptedLLM:
    """Pops one AgentDecision (or raises) per decide() call; records prompts."""

    def __init__(self, *decisions) -> None:
        self.decisions = list(decisions)
        self.prompts: list[list] = []

    async def decide(self, messages):
        self.prompts.append(messages)
        item = self.decisions.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def dec(**kwargs) -> AgentDecision:
    return AgentDecision(**kwargs)
