"""LLM decision contract (PDF §21).

The LLM may only answer with one of three actions; it can never return SQL,
shell, paths, WebSocket commands or task status - anything that smells like
direct system manipulation has no field to live in. All outputs pass Schema
Validation + Tool Registry validation downstream (PDF §121).
"""

from typing import Literal

from pydantic import BaseModel, Field, model_validator


class AgentDecision(BaseModel):
    model_config = {"extra": "forbid"}

    action: Literal["tool_call", "ask_user", "finish"]
    tool_name: str | None = None
    tool_args: dict = Field(default_factory=dict)
    reason: str | None = None
    answer: str | None = None

    @model_validator(mode="after")
    def _tool_call_needs_name(self) -> "AgentDecision":
        if self.action == "tool_call" and not self.tool_name:
            raise ValueError("action=tool_call requires tool_name")
        return self

    def is_tool_call(self) -> bool:
        return self.action == "tool_call" and bool(self.tool_name)

    def is_ask_user(self) -> bool:
        return self.action == "ask_user"

    def is_finish(self) -> bool:
        return self.action == "finish"
