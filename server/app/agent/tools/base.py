"""Tool contract: every Agent-visible tool speaks ToolResult (PDF §36-§37).

Tool errors must never crash the Agent graph: a missing task is
ToolResult.fail("TASK_NOT_FOUND", ...), which flows into observe/evaluate so
the LLM can decide to ask the user, replan, or finish (PDF §37).
"""

import inspect
from enum import Enum
from typing import Any, Awaitable, Callable

from pydantic import BaseModel, ValidationError

from sqlalchemy.orm import Session


class RiskLevel(str, Enum):
    """Tool risk tiers (PDF §24): READ < WRITE < ACTION."""

    READ = "READ"      # query only: devices, tasks, events
    WRITE = "WRITE"    # changes business state: retry_task, cancel_task
    ACTION = "ACTION"  # executes real business work: execute_command


class ToolResult(BaseModel):
    """Standard tool outcome, always carrying its call id (PDF §103)."""

    success: bool
    data: dict[str, Any] | list[Any] | None = None
    error_code: str | None = None
    message: str | None = None
    tool_call_id: str | None = None
    tool_name: str | None = None

    @classmethod
    def ok(
        cls, data: Any = None, *, tool_call_id: str | None = None, tool_name: str | None = None
    ) -> "ToolResult":
        return cls(
            success=True,
            data=data,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
        )

    @classmethod
    def fail(
        cls,
        error_code: str,
        message: str = "",
        *,
        tool_call_id: str | None = None,
        tool_name: str | None = None,
        data: Any = None,
    ) -> "ToolResult":
        return cls(
            success=False,
            data=data,
            error_code=error_code,
            message=message,
            tool_call_id=tool_call_id,
            tool_name=tool_name,
        )

    def with_call(self, tool_call_id: str, tool_name: str) -> "ToolResult":
        """Stamp call identity (used by the executor after policy admission)."""
        self.tool_call_id = tool_call_id
        self.tool_name = tool_name
        return self


class ToolErrorCodes:
    """Canonical tool-layer error codes (LLM reasons on these, PDF §37/§86)."""

    TOOL_NOT_FOUND = "TOOL_NOT_FOUND"
    TOOL_DISABLED = "TOOL_DISABLED"
    INVALID_ARGS = "INVALID_ARGS"
    PERMISSION_DENIED = "PERMISSION_DENIED"
    CONFIRMATION_REQUIRED = "CONFIRMATION_REQUIRED"
    MAX_CALLS_EXCEEDED = "MAX_CALLS_EXCEEDED"
    DEVICE_NOT_FOUND = "DEVICE_NOT_FOUND"
    DEVICE_OFFLINE = "DEVICE_OFFLINE"
    DEVICE_BUSY = "DEVICE_BUSY"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    INVALID_TASK_STATE = "INVALID_TASK_STATE"
    COMMAND_NOT_FOUND = "COMMAND_NOT_FOUND"
    CAPABILITY_MISSING = "CAPABILITY_MISSING"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    INTERNAL_ERROR = "INTERNAL_ERROR"


# Tool handlers receive the validated args dict and one fresh DB session;
# async handlers only (the graph runs in an event loop).
ToolHandler = Callable[[Session, dict], Awaitable[ToolResult]]


class EmptyArgs(BaseModel):
    """Args schema for parameter-less tools; extra keys are rejected (PDF §150)."""

    model_config = {"extra": "forbid"}


class AgentTool:
    """One Agent-callable business capability (PDF §23/§62).

    The tool - not the LLM - owns the truth about what arguments it accepts:
    args_schema is a strict Pydantic model (extra="forbid") so a hallucinated
    "script_path" has nowhere to land. Execution always goes through the
    service layer; a tool never touches SQL or WebSockets itself (PDF §5).
    """

    def __init__(
        self,
        *,
        name: str,
        description: str,
        handler: ToolHandler,
        args_schema: type[BaseModel],
        risk_level: RiskLevel = RiskLevel.READ,
        requires_confirmation: bool = False,
        max_calls: int = 8,
        enabled: bool = True,
        waits_task: bool = False,
    ) -> None:
        self.name = name
        self.description = description
        self.handler = handler
        self.args_schema = args_schema
        self.risk_level = risk_level
        self.requires_confirmation = requires_confirmation
        self.max_calls = max_calls
        self.enabled = enabled
        # waits_task=True: the executor waits for the spawned task to reach a
        # terminal state inside the tool call (PDF §44-§45) instead of
        # letting the LLM poll get_task_detail in a loop.
        self.waits_task = waits_task

    # ---------------------------------------------------------------- schema

    def args_model(self) -> dict:
        """JSON-schema-ish description of the args, fed to the LLM prompt."""
        schema = self.args_schema.model_json_schema()
        schema.pop("title", None)
        return schema

    def describe(self) -> dict:
        return {
            "name": self.name,
            "description": self.description,
            "args": self.args_model(),
            "risk_level": self.risk_level.value,
            "requires_confirmation": self.requires_confirmation,
        }

    # ------------------------------------------------------------- validation

    def validate_args(self, args: dict | None) -> dict:
        """Strict validation; raises ValueError with a safe message on any
        mismatch - including unknown fields (parameter smuggling, PDF §150)."""
        try:
            model = self.args_schema.model_validate(args or {})
        except ValidationError as exc:
            first = exc.errors()[0]
            loc = ".".join(str(p) for p in first.get("loc", ())) or "args"
            if first.get("type") == "extra_forbidden":
                raise ValueError(f"unknown argument '{loc}' is not accepted by {self.name}") from exc
            raise ValueError(f"invalid argument '{loc}': {first.get('msg', 'validation failed')}") from exc
        return model.model_dump()

    # --------------------------------------------------------------- execute

    async def run(self, db: Session, args: dict | None) -> ToolResult:
        """Execute with fresh-args validation; unexpected handler exceptions
        are converted to INTERNAL_ERROR instead of crashing the graph (§37)."""
        try:
            validated = self.validate_args(args)
            result = await self.handler(db, validated)
        except ValueError as exc:
            return ToolResult.fail(ToolErrorCodes.INVALID_ARGS, str(exc))
        except Exception as exc:  # noqa: BLE001 - tool boundary (PDF §37)
            return ToolResult.fail(ToolErrorCodes.INTERNAL_ERROR, f"{type(exc).__name__}: {exc}")
        if not isinstance(result, ToolResult):  # defensive: broken handler
            return ToolResult.fail(ToolErrorCodes.INTERNAL_ERROR, "tool returned a non-ToolResult")
        return result

    @staticmethod
    def is_async(handler: object) -> bool:
        return inspect.iscoroutinefunction(handler)
