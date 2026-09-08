"""Command tool: execute_command (PDF §35/§64-§65).

The highest-risk tool. It accepts ONLY a command name registered in the
Command Registry plus a device name and schema-valid params - never
script_path / shell / python_code / exe_path (PDF §119). Agent Tool ≠
Command: this tool goes Tool -> TaskService -> TaskDispatcher, the same four
fold validation as any other task creation.
"""

from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.agent.tools.base import AgentTool, RiskLevel, ToolErrorCodes, ToolResult
from app.agent.tools.device import _live_busy_device_ids, _resolve_device
from app.agent.tools.registry import ToolRegistry
from app.agent.tools.task import _wait_terminal
from app.command.service import CommandDisabled, CommandError, CommandNotFound, CommandService
from app.core.config import settings
from app.task import models as schemas
from app.task.dispatcher import TaskDispatcher
from app.task.errors import TaskError
from app.task.service import TaskService


class ExecuteCommandArgs(BaseModel):
    model_config = {"extra": "forbid"}

    command: str = Field(min_length=1, description="系统注册表中的命令名，如 yingdao.audit")
    device_name: str = Field(min_length=1, description="目标设备名称")
    params: dict = Field(default_factory=dict, description="命令参数，必须符合命令注册表的 params_schema")


def register_command_tools(registry: ToolRegistry, hub: Any = None) -> None:
    async def execute_command(db: Session, args: dict) -> ToolResult:
        device, err = _resolve_device(db, args["device_name"])
        if err:
            return err
        # busy pre-check gives the LLM a clean DEVICE_BUSY instead of a
        # doomed task (doc §86: observe -> replan, never a dead loop)
        if device.device_id in _live_busy_device_ids(db):
            return ToolResult.fail(
                ToolErrorCodes.DEVICE_BUSY,
                f"device '{device.name}' is running another task",
            )
        # pre-validate against the command registry so the LLM sees the
        # canonical codes (COMMAND_NOT_FOUND / INVALID_ARGS), not a wrapped
        # task validation error (PDF §119: no script paths, ever)
        try:
            command = CommandService(db).require_executable(args["command"])
            CommandService(db).validate_params(command, args.get("params") or {})
        except CommandError as exc:
            code = ToolErrorCodes.INVALID_ARGS
            if isinstance(exc, (CommandNotFound, CommandDisabled)):
                code = ToolErrorCodes.COMMAND_NOT_FOUND
            return ToolResult.fail(code, str(exc))
        try:
            task = TaskService(db).create(
                schemas.TaskCreateIn(
                    name=f"Agent: {device.name} {args['command']}"[:200],
                    target_device_id=device.device_id,
                    steps=[schemas.StepIn(command=args["command"], params=args.get("params") or {})],
                ),
                created_by="tool_agent",
            )
        except TaskError as exc:
            return ToolResult.fail(ToolErrorCodes.VALIDATION_FAILED, str(exc))
        # dispatch immediately; offline -> PENDING -> TaskMonitor re-dispatches
        if hub is not None:
            await TaskDispatcher(hub).dispatch_task(task.task_id)
        status, result = await _wait_terminal(task.task_id, settings.agent_tool_wait_max)
        return ToolResult.ok(
            {"task_id": task.task_id, "status": status, "device_name": device.name, "result": result}
        )

    registry.register(
        AgentTool(
            name="execute_command",
            description="在指定设备上执行一条已注册的业务命令（如 yingdao.audit）。命令必须存在于系统注册表，不接受任何脚本路径。",
            handler=execute_command,
            args_schema=ExecuteCommandArgs,
            risk_level=RiskLevel.ACTION,
            requires_confirmation=settings.agent_confirm_actions,
            max_calls=2,
            waits_task=True,
        )
    )
