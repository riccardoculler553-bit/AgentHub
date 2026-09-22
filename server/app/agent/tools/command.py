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
from app.agent.tools.context import current_run_id
from app.agent.tools.device import _resolve_device
from app.agent.tools.registry import ToolRegistry
from app.command.service import CommandDisabled, CommandError, CommandNotFound, CommandService
from app.core.config import settings
from app.db.database import SessionLocal
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
        # V1.6 P0 0.16: link Task -> AgentRun so the terminal notification can
        # find the conversation (same contract as run_capability), and STOP
        # holding the tool-loop as a ~1900s waiter: the tool returns right
        # after dispatch; the terminal fact reaches the user via the proactive
        # notification sink (agent/notify.py) or get_task_detail polling.
        run_id = current_run_id.get()
        if run_id:
            try:
                from app.agent.runs import AgentRunService

                AgentRunService(SessionLocal()).set_task(
                    run_id, task.task_id, ack_reply=f"已创建任务 {task.task_id}"
                )
            except Exception:  # noqa: BLE001 - notification link is best-effort
                pass
        with SessionLocal() as fresh_db:
            status = TaskService(fresh_db).get(task.task_id).status
        return ToolResult.ok(
            {
                "task_id": task.task_id,
                "command": args["command"],
                "device_name": device.name,
                "status": status,
                "note": "命令已下发；执行完成后会主动通知结果，期间可用 get_task_detail 查询进度。",
            }
        )

    registry.register(
        AgentTool(
            name="execute_command",
            description="在指定设备上执行一条已注册的业务命令（如 yingdao.audit）。命令必须存在于系统注册表，不接受任何脚本路径。立即返回 task_id，完成后主动推送结果。",
            handler=execute_command,
            args_schema=ExecuteCommandArgs,
            risk_level=RiskLevel.ACTION,
            requires_confirmation=settings.agent_confirm_actions,
            max_calls=2,
        )
    )
