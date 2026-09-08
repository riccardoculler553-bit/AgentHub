"""Workflow Agent tools (V1.3 §68-§73/§115-§117).

Agent decides WHICH workflow to run; the engine decides the steps (§205).
run_workflow goes Tool -> WorkflowService -> WorkflowEngine -> TaskService -
never straight to Tasks (§72) and never into step-level LLM decisions (§77).
"""

import asyncio
import logging
import time

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agent.tools.base import AgentTool, EmptyArgs, RiskLevel, ToolErrorCodes, ToolResult
from app.agent.tools.registry import ToolRegistry
from app.core.config import settings
from app.db.database import SessionLocal
from app.workflow.db_models import WorkflowRun
from app.workflow.errors import WorkflowError
from app.workflow.service import WorkflowService
from app.workflow.state import WORKFLOW_TERMINAL_STATES
from app.workflow.waiters import workflow_waiters

logger = logging.getLogger(__name__)


class WorkflowNameArgs(BaseModel):
    model_config = {"extra": "forbid"}

    workflow: str = Field(min_length=1, description="Workflow 名称，如 amazon_daily_report")
    version: str | None = Field(default=None, description="版本；不指定则用 active 版本")


class RunWorkflowArgs(BaseModel):
    model_config = {"extra": "forbid"}

    workflow: str = Field(min_length=1, description="Workflow 名称")
    version: str | None = Field(default=None, description="版本；不指定则用 active 版本")
    variables: dict = Field(default_factory=dict, description="Workflow 变量，如 {\"date\": \"2026-09-08\"}")


class RunIdArgs(BaseModel):
    model_config = {"extra": "forbid"}

    run_id: str = Field(min_length=1)


# ------------------------------------------------------------------- helpers

def _run_summary(service: WorkflowService, run) -> dict:
    return service.run_out(run).model_dump()


async def _wait_run_terminal(run_id: str, timeout: float) -> str | None:
    """Wait for a run to reach a terminal state (§117/§119): waiter event +
    DB polling fallback, mirroring the task _wait_terminal helper."""
    event = workflow_waiters.register(run_id)
    deadline = time.monotonic() + timeout
    try:
        while True:
            with SessionLocal() as db:
                status = db.scalars(
                    select(WorkflowRun.status).where(WorkflowRun.run_id == run_id)
                ).first()
            if status in WORKFLOW_TERMINAL_STATES:
                return status
            if time.monotonic() > deadline:
                return status  # still RUNNING/PENDING: report, don't lie
            try:
                await asyncio.wait_for(
                    asyncio.shield(event.wait()), timeout=settings.agent_poll_interval * 5
                )
            except asyncio.TimeoutError:
                continue
    finally:
        workflow_waiters.unregister(run_id, event)


# ------------------------------------------------------------------- register

def register_workflow_tools(registry: ToolRegistry, hub=None) -> None:
    async def list_workflows(db: Session, args: dict) -> ToolResult:
        service = WorkflowService(db)
        workflows = [
            {
                "workflow_id": w.workflow_id,
                "name": w.name,
                "version": w.version,
                "status": w.status,
                "enabled": w.status == "ENABLED",
                "description": w.description,
                "requires_confirmation": w.requires_confirmation,
            }
            for w in service.list_workflows()
        ]
        return ToolResult.ok({"workflows": workflows})

    async def get_workflow(db: Session, args: dict) -> ToolResult:
        service = WorkflowService(db)
        try:
            workflow = service.find_workflow(args["workflow"], args.get("version"))
        except WorkflowError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        return ToolResult.ok(service.workflow_out(workflow).model_dump())

    async def run_workflow(db: Session, args: dict) -> ToolResult:
        service = WorkflowService(db)
        try:
            run, dispatch_ids = service.create_run(
                args["workflow"],
                version=args.get("version"),
                variables=args.get("variables") or {},
                trigger_type="agent",
                created_by="tool_agent",
            )
        except WorkflowError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        for task_id in dispatch_ids:
            if hub is not None:
                from app.task.dispatcher import TaskDispatcher

                await TaskDispatcher(hub).dispatch_task(task_id)
        # §117: wait for the workflow terminal state inside the tool call;
        # long workflows report RUNNING and the LLM answers accordingly.
        status = await _wait_run_terminal(run.run_id, settings.agent_tool_wait_max)
        with SessionLocal() as fresh_db:
            summary = _run_summary(WorkflowService(fresh_db), service.get_run(run.run_id))
        data = {
            "workflow_run_id": run.run_id,
            "workflow": run.workflow_name,
            "version": run.workflow_version,
            "status": summary["status"],
            "steps": [
                {"name": s["name"], "status": s["status"], "task_id": s["task_id"],
                 "result": s["result"], "error_code": s["error_code"]}
                for s in summary["steps"]
            ],
        }
        if summary["status"] == "FAILED":
            data["error_code"] = summary["error_code"]
            data["error_message"] = summary["error_message"]
        return ToolResult.ok(data)

    async def get_workflow_run(db: Session, args: dict) -> ToolResult:
        service = WorkflowService(db)
        try:
            run = service.get_run(args["run_id"])
        except WorkflowError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        return ToolResult.ok(_run_summary(service, run))

    async def cancel_workflow_run(db: Session, args: dict) -> ToolResult:
        service = WorkflowService(db)
        try:
            outcome = service.cancel_run(args["run_id"])
        except WorkflowError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        task_id = outcome.get("notify_task_id")
        if task_id and hub is not None:
            try:
                from app.task.monitor import TaskMonitor

                await TaskMonitor(hub).notify_cancel(task_id)
            except Exception:  # noqa: BLE001 - notify is best-effort
                logger.exception("workflow cancel notify failed for task %s", task_id)
        return ToolResult.ok({"workflow_run_id": args["run_id"], "status": "CANCELLED"})

    registry.register(
        AgentTool(
            name="list_workflows",
            description="列出系统已注册的业务流程（Workflow）及其版本与启用状态。",
            handler=list_workflows,
            args_schema=EmptyArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="get_workflow",
            description="查看一个 Workflow 的定义：包含哪些步骤、每步的命令与参数来源。",
            handler=get_workflow,
            args_schema=WorkflowNameArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="run_workflow",
            description="启动一个已启用的业务流程（多步骤串行执行）。需要用户确认。返回流程最终状态与各步骤结果。",
            handler=run_workflow,
            args_schema=RunWorkflowArgs,
            risk_level=RiskLevel.ACTION,
            requires_confirmation=settings.agent_confirm_actions,
            max_calls=2,
            waits_task=True,
        )
    )
    registry.register(
        AgentTool(
            name="get_workflow_run",
            description="查询一次 Workflow 运行的进度：当前步骤、各步骤状态与结果。",
            handler=get_workflow_run,
            args_schema=RunIdArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="cancel_workflow_run",
            description="取消一次正在运行的 Workflow（当前步骤的 Task 一并取消）。需要用户确认。",
            handler=cancel_workflow_run,
            args_schema=RunIdArgs,
            risk_level=RiskLevel.WRITE,
            requires_confirmation=True,
            max_calls=2,
            waits_task=True,
        )
    )
