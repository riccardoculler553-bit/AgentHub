"""Capability Agent tools (V1.4 §33/§36-§37/§61).

自然语言 -> Capability: list_capabilities / get_capability surface the
registry; run_capability goes Tool -> TaskService (CAPABILITY execution type)
-> Dispatcher -> CapabilityResolver -> Worker - never straight to workers or
packages (§72 boundary: the Task Engine owns Attempt/Retry/Timeout).
"""

from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agent.tools.base import AgentTool, EmptyArgs, RiskLevel, ToolResult
from app.agent.tools.registry import ToolRegistry
from app.capability_runtime.errors import CapabilityError
from app.capability_runtime.service import CapabilityService
from app.core.config import settings
from app.task.models import TaskCreateIn, StepIn


class CapabilityNameArgs(BaseModel):
    model_config = {"extra": "forbid"}

    capability: str = Field(
        min_length=1, max_length=128,
        description="Capability 名称（<domain>.<resource>.<action>），如 erp.order.export",
    )


class RunCapabilityArgs(BaseModel):
    model_config = {"extra": "forbid"}

    capability: str = Field(min_length=1, max_length=128, description="Capability 名称")
    version: str | None = Field(
        default=None, max_length=32,
        description="固定版本（semver）；不指定则使用当前 PUBLISHED 版本",
    )
    params: dict = Field(default_factory=dict, description="Capability 入参，与 manifest.inputs 对应")


def _capability_out(row) -> dict:
    return {
        "name": row.name,
        "display_name": row.display_name,
        "description": row.description,
        "runtime_type": row.runtime_type,
        "type": row.type,
        "enabled": row.enabled,
        "current_version": row.current_version,
        "risk_level": row.risk_level,
        "requires_confirmation": row.requires_confirmation,
    }


def _task_artifacts(db: Session, task_id: str) -> list[dict]:
    """Artifact index for the finished task (V1.4 §27/§29)."""
    from app.artifact.db_models import Artifact

    rows = db.scalars(
        select(Artifact).where(Artifact.task_id == task_id).order_by(Artifact.id)
    ).all()
    return [
        {"artifact_id": r.artifact_id, "name": r.name, "type": r.type, "size": r.size}
        for r in rows
    ]


def register_capability_tools(registry: ToolRegistry, hub: Any = None) -> None:
    async def list_capabilities(db: Session, args: dict) -> ToolResult:
        capabilities = [_capability_out(c) for c in CapabilityService(db).list_capabilities()]
        return ToolResult.ok({"capabilities": capabilities})

    async def get_capability(db: Session, args: dict) -> ToolResult:
        service = CapabilityService(db)
        try:
            capability = service.require_capability(args["capability"])
        except CapabilityError as exc:
            return ToolResult.fail(exc.code, str(exc))
        data = _capability_out(capability)
        data["versions"] = [
            {"version": v.version, "status": v.status, "created_at": v.created_at.isoformat() if v.created_at else None}
            for v in service.list_versions(capability.name)
        ]
        return ToolResult.ok(data)

    async def run_capability(db: Session, args: dict) -> ToolResult:
        from app.task.dispatcher import TaskDispatcher
        from app.task.errors import TaskError
        from app.task.service import TaskService

        try:
            task = TaskService(db).create(
                TaskCreateIn(
                    name=f"[Agent] {args['capability']}"[:200],
                    steps=[StepIn(command=args["capability"], params=args.get("params") or {})],
                    execution_type="CAPABILITY",
                    capability_version=args.get("version"),
                    source_type="AGENT",
                ),
                created_by="tool_agent",
            )
        except TaskError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        if hub is not None:
            await TaskDispatcher(hub).dispatch_task(task.task_id)
        # waits_task: reuse the task _wait_terminal helper (§44 internal wait).
        from app.agent.tools.task import _wait_terminal

        status, result = await _wait_terminal(task.task_id, settings.agent_tool_wait_max)
        from app.db.database import SessionLocal

        with SessionLocal() as fresh_db:
            artifacts = _task_artifacts(fresh_db, task.task_id)
        data = {
            "task_id": task.task_id,
            "capability": task.capability_name,
            "version": task.capability_version,
            "status": status,
            "artifacts": artifacts,
            "result": (result or {}).get("payload", {}).get("result") if (result or {}).get("event") == "task.success" else None,
        }
        if result and result.get("event") != "task.success":
            error = result.get("payload", {}).get("error") or {}
            data["error_code"] = error.get("code") or result.get("payload", {}).get("error_code")
            data["error_message"] = error.get("message") or result.get("payload", {}).get("error_message")
        return ToolResult.ok(data)

    registry.register(
        AgentTool(
            name="list_capabilities",
            description="列出平台注册的自动化能力（Capability）：名称、运行时、当前版本与风险等级。",
            handler=list_capabilities,
            args_schema=EmptyArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="get_capability",
            description="查看一个自动化能力的详情：入参约定（manifest.inputs）、可用版本列表与发布状态。",
            handler=get_capability,
            args_schema=CapabilityNameArgs,
            risk_level=RiskLevel.READ,
        )
    )
    registry.register(
        AgentTool(
            name="run_capability",
            description="执行一个自动化能力（由平台自动选择 Worker 并按需拉取程序包），等待执行结束返回结果与产物列表。需要用户确认。",
            handler=run_capability,
            args_schema=RunCapabilityArgs,
            risk_level=RiskLevel.ACTION,
            requires_confirmation=settings.agent_confirm_actions,
            max_calls=2,
            waits_task=True,
        )
    )
