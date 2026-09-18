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
from app.agent.tools.context import current_run_id
from app.agent.tools.registry import ToolRegistry
from app.capability_runtime.errors import CapabilityError
from app.capability_runtime.service import CapabilityService
from app.core.config import settings
from app.db.database import SessionLocal
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
    device: str | None = Field(
        default=None, max_length=128,
        description="目标设备（设备名称或 device_id，如 办公室电脑02）；不指定则由平台自动选择 Worker",
    )
    # V1.5 §11/§15: manifest input name -> input artifact ids（数据经 Artifact
    # 平面传输，Task 只保存引用；如 {"data_dir": ["art_x"], "mapping_dir": ["art_y"]}）
    inputs: dict[str, list[str]] = Field(
        default_factory=dict,
        description="输入 Artifact 引用：manifest.inputs 中的输入名 -> artifact_id 列表",
    )
    # V1.5: 大任务超时覆盖（秒）。大数据处理任务建议显式给大值，如 7200。
    timeout_seconds: int | None = Field(
        default=None, ge=60, le=86400,
        description="任务超时秒数（60~86400）；不填用平台默认 1800。大文件任务建议 7200",
    )
    params: dict = Field(default_factory=dict, description="Capability 其他入参，与 manifest.inputs 对应")
    # Phase 4: 默认异步——创建任务立即返回 CONFIGURED，终态由通知层主动推送；
    # wait=True 保留旧的同步等待语义（等待上限 agent_tool_wait_max）。
    wait: bool = Field(
        default=False,
        description="是否同步等待任务终态。默认 False：立即返回 task_id（CONFIGURED），完成后主动通知",
    )


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
        from app.task.models import InputArtifactIn
        from app.task.service import TaskService

        # V1.5 §11: resolve the device NAME the user speaks into a real
        # device_id - worker names never leak into the execution layer.
        target_device_id = None
        device_name = (args.get("device") or "").strip()
        if device_name:
            from app.agent.tools.device import _resolve_device

            device, err = _resolve_device(db, device_name)
            if err is not None:
                return err
            target_device_id = device.device_id

        # V1.5 §15: artifact references (role = manifest input name).
        input_artifacts = [
            InputArtifactIn(artifact_id=artifact_id, role=role)
            for role, ids in (args.get("inputs") or {}).items()
            for artifact_id in (ids or [])
        ]

        try:
            task = TaskService(db).create(
                TaskCreateIn(
                    name=f"[Agent] {args['capability']}"[:200],
                    steps=[StepIn(command=args["capability"], params=args.get("params") or {})],
                    execution_type="CAPABILITY",
                    capability_version=args.get("version"),
                    target_device_id=target_device_id,
                    input_artifacts=input_artifacts,
                    timeout_seconds=args.get("timeout_seconds"),
                    source_type="AGENT",
                ),
                created_by="tool_agent",
            )
        except TaskError as exc:
            return ToolResult.fail(exc.code.upper(), str(exc))
        if hub is not None:
            await TaskDispatcher(hub).dispatch_task(task.task_id)
        # Phase 3: link Task -> AgentRun so the terminal notification can find
        # the conversation even after this run has finished (best-effort).
        run_id = current_run_id.get()
        if run_id:
            try:
                from app.agent.runs import AgentRunService

                AgentRunService(SessionLocal()).set_task(
                    run_id, task.task_id, ack_reply=f"已创建任务 {task.task_id}"
                )
            except Exception:  # noqa: BLE001 - notification link is best-effort
                pass
        # Phase 4: default async - the Tool Loop must not pay for long waits.
        if not args.get("wait"):
            return ToolResult.ok(
                {
                    "task_id": task.task_id,
                    "capability": task.capability_name,
                    "version": task.capability_version,
                    "status": "CONFIGURED",
                    "note": "任务已配置并派发；执行完成后会主动通知结果与产物清单，"
                            "期间可用 get_task_detail 查询进度。",
                }
            )
        # waits_task: reuse the task _wait_terminal helper (§44 internal wait).
        from app.agent.tools.task import _wait_terminal

        status, result = await _wait_terminal(task.task_id, settings.agent_tool_wait_max)
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
            description="执行一个自动化能力（由平台自动选择 Worker 并按需拉取程序包）。"
                        "默认立即返回 task_id（状态 CONFIGURED），任务完成后主动推送结果与产物；"
                        "需要同步拿到结果时传 wait=true（等待上限受平台限制）。需要用户确认。",
            handler=run_capability,
            args_schema=RunCapabilityArgs,
            risk_level=RiskLevel.ACTION,
            requires_confirmation=settings.agent_confirm_actions,
            max_calls=2,
            waits_task=True,
        )
    )
