"""Persistent-process Agent tools (V1.7 §52).

start_process / stop_process / restart_process / get_worker_processes mirror
the process API - the LLM NEVER computes placement or CPU weights (§53); the
device is chosen (or pinned) by the resolver, the supervisor runs the program.
"""

from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.agent.tools.base import AgentTool, EmptyArgs, RiskLevel, ToolErrorCodes, ToolResult
from app.agent.tools.device import _resolve_device
from app.agent.tools.registry import ToolRegistry


class StartProcessArgs(BaseModel):
    model_config = {"extra": "forbid"}

    capability: str = Field(min_length=1, description="Capability 名称，execution.mode 须为 service")
    device: str | None = Field(default=None, max_length=128, description="目标设备（名称或 id）；不填则由平台选择")
    version: str | None = Field(default=None, max_length=32, description="固定版本（semver）")


class ProcessIdArgs(BaseModel):
    model_config = {"extra": "forbid"}

    process_id: str = Field(min_length=1, description="ProcessInstance ID")


class ListProcessesArgs(BaseModel):
    model_config = {"extra": "forbid"}

    device: str | None = Field(default=None, max_length=128, description="按设备过滤")


def register_process_tools(registry: ToolRegistry, hub: Any = None) -> None:
    async def start_process(db: Session, args: dict) -> ToolResult:
        from app.api.processes import _process_out  # reuse the API contract
        from app.capability_runtime.errors import CapabilityError
        from app.capability_runtime.service import CapabilityService
        from app.websocket.protocol import Envelope, MessageType, new_message_id
        from app.worker.service import WorkerService

        device_id = None
        device_name = (args.get("device") or "").strip()
        if device_name:
            device, err = _resolve_device(db, device_name)
            if err is not None:
                return err
            device_id = device.device_id
        try:
            version = CapabilityService(db).get_published_version(args["capability"], args.get("version"))
        except CapabilityError as exc:
            return ToolResult.fail(exc.code, str(exc))
        if device_id is None:
            return ToolResult.fail(
                ToolErrorCodes.VALIDATION_FAILED,
                "service-mode processes need an explicit device; add device='设备名'",
            )
        row = WorkerService(db).create_process(
            device_id, version.capability_name, version.version,
            package_id=version.package_id, requested_by="tool_agent",
        )
        if hub is None:
            return ToolResult.fail(ToolErrorCodes.VALIDATION_FAILED, "hub unavailable")
        sent = await hub.send_to_device(
            device_id,
            Envelope(id=new_message_id("ps"), type=MessageType.PROCESS_START,
                     data={"process_id": row.process_id, "capability": row.capability,
                           "version": row.version, "package_id": version.package_id,
                           "checksum": version.checksum}),
        )
        if sent == 0:
            WorkerService(db).update_process_status(row.process_id, "FAILED", error="device offline at start")
            return ToolResult.fail(ToolErrorCodes.DEVICE_NOT_FOUND, "device has no live connection")
        return ToolResult.ok(_process_out(WorkerService(db).get_process(row.process_id)))

    async def stop_process(db: Session, args: dict) -> ToolResult:
        from app.api.processes import _process_out
        from app.websocket.protocol import Envelope, MessageType, new_message_id
        from app.worker.service import WorkerService

        row = WorkerService(db).get_process(args["process_id"])
        if row is None:
            return ToolResult.fail(ToolErrorCodes.TASK_NOT_FOUND, f"process not found: {args['process_id']}")
        WorkerService(db).update_process_status(row.process_id, "STOPPING")
        sent = await hub.send_to_device(
            row.device_id,
            Envelope(id=new_message_id("ps"), type=MessageType.PROCESS_STOP,
                     data={"process_id": row.process_id}),
        ) if hub else 0
        if not sent:
            return ToolResult.fail(ToolErrorCodes.DEVICE_NOT_FOUND, "device has no live connection")
        return ToolResult.ok(_process_out(WorkerService(db).get_process(row.process_id)))

    async def restart_process(db: Session, args: dict) -> ToolResult:
        from app.api.processes import _process_out
        from app.websocket.protocol import Envelope, MessageType, new_message_id
        from app.worker.service import WorkerService

        row = WorkerService(db).get_process(args["process_id"])
        if row is None:
            return ToolResult.fail(ToolErrorCodes.TASK_NOT_FOUND, f"process not found: {args['process_id']}")
        WorkerService(db).update_process_status(row.process_id, "STARTING")
        sent = await hub.send_to_device(
            row.device_id,
            Envelope(id=new_message_id("ps"), type=MessageType.PROCESS_RESTART,
                     data={"process_id": row.process_id}),
        ) if hub else 0
        if not sent:
            return ToolResult.fail(ToolErrorCodes.DEVICE_NOT_FOUND, "device has no live connection")
        return ToolResult.ok(_process_out(WorkerService(db).get_process(row.process_id)))

    async def get_worker_processes(db: Session, args: dict) -> ToolResult:
        from app.worker.service import WorkerService

        device_id = None
        if (args.get("device") or "").strip():
            device, err = _resolve_device(db, args["device"])
            if err is not None:
                return err
            device_id = device.device_id
        rows = WorkerService(db).list_processes(device_id=device_id)
        return ToolResult.ok({"processes": [_process_out(r) for r in rows]})

    registry.register(AgentTool(
        name="start_process",
        description="在一台设备的后台长期运行一个 service 模式能力（持久进程）。设备必填；不用于一次性任务。",
        handler=start_process,
        args_schema=StartProcessArgs,
        risk_level=RiskLevel.ACTION,
        requires_confirmation=True,
        max_calls=2,
    ))
    registry.register(AgentTool(
        name="stop_process",
        description="停止一个持久进程实例（STARTING/RUNNING 状态）。",
        handler=stop_process,
        args_schema=ProcessIdArgs,
        risk_level=RiskLevel.WRITE,
        max_calls=2,
    ))
    registry.register(AgentTool(
        name="restart_process",
        description="重启一个持久进程实例。",
        handler=restart_process,
        args_schema=ProcessIdArgs,
        risk_level=RiskLevel.WRITE,
        max_calls=2,
    ))
    registry.register(AgentTool(
        name="get_worker_processes",
        description="查询持久进程实例及其状态（可按设备过滤）。",
        handler=get_worker_processes,
        args_schema=ListProcessesArgs,
        risk_level=RiskLevel.READ,
    ))
