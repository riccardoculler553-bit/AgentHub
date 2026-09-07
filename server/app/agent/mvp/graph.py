"""MVP Main Agent graph (PDF §20/§59):

    START -> analyze -> resolve -> create_task -> dispatch -> wait_result
          -> build_reply -> END

Any node that sets `error` short-circuits to build_reply, which turns it into
a friendly Chinese reply (PDF §26-§28/§84). Every node opens its own DB
session; nothing is shared across awaits. The agent never touches WebSockets:
dispatch goes through TaskDispatcher, waiting uses TaskWaiters events with DB
polling as fallback.
"""

import asyncio
import logging
import time
from typing import TypedDict

from langgraph.graph import END, START, StateGraph
from sqlalchemy import select

from app.agent.mvp.analyzer import analyze_with_llm
from app.agent.mvp.tools import command_label, tool_registry
from app.capability.service import CapabilityService
from app.core.config import settings
from app.db.database import SessionLocal
from app.db.models import Device
from app.task import models as schemas
from app.task.db_models import Task
from app.task.dispatcher import TaskDispatcher
from app.task.service import LIVE_TASK_STATES, TERMINAL_TASK_STATES, TaskService
from app.task.waiters import task_waiters

logger = logging.getLogger(__name__)


class MvpState(TypedDict, total=False):
    # request context
    text: str
    # analyze output
    device_name: str  # device mentioned in the message ("" = none)
    command: str
    # resolve output
    device_id: str | None
    display_name: str  # canonical device.name for user-facing replies
    ack: str
    # task lifecycle
    task_id: str | None
    task_status: str | None
    task_result: dict | None
    # {"kind": ..., "message": ...} - short-circuits to build_reply
    error: dict | None
    # final user-facing reply
    reply: str


def _error(kind: str, message: str = "") -> dict:
    return {"kind": kind, "message": message}


def _biz(command: str) -> str:
    return command_label(command)


def build_mvp_graph(hub) -> "object":  # compiled StateGraph
    dispatcher = TaskDispatcher(hub)

    # ------------------------------------------------------------- nodes

    async def analyze(state: MvpState) -> dict:
        with SessionLocal() as db:
            device_names = [
                name for (name,) in db.execute(select(Device.name).where(Device.revoked_at.is_(None))).all()
            ]
        intent = await analyze_with_llm(state["text"], device_names)
        if intent is None or intent.intent != "run_command":
            return {"error": _error("unsupported")}
        # No default injection: when nothing is mentioned the routing layer
        # (whitelist / auto-discovery) decides which device runs the job.
        return {"device_name": intent.device_name.strip(), "command": intent.command}

    async def resolve(state: MvpState) -> dict:
        """Device routing (the "which computer can run what" logic).

        Candidate order: device mentioned in the message -> configured
        whitelist (tool.devices, priority order) -> auto-discovery of every
        device reporting the capability (only when no whitelist). First
        candidate that is registered + capable + online + not busy wins;
        otherwise we keep scanning (busy jump-over), and the error is chosen
        by priority: busy > offline > capability > not found.
        """
        mentioned = state["device_name"].strip()
        command = state["command"]
        tool = tool_registry.by_command(command)

        with SessionLocal() as db:
            devices = db.execute(
                select(Device).where(Device.revoked_at.is_(None))
            ).scalars().all()
            by_name = {d.name.lower(): d for d in devices}
            capability = CapabilityService(db)
            busy_by_device = {
                row[0]
                for row in db.execute(
                    select(Task.target_device_id).where(
                        Task.status.in_(LIVE_TASK_STATES),
                        Task.target_device_id.isnot(None),
                    )
                ).all()
            }

        candidates: list[str] = []
        if mentioned:
            candidates.append(mentioned)
        if tool is not None:
            candidates.extend(tool.devices)
            if not tool.devices:
                # no whitelist configured: auto-discover capable devices
                candidates.extend(
                    d.name for d in devices if capability.has_capability(d.device_id, command)
                )
        seen: set[str] = set()
        ordered = [c for c in candidates if not (c.lower() in seen or seen.add(c.lower()))]

        errors: dict[str, dict] = {}
        priority = ("device_busy", "device_offline", "capability_missing")
        for name in ordered:
            device = by_name.get(name.lower())
            if device is None:
                continue
            # offline wins over capability: a powered-off machine tells the
            # user nothing about what it supports
            if not hub.is_device_online(device.device_id):
                errors.setdefault("device_offline", _error("device_offline", device.name))
                continue
            if not capability.has_capability(device.device_id, command):
                errors.setdefault("capability_missing", _error("capability_missing", device.name))
                continue
            if device.device_id in busy_by_device:
                errors.setdefault("device_busy", _error("device_busy", device.name))
                continue
            ack = f"收到，正在启动{device.name}的{_biz(command)}任务。"
            return {
                "device_id": device.device_id,
                "display_name": device.name,
                "ack": ack,
            }

        if errors:
            for kind in priority:
                if kind in errors:
                    return {"error": errors[kind]}
        mention = mentioned or (tool.devices[0] if tool is not None and tool.devices else "")
        return {"error": _error("device_not_found", mention)}

    async def create_task(state: MvpState) -> dict:
        with SessionLocal() as db:
            payload = schemas.TaskCreateIn(
                name=f"MVP: {state['display_name']}的{_biz(state['command'])}"[:200],
                target_device_id=state["device_id"],
                steps=[schemas.StepIn(command=state["command"], params={})],
            )
            try:
                task = TaskService(db).create(payload, created_by="mvp_agent")
            except TaskError as exc:
                return {"error": _error("validation", str(exc))}
        return {"task_id": task.task_id, "task_status": task.status}

    async def dispatch(state: MvpState) -> dict:
        task_id = state["task_id"]
        sent = await dispatcher.dispatch_task(task_id)
        if sent:
            return {}
        # Re-read the task: the dispatcher may have failed it (DEVICE_BUSY
        # race) or left it PENDING (device dropped between check and send -
        # TaskMonitor re-dispatches while within max wait).
        with SessionLocal() as db:
            try:
                status = db.scalars(select(Task.status).where(Task.task_id == task_id)).first()
            except TaskError:
                status = None
        if status == "FAILED":
            return {"error": _error("device_busy", state.get("display_name", ""))}
        return {}  # stay in wait_result; monitor may still complete it

    async def wait_result(state: MvpState) -> dict:
        task_id = state["task_id"]
        event = task_waiters.register(task_id)
        deadline = time.monotonic() + settings.agent_run_max_wait
        try:
            while True:
                with SessionLocal() as db:
                    status = db.scalars(select(Task.status).where(Task.task_id == task_id)).first()
                if status in TERMINAL_TASK_STATES:
                    with SessionLocal() as db:
                        service = TaskService(db)
                        result = None
                        for evt in reversed(service.get_events(task_id)):
                            if evt.event_type in ("task.success", "task.failed", "task.timeout"):
                                result = {"event": evt.event_type, "payload": evt.payload}
                                break
                    return {
                        "task_status": status,
                        "task_result": result,
                        "error": None,
                    }
                if time.monotonic() > deadline:
                    return {
                        "error": _error(
                            "agent_wait_timeout",
                            f"no result after {settings.agent_run_max_wait}s",
                        )
                    }
                try:
                    await asyncio.wait_for(asyncio.shield(event.wait()), timeout=settings.agent_poll_interval * 5)
                except asyncio.TimeoutError:
                    continue
        finally:
            task_waiters.unregister(task_id, event)

    async def build_reply(state: MvpState) -> dict:
        error = state.get("error")
        name = state.get("display_name") or state.get("device_name") or "目标设备"
        biz = _biz(state.get("command", "yingdao.audit"))
        if error:
            kind = error.get("kind")
            if kind == "unsupported":
                labels = "、".join(t.label for t in tool_registry.tools) or "审单"
                return {"reply": f"暂时只支持{labels}任务，例如：“运行办公室电脑02的{tool_registry.tools[0].label}”。"}
            if kind == "device_not_found":
                mention = error.get("message") or name
                return {"reply": f"没有找到“{mention}”，请检查设备名称。"}
            if kind == "device_offline":
                return {"reply": f"{name}当前处于离线状态，暂时无法执行{biz}任务。"}
            if kind == "capability_missing":
                return {"reply": f"{name}不支持“{biz}”功能。"}
            if kind == "device_busy":
                return {"reply": f"{name}当前正在执行其他任务，请稍后再试。"}
            if kind == "agent_wait_timeout":
                return {"reply": f"{name}的{biz}任务长时间未返回结果，请稍后在控制台查看任务状态。"}
            return {"reply": f"{name}的{biz}任务执行失败：{error.get('message', '未知错误')}"[:500]}

        status = state.get("task_status")
        payload = (state.get("task_result") or {}).get("payload") or {}
        nested = payload.get("error") or {}
        err_code = str(payload.get("error_code") or nested.get("code") or "")
        if status == "SUCCESS":
            return {"reply": f"{name}的{biz}任务已完成。"}
        if status == "TIMEOUT":
            return {"reply": f"{name}的{biz}任务执行超时。"}
        if status == "CANCELLED":
            return {"reply": f"{name}的{biz}任务已取消。"}
        if err_code == "EXECUTOR_BUSY":
            # 影刀进程/日志显示该电脑正在跑别的任务 (参考 dingtalk-xbot-audit)
            return {"reply": f"{name}当前正在运行其他程序，请稍后再试。"}
        if err_code in ("EXECUTOR_LAUNCH_FAILED", "EXECUTOR_START_FAILED"):
            return {"reply": f"{name}的{biz}任务启动失败，请检查该电脑上的影刀配置。"}
        err_msg = str(payload.get("error_message") or nested.get("message") or "").strip()
        suffix = f"\n错误：{err_msg}" if err_msg else ""
        return {"reply": f"{name}的{biz}任务执行失败。{suffix}"[:500]}

    # ------------------------------------------------------------- wiring

    def _or_build_reply(next_node: str):
        def route(state: MvpState) -> str:
            return "build_reply" if state.get("error") else next_node

        return route

    graph = StateGraph(MvpState)
    graph.add_node("analyze", analyze)
    graph.add_node("resolve", resolve)
    graph.add_node("create_task", create_task)
    graph.add_node("dispatch", dispatch)
    graph.add_node("wait_result", wait_result)
    graph.add_node("build_reply", build_reply)

    graph.add_edge(START, "analyze")
    graph.add_conditional_edges("analyze", _or_build_reply("resolve"), {"resolve": "resolve", "build_reply": "build_reply"})
    graph.add_conditional_edges("resolve", _or_build_reply("create_task"), {"create_task": "create_task", "build_reply": "build_reply"})
    graph.add_conditional_edges("create_task", _or_build_reply("dispatch"), {"dispatch": "dispatch", "build_reply": "build_reply"})
    graph.add_edge("dispatch", "wait_result")
    graph.add_edge("wait_result", "build_reply")
    graph.add_edge("build_reply", END)

    return graph.compile()
