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
from app.agent.mvp.schemas import COMMAND_LABELS
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
    device_name: str  # name as understood from the message (or default)
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
    return COMMAND_LABELS.get(command, command)


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
        device_name = intent.device_name.strip() or settings.agent_default_device_name
        return {"device_name": device_name, "command": intent.command}

    async def resolve(state: MvpState) -> dict:
        mentioned = state["device_name"]
        command = state["command"]
        with SessionLocal() as db:
            device = db.scalars(
                select(Device).where(Device.revoked_at.is_(None), Device.name.ilike(mentioned))
            ).first()
            if device is None:  # substring fallback for loose mentions
                candidates = (
                    db.execute(select(Device).where(Device.revoked_at.is_(None))).scalars().all()
                )
                device = next(
                    (d for d in candidates if mentioned.lower() in d.name.lower()), None
                )
            if device is None:
                return {"error": _error("device_not_found", mentioned)}
            display_name = device.name
            device_id = device.device_id
            has_cap = CapabilityService(db).has_capability(device_id, command)
            busy = db.scalars(
                select(Task.task_id).where(
                    Task.target_device_id == device_id,
                    Task.status.in_(LIVE_TASK_STATES),
                )
            ).first()

        # Realtime presence comes from the hub, not the (possibly stale) DB.
        online = hub.is_device_online(device_id)
        if not online:
            return {"error": _error("device_offline", display_name)}
        if not has_cap:
            return {"error": _error("capability_missing", display_name)}
        if busy is not None:
            return {"error": _error("device_busy", display_name)}
        ack = f"收到，正在启动{display_name}的{_biz(command)}任务。"
        return {"device_id": device_id, "display_name": display_name, "ack": ack}

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
                return {"reply": f"暂时只支持运行{_biz('yingdao.audit')}任务，例如：“运行办公室电脑02的审单”。"}
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
        if status == "SUCCESS":
            return {"reply": f"{name}的{biz}任务已完成。"}
        if status == "TIMEOUT":
            return {"reply": f"{name}的{biz}任务执行超时。"}
        if status == "CANCELLED":
            return {"reply": f"{name}的{biz}任务已取消。"}
        payload = (state.get("task_result") or {}).get("payload") or {}
        err_msg = str(payload.get("error_message") or "").strip()
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
