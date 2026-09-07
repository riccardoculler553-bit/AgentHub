"""Main Agent graph (LangGraph): plan -> create -> dispatch -> wait -> evaluate.

Node rules (PDF Phase 12-13):
- Every node opens its own DB session; nothing is shared across awaits.
- The Agent never touches WebSockets directly: dispatch goes through
  TaskDispatcher, waiting is pure DB polling (TaskMonitor re-dispatches
  PENDING tasks when the device comes online).
- Decisions: finish | retry | replan. Retry re-dispatches the same task,
  replan rebuilds the plan from the original request plus the error.
"""

import asyncio
import logging
import time

from langgraph.graph import END, START, StateGraph
from sqlalchemy.orm import Session

from app.agent import tools
from app.agent.planner import PlanError, build_plan
from app.agent.state import AgentState
from app.core.config import settings
from app.db.database import SessionLocal
from app.task import models as schemas
from app.task.dispatcher import TaskDispatcher
from app.task.errors import TaskError
from app.task.service import TERMINAL_TASK_STATES, TaskService

logger = logging.getLogger(__name__)

MAX_REPLANS = 2


def _error(kind: str, message: str) -> dict:
    return {"kind": kind, "message": message}


def _final_result(service: TaskService, task_id: str) -> dict | None:
    for event in reversed(service.get_events(task_id)):
        if event.event_type in ("task.success", "task.failed", "task.timeout"):
            return {"event": event.event_type, "payload": event.payload}
    return None


def build_graph(hub):
    """Compile the Main Agent graph bound to the server's ConnectionHub."""

    dispatcher = TaskDispatcher(hub)

    # ------------------------------------------------------------- nodes

    async def gather_context(state: AgentState) -> dict:
        with SessionLocal() as db:
            from app.capability.service import CapabilityService

            context = {
                "devices": tools.list_devices(db),
                "commands": tools.list_commands(db),
                "capabilities": CapabilityService(db).get_all_grouped(),
            }
        return {"context": context, "message": "context gathered"}

    async def plan(state: AgentState) -> dict:
        error = state.get("error")
        with SessionLocal() as db:
            try:
                plan_ = build_plan(db, state["user_request"], state.get("context") or {}, error)
            except PlanError as exc:
                return {
                    "execution_plan": None,
                    "error": _error("plan_invalid", exc.message),
                    "message": f"plan failed: {exc.message}",
                }
        return {"execution_plan": plan_, "error": None, "message": plan_.get("rationale", "")}

    async def create_task(state: AgentState) -> dict:
        plan_ = state.get("execution_plan") or {}
        steps = plan_.get("steps") or []
        if not steps:
            return {"error": _error("plan_invalid", "plan has no steps")}
        with SessionLocal() as db:
            device_id, err = _resolve_device(db, plan_, steps[0]["command"])
            if err:
                return {"error": err}
            payload = schemas.TaskCreateIn(
                name=state["user_request"][:200],
                target_device_id=device_id,
                steps=[schemas.StepIn(command=s["command"], params=s.get("params") or {}) for s in steps],
            )
            try:
                task = TaskService(db).create(payload, created_by="main_agent")
            except TaskError as exc:
                return {"error": _error("validation", str(exc))}
        return {"task_id": task.task_id, "task_status": task.status, "error": None, "message": f"task {task.task_id} created"}

    async def dispatch(state: AgentState) -> dict:
        task_id = state["task_id"]
        sent = await dispatcher.dispatch_task(task_id)
        if sent:
            return {"message": f"task {task_id} dispatched"}
        # Device offline: stay PENDING, TaskMonitor re-dispatches when it
        # comes online (within task_offline_max_wait).
        return {"message": f"task {task_id} waits for device (offline), monitor will retry"}

    async def wait_result(state: AgentState) -> dict:
        task_id = state["task_id"]
        deadline = time.monotonic() + settings.agent_max_wait
        while True:
            with SessionLocal() as db:
                service = TaskService(db)
                try:
                    task = service.get(task_id)
                except TaskError:
                    return {"task_status": None, "error": _error("task_lost", f"task {task_id} disappeared")}
                status = task.status
                if status in TERMINAL_TASK_STATES:
                    return {
                        "task_status": status,
                        "task_result": _final_result(service, task_id),
                        "error": None,
                        "message": f"task {task_id} finished as {status}",
                    }
            if time.monotonic() > deadline:
                # Leave the task to TaskMonitor's lifecycle (it re-dispatches
                # PENDING and watchdog-times-out live tasks). The agent run
                # just stops waiting.
                return {
                    "task_status": status,
                    "error": _error("agent_wait_timeout", f"agent gave up waiting after {settings.agent_max_wait}s"),
                    "message": f"task {task_id} still {status}, agent wait timed out",
                }
            await asyncio.sleep(settings.agent_poll_interval)

    # Plan-quality failures worth one more planning round; everything else
    # (task_lost, retry_failed, agent_wait_timeout) ends the run.
    REPLAN_KINDS = {"plan_invalid", "validation", "capability_missing", "device_not_found"}

    async def evaluate(state: AgentState) -> dict:
        logger.debug(
            "evaluate: error=%s task_status=%s retry_count=%s replan_count=%s",
            state.get("error"), state.get("task_status"),
            state.get("retry_count"), state.get("replan_count"),
        )
        error = state.get("error")
        if error:
            if error.get("kind") in REPLAN_KINDS and state.get("replan_count", 0) < MAX_REPLANS:
                return {"decision": "replan"}
            return {"decision": "finish"}

        status = state.get("task_status")
        if status in ("FAILED", "TIMEOUT") and state.get("retry_count", 0) < settings.task_max_attempts:
            return {"decision": "retry"}
        return {"decision": "finish"}

    async def retry(state: AgentState) -> dict:
        task_id = state["task_id"]
        try:
            with SessionLocal() as db:
                TaskService(db).request_retry(task_id)
        except TaskError as exc:
            return {"error": _error("retry_failed", str(exc))}
        await dispatcher.dispatch_task(task_id)
        return {
            "retry_count": state.get("retry_count", 0) + 1,
            "error": None,
            "task_status": None,
            "task_result": None,
            "message": f"retry #{state.get('retry_count', 0) + 1} for task {task_id}",
        }

    def _after_retry(state: AgentState) -> str:
        # A failed request_retry must not fall through to wait_result, whose
        # fresh poll would erase the error and loop forever.
        return "wait_result" if state.get("task_id") and not state.get("error") else "evaluate"

    async def replan(state: AgentState) -> dict:
        return {
            "replan_count": state.get("replan_count", 0) + 1,
            "task_id": None,
            "task_status": None,
            "task_result": None,
            "message": f"replanning (#{state.get('replan_count', 0) + 1})",
        }

    async def finish(state: AgentState) -> dict:
        error = state.get("error")
        status = state.get("task_status")
        if error:
            message = f"failed: {error.get('kind')}: {error.get('message')}"
        elif status == "SUCCESS":
            message = f"task {state.get('task_id')} completed successfully"
        else:
            message = f"task {state.get('task_id')} ended as {status}"
        return {"message": message}

    # ------------------------------------------------------------- wiring

    def _has_plan(state: AgentState) -> str:
        return "create_task" if state.get("execution_plan") else "evaluate"

    def _has_task(state: AgentState) -> str:
        return "dispatch" if state.get("task_id") else "evaluate"

    def _route_decision(state: AgentState) -> str:
        decision = state.get("decision")
        if decision == "retry":
            return "retry"
        if decision == "replan":
            return "replan"
        return "finish"

    graph = StateGraph(AgentState)
    graph.add_node("gather_context", gather_context)
    graph.add_node("plan", plan)
    graph.add_node("create_task", create_task)
    graph.add_node("dispatch", dispatch)
    graph.add_node("wait_result", wait_result)
    graph.add_node("evaluate", evaluate)
    graph.add_node("retry", retry)
    graph.add_node("replan", replan)
    graph.add_node("finish", finish)

    graph.add_edge(START, "gather_context")
    graph.add_edge("gather_context", "plan")
    graph.add_conditional_edges("plan", _has_plan, {"create_task": "create_task", "evaluate": "evaluate"})
    graph.add_conditional_edges("create_task", _has_task, {"dispatch": "dispatch", "evaluate": "evaluate"})
    graph.add_edge("dispatch", "wait_result")
    graph.add_edge("wait_result", "evaluate")
    graph.add_conditional_edges("evaluate", _route_decision, {"retry": "retry", "replan": "replan", "finish": "finish"})
    graph.add_conditional_edges("retry", _after_retry, {"wait_result": "wait_result", "evaluate": "evaluate"})
    graph.add_edge("replan", "plan")
    graph.add_edge("finish", END)

    return graph.compile()


def _resolve_device(db: Session, plan: dict, first_command: str) -> tuple[str | None, dict | None]:
    """device_hint -> device_id. Prefers capable + online devices."""
    devices = tools.list_devices(db)
    capable_ids = {d["device_id"] for d in tools.search_devices_by_capability(db, first_command)}
    hint = (plan.get("device_hint") or "").strip()

    def _pick(candidates: list[dict]) -> str | None:
        online = [d for d in candidates if d["online"]]
        return (online or candidates)[0]["device_id"] if candidates else None

    if hint:
        exact = next((d for d in devices if d["device_id"].lower() == hint.lower()), None)
        pool = [d for d in devices if hint.lower() in str(d.get("name", "")).lower()]
        candidates = [exact] if exact else pool
        capable = [d for d in candidates if d["device_id"] in capable_ids]
        if capable:
            return _pick(capable), None
        if candidates:
            return None, _error("capability_missing", f"device '{hint}' lacks capability '{first_command}'")
        return None, _error("device_not_found", f"no device matches hint '{hint}'")

    capable = [d for d in devices if d["device_id"] in capable_ids]
    if capable:
        return _pick(capable), None
    return None, _error("capability_missing", f"no device reports capability '{first_command}'")
