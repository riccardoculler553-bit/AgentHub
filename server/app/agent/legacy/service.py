"""AgentService: run entry point for the Main Agent graph."""

import logging

from app.agent.legacy.graph import build_graph
from app.agent.legacy.state import AgentState

logger = logging.getLogger(__name__)

_RESULT_KEYS = (
    "request",
    "execution_plan",
    "task_id",
    "task_status",
    "task_result",
    "error",
    "decision",
    "message",
    "retry_count",
    "replan_count",
)


class AgentService:
    def __init__(self, hub) -> None:
        self.graph = build_graph(hub)

    async def run(self, request: str) -> dict:
        initial: AgentState = {
            "user_request": request,
            "message": "",
            "retry_count": 0,
            "replan_count": 0,
        }
        # Bounded by node-level waits; extra headroom for LLM planning calls.
        config = {"recursion_limit": 60}
        final: AgentState = dict(initial)
        async for event in self.graph.astream(initial, config=config):
            for node_name, update in event.items():
                if isinstance(update, dict):
                    final.update(update)
                logger.info("agent node %s done", node_name)
        logger.info(
            "agent run finished: decision=%s task=%s status=%s",
            final.get("decision"), final.get("task_id"), final.get("task_status"),
        )
        result = {key: final.get(key) for key in _RESULT_KEYS}
        result["request"] = request
        return result
