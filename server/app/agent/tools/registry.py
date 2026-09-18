"""ToolRegistry: the only tool catalogue the LLM can see (PDF §60-§63).

The registry holds AgentTool objects; business services, repositories and
ConnectionHub are never handed to the model directly. LLM-facing tool
descriptions come exclusively from registry.enabled_descriptions().
"""

import threading

from app.agent.tools.base import AgentTool


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: dict[str, AgentTool] = {}
        self._lock = threading.Lock()

    def register(self, tool: AgentTool, *, replace: bool = False) -> None:
        with self._lock:
            if tool.name in self._tools and not replace:
                raise ValueError(f"tool already registered: {tool.name}")
            self._tools[tool.name] = tool

    def get(self, name: str) -> AgentTool | None:
        return self._tools.get(name)

    def all(self) -> list[AgentTool]:
        return list(self._tools.values())

    def enabled(self) -> list[AgentTool]:
        """Only enabled tools exist for the LLM (PDF §63 whitelist)."""
        return [t for t in self._tools.values() if t.enabled]

    def enabled_descriptions(self) -> list[dict]:
        return [t.describe() for t in self.enabled()]

    def clear(self) -> None:
        with self._lock:
            self._tools.clear()


def build_default_registry(hub=None) -> ToolRegistry:
    """Fresh registry populated with the V1.2 standard tool set (PDF §26).

    Built per application (and per test) instead of a mutable module global,
    so tests never leak tools into each other. `hub` gives online-status
    tools and dispatching tools access to live connection state.
    """
    from app.agent.tools.artifact import register_artifact_tools
    from app.agent.tools.capability import register_capability_tools
    from app.agent.tools.command import register_command_tools
    from app.agent.tools.device import register_device_tools
    from app.agent.tools.task import register_task_tools
    from app.agent.tools.workflow import register_workflow_tools

    registry = ToolRegistry()
    register_device_tools(registry, hub)
    register_task_tools(registry, hub)
    register_command_tools(registry, hub)
    register_workflow_tools(registry, hub)
    register_capability_tools(registry, hub)
    register_artifact_tools(registry, hub)
    return registry
