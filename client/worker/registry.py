"""Worker-side Executor Registry.

Server Command Registry defines what the system allows; this registry defines
what THIS worker can actually do (PDF §18/§144). A command is dispatchable only
when both sides agree:

    Command Registry (server, system allows)  AND  Executor Registry (worker, I can)
    = executable

Executor instances are resolved per declared command via its executor_type in
the local capabilities.json.
"""

import json
from pathlib import Path

from worker.executors.echo import EchoExecutor
from worker.executors.python_executor import PythonExecutor
from worker.executors.yingdao import YingdaoExecutor

# executor_type (declared locally) -> executor implementation
EXECUTOR_TYPES = {
    "echo": EchoExecutor,
    "python": PythonExecutor,
    "yingdao": YingdaoExecutor,
}

CAPABILITIES_FILE = Path(__file__).resolve().parent / "capabilities.json"


def load_capability_config() -> list[dict]:
    try:
        data = json.loads(CAPABILITIES_FILE.read_text(encoding="utf-8"))
        return data.get("capabilities", [])
    except (OSError, ValueError):
        return []


def build_command_registry() -> dict:
    """{command_name: executor_instance} - the Worker Executor Registry.
    Each instance receives its capabilities.json config block (robot_uuid,
    shadowbot_path, ...)."""
    registry: dict = {}
    for item in load_capability_config():
        executor_type = item.get("executor_type")
        name = str(item.get("name", "")).strip()
        if name and executor_type in EXECUTOR_TYPES and name not in registry:
            executor = EXECUTOR_TYPES[executor_type]()
            executor.configure(item)
            registry[name] = executor
    return registry


def reportable_capabilities() -> list[dict]:
    """Capabilities advertised to the server: declared locally AND backed by a
    real executor implementation."""
    registry = build_command_registry()
    return [
        {"name": item["name"], "version": item.get("version", "1.0")}
        for item in load_capability_config()
        if str(item.get("name", "")).strip() in registry
    ]
