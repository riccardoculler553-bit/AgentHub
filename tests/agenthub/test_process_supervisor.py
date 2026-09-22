"""V1.7 ProcessSupervisor tests (§13/§14).

Real child processes: a long-running python script is launched, reported
RUNNING, then stopped -> STOPPED. The restart policy re-launches an
unexpectedly exited child (bounded by max_restarts) and the status callbacks
carry every transition.
"""

import asyncio
import sys
import uuid

import pytest

from worker.capability.manifest import Manifest
from worker.process import ProcessError, ProcessSupervisor


def _manifest(config: dict) -> Manifest:
    return Manifest(
        name="p.a.c", version="1.0.0", runtime="python", entrypoint="main", config=config
    )


class _FakeInstall:
    """Mimics CapabilityCache.Install without the dataclass import dance."""

    def __init__(self, package_dir, manifest) -> None:
        self.path = package_dir
        self.manifest = manifest
        self.name = manifest.name
        self.version = manifest.version
        self.checksum = "c0ffee"


class _FakeCapManager:
    def __init__(self, package_dir, manifest) -> None:
        self._install = _FakeInstall(package_dir, manifest)
        self.calls: list[tuple[str, str]] = []

    async def ensure(self, name, version, package_id=None, checksum=None):
        self.calls.append((name, version))
        return self._install


def _script(tmp_path, body: str) -> str:
    script = tmp_path / f"child_{uuid.uuid4().hex[:8]}.py"
    script.write_text(body, encoding="utf-8")
    return str(script)


def _supervisor(package_dir, manifest, statuses: list):
    manager = _FakeCapManager(package_dir, manifest)
    supervisor = ProcessSupervisor(manager, _collector(statuses))
    return supervisor, manager


def _collector(statuses: list):
    async def report(envelope: dict) -> None:
        statuses.append(envelope["data"])

    return report


@pytest.mark.anyio
async def test_supervisor_start_stop_lifecycle(tmp_path):
    statuses: list[dict] = []
    script = _script(tmp_path, "import time\nwhile True:\n    time.sleep(0.2)\n")
    manifest = _manifest({
        "entrypoint_command": script,
        "entrypoint": {"command": script, "args": []},
        "restart": {"policy": "never"},
    })
    supervisor, manager = _supervisor(tmp_path, manifest, statuses)

    brief = await supervisor.start({
        "process_id": "proc_1", "capability": "p.a.c", "version": "1.0.0",
        "package_id": "pkg_1", "checksum": None,
    })
    try:
        assert brief["status"] == "RUNNING" and brief["pid"]
        # at least STARTING+RUNNING reported
        assert [s["status"] for s in statuses][:2] == ["STARTING", "RUNNING"]
        stopped = await supervisor.stop("proc_1")
        assert stopped["status"] == "STOPPED"
        assert statuses[-1]["status"] == "STOPPED"
    finally:
        # hard cleanup in case stop failed
        inst = supervisor._instances.get("proc_1")
        if inst and inst.proc and inst.proc.poll() is None:
            inst.proc.kill()
    await asyncio.sleep(0.05)


@pytest.mark.anyio
async def test_supervisor_restart_policy_on_unexpected_exit(tmp_path):
    statuses: list[dict] = []
    # exits immediately -> on-failure policy relaunches (max_restarts=1, 0s backoff)
    script = _script(tmp_path, "import sys\nsys.exit(3)\n")
    manifest = _manifest({
        "entrypoint_command": script,
        "entrypoint": {"command": script, "args": []},
        "restart": {"policy": "on-failure", "max_restarts": 1, "backoff_sec": 0},
    })
    supervisor, _manager = _supervisor(tmp_path, manifest, statuses)

    await supervisor.start({
        "process_id": "proc_2", "capability": "p.a.c", "version": "1.0.0",
    })
    # monitor loop: exit detected -> STARTING(restart) -> FAILED after budget
    deadline = asyncio.get_event_loop().time() + 15
    while asyncio.get_event_loop().time() < deadline:
        if statuses and statuses[-1].get("status") == "FAILED":
            break
        await asyncio.sleep(0.2)
    final = statuses[-1]
    assert final["status"] == "FAILED"
    assert final["restart_count"] == 1  # bounded by max_restarts, no infinite loop
    assert "exited unexpectedly" in (final.get("error") or "")


@pytest.mark.anyio
async def test_supervisor_rejects_missing_entrypoint(tmp_path):
    statuses: list[dict] = []
    manifest = _manifest({"entrypoint_command": "no_such_script.py"})
    supervisor, _manager = _supervisor(tmp_path, manifest, statuses)
    with pytest.raises(ProcessError, match="INVALID_PACKAGE"):
        await supervisor.start({
            "process_id": "proc_3", "capability": "p.a.c", "version": "1.0.0",
        })
