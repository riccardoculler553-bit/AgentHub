"""Worker persistent-process plane (V1.7 §8-§14).

ProcessSupervisor manages long-lived capability processes on a Worker:

    Windows Service -> AgentHub Worker -> ProcessSupervisor -> Python A/B/...

Lifecycle separation (§14): a supervised Python crashing is NOT the worker
dying; the worker dying is NOT the device disappearing. Instances are keyed
by process_id (the server's worker_processes row id) and reported back via
process.status envelopes through an injected async report callback.

The supervisor reuses the installed package (CapabilityManager.ensure) and
the V1.7 agenthub config (entrypoint.command / args / workspace /
environment.variables); restart policy comes from the config restart block.
"""

import asyncio
import logging
import os
import subprocess
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

from worker.capability.cache import work_root
from worker.capability.executors import _entrypoint_args
from worker.capability.manifest import config_entrypoint_command, load_package_config

logger = logging.getLogger(__name__)

PROCESS_POLL_INTERVAL = 2.0
_STOP_GRACE_SEC = 8.0
_DEFAULT_RESTART = {"policy": "never", "max_restarts": 5, "backoff_sec": 10}


class ProcessError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


@dataclass
class ProcessInstance:
    process_id: str
    capability: str
    version: str
    package_dir: Path
    manifest: object                      # merged Manifest
    python_exe: str
    restart: dict = field(default_factory=dict)
    proc: subprocess.Popen | None = None
    log_fh: object | None = None
    status: str = "STARTING"
    pid: int | None = None
    restart_count: int = 0
    last_error: str | None = None
    stopping: bool = False
    restart_at: float | None = None       # monotonic deadline for a pending auto-restart
    workspace: Path | None = None

    def brief(self) -> dict:
        return {
            "process_id": self.process_id,
            "capability": self.capability,
            "version": self.version,
            "status": self.status,
            "pid": self.pid,
            "restart_count": self.restart_count,
            "last_error": self.last_error,
        }


class ProcessSupervisor:
    def __init__(self, capability_manager, report) -> None:
        """capability_manager: CapabilityManager (package ensure); report:
        async callable receiving a protocol envelope dict (process.status)."""
        self.capability_manager = capability_manager
        self._report = report
        self._instances: dict[str, ProcessInstance] = {}
        self._monitor: asyncio.Task | None = None

    # ------------------------------------------------------------------ API

    async def start(self, data: dict) -> dict:
        """process.start handler: install (Lazy Pull) then launch."""
        process_id = str(data.get("process_id", ""))
        capability = str(data.get("capability", ""))
        version = str(data.get("version", ""))
        if not process_id or not capability or not version:
            raise ProcessError("INVALID_PARAMS", "process.start requires process_id/capability/version")
        if process_id in self._instances and self._instances[process_id].status in ("STARTING", "RUNNING"):
            return self._instances[process_id].brief()

        installed = await self.capability_manager.ensure(
            capability, version, str(data.get("package_id") or ""), data.get("checksum")
        )
        package_dir = Path(installed.path)
        try:
            manifest = load_package_config(package_dir, installed.manifest)
        except ValueError as exc:
            raise ProcessError("INVALID_PACKAGE", str(exc)) from exc

        config = manifest.config
        restart = dict(_DEFAULT_RESTART)
        restart.update(config.get("restart") or {})
        instance = ProcessInstance(
            process_id=process_id,
            capability=capability,
            version=version,
            package_dir=package_dir,
            manifest=manifest,
            python_exe=self._python_exe(capability, version, package_dir),
            restart=restart,
        )
        self._instances[process_id] = instance
        self._ensure_monitor()
        await self._report_status(instance)  # STARTING
        self._launch(instance)
        await self._report_status(instance)  # RUNNING
        return instance.brief()

    async def stop(self, process_id: str) -> dict:
        instance = self._instances.get(process_id)
        if instance is None or instance.proc is None or instance.proc.poll() is not None:
            if instance is not None:
                instance.status = "STOPPED"
                await self._report_status(instance)
                return instance.brief()
            return {"process_id": process_id, "status": "STOPPED"}
        instance.stopping = True
        instance.status = "STOPPING"
        await self._report_status(instance)
        await asyncio.to_thread(self._terminate, instance.proc)
        instance.status = "STOPPED"
        instance.stopped_at = time.time()
        await self._report_status(instance)
        return instance.brief()

    async def restart(self, process_id: str) -> dict:
        """Manual restart (§12): stop, then launch again on the same target."""
        instance = self._instances.get(process_id)
        if instance is None:
            raise ProcessError("PROCESS_NOT_FOUND", f"no running instance {process_id}")
        await self.stop(process_id)
        instance.stopping = False
        instance.status = "STARTING"
        self._launch(instance)
        await self._report_status(instance)
        return instance.brief()

    def status(self, process_id: str) -> dict | None:
        instance = self._instances.get(process_id)
        if instance is None:
            return None
        # refresh from the live child before answering
        if instance.proc is not None and instance.proc.poll() is not None and instance.status == "RUNNING":
            instance.status = "FAILED"
            instance.last_error = f"process exited with code {instance.proc.returncode}"
        return instance.brief()

    def all_statuses(self) -> list[dict]:
        return [self.status(i.process_id) or {} for i in list(self._instances.values())]

    def log_tail(self, process_id: str, tail_bytes: int = 8000) -> str:
        instance = self._instances.get(process_id)
        if instance is None or instance.workspace is None:
            return ""
        log_path = instance.workspace / "logs" / "process.log"
        if not log_path.is_file():
            return ""
        data = log_path.read_bytes()[-max(1, min(int(tail_bytes), 200_000)):]
        return data.decode("utf-8", errors="replace")

    # -------------------------------------------------------------- internal

    def _launch(self, instance: ProcessInstance) -> ProcessInstance:
        """Blocking-free launch: Popen runs on a thread (V1.5 lesson)."""
        config = instance.manifest.config
        workspace = work_root() / "processes" / instance.process_id
        input_dir = workspace / Path(str((config.get("workspace") or {}).get("input_dir") or "input")).name
        output_dir = workspace / Path(str((config.get("workspace") or {}).get("output_dir") or "output")).name
        log_dir = workspace / Path(str((config.get("workspace") or {}).get("log_dir") or "logs")).name
        for d in (input_dir, output_dir, log_dir):
            d.mkdir(parents=True, exist_ok=True)
        instance.workspace = workspace

        command = config_entrypoint_command(config)
        script = instance.package_dir / (command.replace("\\", "/") if command else "main.py")
        if not script.is_file():
            script = instance.package_dir / f"{instance.manifest.entrypoint or 'main'}.py"
        if not script.is_file():
            raise ProcessError("INVALID_PACKAGE", f"entrypoint script not found: {script}")

        placeholders = {
            "INPUT_DIR": str(input_dir),
            "OUTPUT_DIR": str(output_dir),
            "EXECUTION_DIR": str(workspace),
            "PACKAGE_DIR": str(instance.package_dir),
            "LOG_DIR": str(log_dir),
        }
        env = os.environ.copy()
        env.update(placeholders)
        env.update(PYTHONUTF8="1", PYTHONIOENCODING="utf-8")
        for key, value in ((config.get("environment") or {}).get("variables") or {}).items():
            env[str(key)] = str(value)

        log_path = log_dir / "process.log"
        log_fh = open(log_path, "ab")  # noqa: SIM115 - lifecycle-managed by the instance
        try:
            proc = subprocess.Popen(
                [instance.python_exe, str(script), *_entrypoint_args(instance.manifest, placeholders)],
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                cwd=str(instance.package_dir),
                env=env,
                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
            )
        except OSError as exc:
            log_fh.close()
            raise ProcessError("PROCESS_START_FAILED", str(exc)) from exc
        instance.proc = proc
        instance.pid = proc.pid
        instance.log_fh = log_fh
        instance.status = "RUNNING"
        instance.stopping = False
        instance.last_error = None
        instance.restart_at = None
        logger.info("process %s started (%s@%s, pid=%s)", instance.process_id, instance.capability, instance.version, proc.pid)
        return instance

    def _python_exe(self, capability: str, version: str, package_dir: Path) -> str:
        """The per-capability venv interpreter when it exists (same layout as
        the one-shot executor); sys.executable otherwise."""
        python_exe = work_root() / "envs" / capability / version / (
            "Scripts/python.exe" if os.name == "nt" else "bin/python"
        )
        if python_exe.is_file():
            return str(python_exe)
        if (package_dir / "requirements.txt").is_file():
            logger.warning(
                "capability %s@%s has requirements.txt but no venv yet; "
                "the first one-shot run (or service start) will create it",
                capability, version,
            )
        return sys.executable

    def _terminate(self, proc: subprocess.Popen) -> None:
        try:
            proc.terminate()
            try:
                proc.wait(timeout=_STOP_GRACE_SEC)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait(timeout=5)
        except OSError as exc:
            logger.warning("terminate failed for pid %s: %s", proc.pid, exc)

    def _ensure_monitor(self) -> None:
        if self._monitor is None or self._monitor.done():
            self._monitor = asyncio.create_task(self._monitor_loop())

    async def _monitor_loop(self) -> None:
        """Crash detection + restart policy (§13). Never dies on one bad tick."""
        while True:
            try:
                for instance in list(self._instances.values()):
                    proc = instance.proc
                    if proc is None:
                        continue
                    if proc.poll() is not None:
                        self._close_log(instance)
                        if instance.stopping:
                            continue  # stop() owns the terminal transition
                        await self._handle_unexpected_exit(instance, proc.returncode)
                    elif instance.status == "RUNNING":
                        pass
                    if instance.restart_at is not None and time.monotonic() >= instance.restart_at:
                        instance.restart_at = None
                        await self._auto_restart(instance)
            except Exception:  # noqa: BLE001 - supervisor must survive anything
                logger.exception("process supervisor tick failed")
            await asyncio.sleep(PROCESS_POLL_INTERVAL)

    async def _handle_unexpected_exit(self, instance: ProcessInstance, returncode: int | None) -> None:
        policy = (instance.restart or {}).get("policy", "never")
        max_restarts = int((instance.restart or {}).get("max_restarts", 0))
        backoff = float((instance.restart or {}).get("backoff_sec", 10))
        instance.last_error = f"process exited unexpectedly with code {returncode}"
        if policy in ("on-failure", "always") and instance.restart_count < max_restarts:
            instance.restart_count += 1
            instance.status = "STARTING"
            instance.restart_at = time.monotonic() + backoff
            logger.warning(
                "process %s exited (code %s); auto-restart #%s in %ss",
                instance.process_id, returncode, instance.restart_count, backoff,
            )
            await self._report_status(instance)
            return
        instance.status = "FAILED"
        logger.error("process %s FAILED: %s", instance.process_id, instance.last_error)
        await self._report_status(instance)

    async def _auto_restart(self, instance: ProcessInstance) -> None:
        try:
            self._launch(instance)
        except ProcessError as exc:
            instance.status = "FAILED"
            instance.last_error = exc.message
        await self._report_status(instance)

    def _close_log(self, instance: ProcessInstance) -> None:
        if instance.log_fh is not None:
            try:
                instance.log_fh.close()
            except OSError:
                pass
            instance.log_fh = None

    async def _report_status(self, instance: ProcessInstance) -> None:
        try:
            from worker.capability.executors import _entrypoint_args  # noqa: F401 - import sanity

            await self._report(
                {
                    "type": "process.status",
                    "data": {
                        "process_id": instance.process_id,
                        "status": instance.status,
                        "pid": instance.pid,
                        "restart_count": instance.restart_count,
                        "error": instance.last_error,
                    },
                }
            )
        except Exception:  # noqa: BLE001 - status reporting is best-effort
            logger.exception("process status report failed for %s", instance.process_id)
