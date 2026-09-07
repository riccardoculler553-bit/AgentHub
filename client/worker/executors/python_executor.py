"""PythonExecutor: run pre-registered local scripts via subprocess.

Script lookup is convention-based (no paths from the Agent, ever):
    command "python.demo"   -> worker/scripts/python_demo.py
    command "excel.merge"   -> worker/scripts/excel_merge.py

The script receives the task params as `--params-json <json>` on argv and
reports its result as a JSON object printed to stdout. Non-zero exit codes
map to structured ExecutionError codes.
"""

import asyncio
import json
import re
import subprocess
import sys
from pathlib import Path

from worker.executor import ExecutionError, Executor

_SCRIPT_NAME = re.compile(r"^[A-Za-z0-9_]+\.(py)$")
SCRIPTS_DIR = Path(__file__).resolve().parent.parent / "scripts"
POLL_INTERVAL = 0.2


class PythonExecutor(Executor):
    name = "python"

    def validate(self, params: dict) -> None:
        if not isinstance(params, dict):
            raise ValueError("params must be an object")

    def _script_path(self, command: str) -> Path:
        script_name = command.replace(".", "_") + ".py"
        if not _SCRIPT_NAME.match(script_name):
            raise ExecutionError("INVALID_PARAMS", f"illegal command name: {command}")
        path = (SCRIPTS_DIR / script_name).resolve()
        if SCRIPTS_DIR.resolve() not in path.parents or not path.is_file():
            # Not pre-registered on this worker - hard stop, never guess paths.
            raise ExecutionError(
                "EXECUTOR_SCRIPT_NOT_FOUND",
                f"no pre-registered script for command {command} (expected {script_name})",
            )
        return path

    async def execute(self, params: dict, config: dict, progress, cancel: asyncio.Event) -> dict:
        command = config.get("__command__", "")
        script = self._script_path(command)
        await progress(5, f"starting {script.name}")

        argv = [sys.executable, str(script), "--params-json", json.dumps(params, ensure_ascii=False)]
        try:
            proc = subprocess.Popen(
                argv,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                cwd=str(SCRIPTS_DIR),
            )
        except OSError as exc:
            raise ExecutionError("EXECUTOR_START_FAILED", str(exc)) from exc

        try:
            stdout, stderr = "", ""
            while True:
                if cancel.is_set():
                    proc.terminate()
                    try:
                        proc.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        proc.kill()
                    raise ExecutionError("EXECUTOR_CANCELLED", "cancelled by server")
                try:
                    stdout, stderr = proc.communicate(timeout=POLL_INTERVAL)
                    break
                except subprocess.TimeoutExpired:
                    continue
        except ExecutionError:
            raise
        except Exception as exc:  # unexpected failure around the process
            proc.kill()
            raise ExecutionError("EXECUTOR_FAILED", str(exc)) from exc

        await progress(90, "collecting result")
        if proc.returncode != 0:
            raise ExecutionError("EXECUTOR_FAILED", (stderr or stdout or "")[-800:] or f"exit {proc.returncode}")

        return self._parse_result(stdout)

    @staticmethod
    def _parse_result(stdout: str) -> dict:
        lines = [line.strip() for line in (stdout or "").splitlines() if line.strip()]
        for line in reversed(lines):
            if line.startswith("{") and line.endswith("}"):
                try:
                    parsed = json.loads(line)
                    if isinstance(parsed, dict):
                        return parsed
                except ValueError:
                    continue
        return {"output": (stdout or "")[-2000:]}
