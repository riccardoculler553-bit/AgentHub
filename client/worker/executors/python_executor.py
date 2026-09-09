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
        # Output goes to temp files, not pipes: polling a piped child with
        # repeated communicate(timeout=...) is broken on Windows (returns
        # early with a stale returncode) and large output can deadlock the
        # child on a full pipe buffer.
        import tempfile

        out_f = tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
        err_f = tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
        try:
            try:
                proc = subprocess.Popen(
                    argv,
                    stdout=out_f,
                    stderr=err_f,
                    text=True,
                    encoding="utf-8",
                    cwd=str(SCRIPTS_DIR),
                )
            except OSError as exc:
                raise ExecutionError("EXECUTOR_START_FAILED", str(exc)) from exc

            def _wait_child() -> str:
                # Runs in a worker thread: a synchronous wait loop on the
                # event loop thread would starve cancel sources (WS reads,
                # cancel tasks) and block heartbeats for the whole execution.
                while True:
                    if cancel.is_set():
                        return "cancelled"
                    try:
                        proc.wait(timeout=POLL_INTERVAL)
                        return "exited"
                    except subprocess.TimeoutExpired:
                        continue

            try:
                verdict = await asyncio.to_thread(_wait_child)
            except ExecutionError:
                raise
            except Exception as exc:  # unexpected failure around the process
                proc.kill()
                raise ExecutionError("EXECUTOR_FAILED", str(exc)) from exc
            if verdict == "cancelled":
                proc.terminate()
                try:
                    proc.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    proc.kill()
                raise ExecutionError("EXECUTOR_CANCELLED", "cancelled by server")

            await progress(90, "collecting result")
            out_f.seek(0)
            err_f.seek(0)
            stdout, stderr = out_f.read(), err_f.read()
        finally:
            out_f.close()
            err_f.close()

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
