"""Capability Runtime executors (V1.4 §12-§16).

One executor per runtime_type, resolved from the package manifest - the
TaskManager never branches on runtime. Base contract (§12):

    validate(manifest, params)        raise ValueError
    execute(context, progress, cancel) -> CapabilityResult (normalized §44)
    cancel(execution_id)              best-effort

Security (§15/§16): URLs come from the package manifest.config only - never
from task params; there is no arbitrary shell/local executor in V1.4.
"""

import asyncio
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from worker.capability.context import ExecutionContext
from worker.capability.manifest import Manifest
from worker.capability.result import CapabilityResult
from worker.executors.yingdao import YingdaoExecutor

_POLL_INTERVAL = 0.2
_ENTRY_SAFE = re.compile(r"^[A-Za-z0-9_]+$")
_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


class CapabilityExecutor:
    runtime_type = "base"

    def validate(self, manifest: Manifest, params: dict) -> None:
        """Structural validation before execute. Raises ValueError."""
        _validate_required_inputs(manifest, params)

    async def execute(
        self,
        context: ExecutionContext,
        progress,
        cancel: asyncio.Event,
    ) -> CapabilityResult:
        raise NotImplementedError

    async def cancel(self, execution_id: str) -> None:  # pragma: no cover - default no-op
        return None


def _validate_required_inputs(manifest: Manifest, params: dict) -> None:
    if not isinstance(params, dict):
        raise ValueError("params must be an object")
    for name, spec in (manifest.inputs or {}).items():
        if isinstance(spec, dict) and spec.get("required") and name not in params:
            raise ValueError(f"missing required param: {name}")


# --------------------------------------------------------------------- python


class PythonCapabilityExecutor(CapabilityExecutor):
    """Runs the package entrypoint script (V1.4 §14).

    Package contract for python capabilities:
      - entrypoint <name> maps to <package_dir>/<name>.py
      - process cwd = package dir (resources load relative); env carries
        CAPABILITY_PARAMS / CAPABILITY_CONTEXT / CAPABILITY_EXECUTION_DIR
      - outputs go into the execution dir; the script reports its result as a
        JSON object printed to stdout OR written to <execution_dir>/result.json
      - result.json "artifacts": [{"path": "orders.xlsx", "name": ...,
        "type": "file"}] - paths relative to the execution dir
    """

    runtime_type = "python"

    def validate(self, manifest: Manifest, params: dict) -> None:
        # Structural checks only - the package dir is not known here; the
        # entrypoint file is verified in execute() where the context exists.
        super().validate(manifest, params)
        if not _ENTRY_SAFE.match(manifest.entrypoint or ""):
            raise ValueError(f"illegal entrypoint: {manifest.entrypoint!r}")

    @staticmethod
    def _script_path(manifest: Manifest) -> Path:
        return Path(str(manifest.entrypoint or "main") + ".py")

    async def execute(self, context: ExecutionContext, progress, cancel: asyncio.Event) -> CapabilityResult:
        package_dir = Path(context.package_dir)
        script = package_dir / self._script_path(context.manifest)
        if not script.is_file():
            return CapabilityResult.fail(
                "INVALID_PACKAGE", f"entrypoint script not found in package: {script.name}"
            )
        exec_dir = context.execution_dir()
        # Start clean: a stale result.json from an earlier run in the same dir
        # must never be reported as this execution's result.
        try:
            (exec_dir / "result.json").unlink()
        except OSError:
            pass
        context.write_context()
        (exec_dir / "params.json").write_text(
            json.dumps(context.params, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        await progress(5, f"starting {script.name}")

        env = os.environ.copy()
        env.update(
            CAPABILITY_PARAMS=json.dumps(context.params, ensure_ascii=False),
            CAPABILITY_CONTEXT=json.dumps(context.to_dict(), ensure_ascii=False),
            CAPABILITY_PACKAGE_DIR=str(package_dir),
            CAPABILITY_EXECUTION_DIR=str(exec_dir),
        )
        # Output goes to temp files, not pipes: polling a piped child with
        # repeated communicate(timeout=...) is broken on Windows (returns
        # early with a stale returncode) and large output can deadlock the
        # child on a full pipe buffer. wait(timeout) + files has neither issue.
        import tempfile as _tempfile

        try:
            out_f = _tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
            err_f = _tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
        except OSError as exc:
            return CapabilityResult.fail("CAPABILITY_EXECUTION_FAILED", f"output capture failed: {exc}")
        try:
            try:
                proc = subprocess.Popen(
                    [sys.executable, str(script)],
                    stdout=out_f,
                    stderr=err_f,
                    text=True,
                    encoding="utf-8",
                    cwd=str(package_dir),
                    env=env,
                )
            except OSError as exc:
                return CapabilityResult.fail("CAPABILITY_EXECUTION_FAILED", f"process start failed: {exc}")

            def _wait_child() -> str:
                # Runs in a worker thread: a synchronous wait loop on the
                # event loop thread would starve cancel sources (WS reads,
                # cancel tasks) and block heartbeats for the whole execution.
                while True:
                    if cancel.is_set():
                        return "cancelled"
                    try:
                        proc.wait(timeout=_POLL_INTERVAL)
                        return "exited"
                    except subprocess.TimeoutExpired:
                        continue

            try:
                verdict = await asyncio.to_thread(_wait_child)
            except Exception as exc:  # defensive: never crash the consumer loop
                _kill(proc)
                return CapabilityResult.fail("CAPABILITY_EXECUTION_FAILED", str(exc)[:500])
            if verdict == "cancelled":
                _kill(proc)
                return CapabilityResult.fail("CAPABILITY_CANCELLED", "cancelled by server")

            await progress(90, "collecting result")
            out_f.seek(0)
            err_f.seek(0)
            stdout, stderr = out_f.read(), err_f.read()
        finally:
            out_f.close()
            err_f.close()

        if proc.returncode != 0:
            tail = (stderr or stdout or "")[-800:] or f"exit {proc.returncode}"
            return CapabilityResult.fail("CAPABILITY_EXECUTION_FAILED", tail)

        payload = self._collect_result(exec_dir, stdout)
        files = _artifact_files(payload.pop("artifacts", None), exec_dir)
        return CapabilityResult.ok(data=payload, artifact_files=files)

    @staticmethod
    def _collect_result(exec_dir: Path, stdout: str) -> dict:
        result_file = exec_dir / "result.json"
        if result_file.is_file():
            try:
                parsed = json.loads(result_file.read_text(encoding="utf-8"))
                if isinstance(parsed, dict):
                    return parsed
            except ValueError:
                pass
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


def _artifact_files(entries, exec_dir: Path) -> list[tuple[str, Path]]:
    """result.json artifact descriptors -> (upload_name, absolute_path)."""
    files: list[tuple[str, Path]] = []
    if not isinstance(entries, list):
        return files
    for entry in entries:
        if not isinstance(entry, dict) or not entry.get("path"):
            continue
        raw = Path(str(entry["path"]))
        path = raw if raw.is_absolute() else exec_dir / raw
        try:
            resolved = path.resolve()
            resolved.relative_to(exec_dir.resolve())  # no escaping the execution dir
        except (ValueError, OSError):
            continue
        if not resolved.is_file():
            continue
        name = str(entry.get("name") or resolved.name)
        files.append((name, resolved))
    return files


def _kill(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


# -------------------------------------------------------------------- yingdao


class YingdaoCapabilityExecutor(CapabilityExecutor):
    """Yingdao RPA runtime (V1.4 §13): delegates to the proven worker
    YingdaoExecutor; robot identity comes from the package manifest.config."""

    runtime_type = "yingdao"

    def validate(self, manifest: Manifest, params: dict) -> None:
        super().validate(manifest, params)
        config = dict(manifest.config or {})
        if not config.get("robot_uuid"):
            raise ValueError("manifest.config.robot_uuid missing for yingdao capability")
        if not config.get("shadowbot_path"):
            raise ValueError("manifest.config.shadowbot_path missing for yingdao capability")

    async def execute(self, context: ExecutionContext, progress, cancel: asyncio.Event) -> CapabilityResult:
        executor = YingdaoExecutor()
        executor.configure(dict(context.manifest.config or {}))
        try:
            data = await executor.execute(
                context.params,
                {"__command__": context.capability, "timeout": context.timeout},
                progress,
                cancel,
            )
        except Exception as exc:  # ExecutionError from the inner executor
            code = getattr(exc, "code", "CAPABILITY_EXECUTION_FAILED")
            mapped = "CAPABILITY_CANCELLED" if code == "EXECUTOR_CANCELLED" else "CAPABILITY_EXECUTION_FAILED"
            retryable = code == "EXECUTOR_BUSY"
            return CapabilityResult.fail(mapped, getattr(exc, "message", str(exc)), retryable=retryable)
        if cancel.is_set():
            return CapabilityResult.fail("CAPABILITY_CANCELLED", "cancelled by server")
        if not isinstance(data, dict):
            data = {"output": data}
        return CapabilityResult.ok(data=data)


# ----------------------------------------------------------------------- http


class HttpCapabilityExecutor(CapabilityExecutor):
    """Enterprise API runtime (V1.4 §15): calls ONLY the predefined endpoint in
    manifest.config - task params carry data, never the URL."""

    runtime_type = "http"

    def validate(self, manifest: Manifest, params: dict) -> None:
        super().validate(manifest, params)
        import httpx  # deferred: only needed by this runtime

        config = manifest.config or {}
        url = str(config.get("url", "")).strip()
        method = str(config.get("method", "GET")).upper()
        if not url:
            raise ValueError("manifest.config.url missing for http capability")
        if not url.lower().startswith(("http://", "https://")):
            raise ValueError(f"illegal http url: {url!r}")
        if method not in _HTTP_METHODS:
            raise ValueError(f"illegal http method: {method!r}")
        del httpx

    async def execute(self, context: ExecutionContext, progress, cancel: asyncio.Event) -> CapabilityResult:
        import httpx

        config = dict(context.manifest.config or {})
        url = str(config["url"])
        method = str(config.get("method", "GET")).upper()
        headers = {str(k): str(v) for k, v in (config.get("headers") or {}).items()}
        await progress(10, f"{method} {url}")
        if cancel.is_set():
            return CapabilityResult.fail("CAPABILITY_CANCELLED", "cancelled by server")
        try:
            async with httpx.AsyncClient(timeout=context.timeout) as client:
                if method in ("GET", "DELETE"):
                    response = await client.request(method, url, params=context.params, headers=headers)
                else:
                    response = await client.request(method, url, json=context.params, headers=headers)
        except (httpx.HTTPError, OSError) as exc:
            return CapabilityResult.fail("CAPABILITY_EXECUTION_FAILED", f"http request failed: {exc}")
        if cancel.is_set():
            return CapabilityResult.fail("CAPABILITY_CANCELLED", "cancelled by server")
        await progress(90, "parsing response")
        if response.status_code < 200 or response.status_code >= 300:
            return CapabilityResult.fail(
                "CAPABILITY_EXECUTION_FAILED",
                f"HTTP {response.status_code}: {response.text[:300]}",
            )
        try:
            data = response.json()
            if not isinstance(data, dict):
                data = {"data": data}
        except ValueError:
            data = {"text": response.text[:2000]}
        return CapabilityResult.ok(data=data, metrics={"http_status": response.status_code})


# ------------------------------------------------------------------- registry

RUNTIME_EXECUTORS: dict[str, type[CapabilityExecutor]] = {
    PythonCapabilityExecutor.runtime_type: PythonCapabilityExecutor,
    YingdaoCapabilityExecutor.runtime_type: YingdaoCapabilityExecutor,
    HttpCapabilityExecutor.runtime_type: HttpCapabilityExecutor,
}


def create_executor(runtime: str) -> CapabilityExecutor:
    executor_cls = RUNTIME_EXECUTORS.get(runtime)
    if executor_cls is None:
        raise ValueError(f"unsupported capability runtime: {runtime!r}")
    return executor_cls()
