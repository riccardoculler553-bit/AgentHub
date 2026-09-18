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
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

from worker.capability.cache import work_root
from worker.capability.context import ExecutionContext
from worker.capability.manifest import Manifest
from worker.capability.result import CapabilityResult
from worker.executors.yingdao import YingdaoExecutor

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.2
_ENTRY_SAFE = re.compile(r"^[A-Za-z0-9_]+$")
_HTTP_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


class _EnvironmentError(Exception):
    """Python environment preparation failure carrying a §33 error code."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class _ChildFailed(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


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

        # V1.5 §35/§42: inject declared artifact_directory outputs - the
        # workspace owns the paths (and creates them), the capability just
        # consumes them (params[<name>]).
        output_dir = ""
        for out_name, spec in (context.manifest.outputs or {}).items():
            if isinstance(spec, dict) and spec.get("type") == "artifact_directory":
                subdir = Path(str(spec.get("path") or "output")).name
                out_path = exec_dir / subdir
                out_path.mkdir(parents=True, exist_ok=True)
                output_dir = str(out_path)
                context.params[str(out_name)] = output_dir

        # Phase 7: context/params persistence is data-plane IO - off the loop.
        await asyncio.to_thread(context.write_context)
        await asyncio.to_thread(
            (exec_dir / "params.json").write_text,
            json.dumps(context.params, ensure_ascii=False, indent=2),
            "utf-8",
        )
        await progress(5, f"starting {script.name}")

        env = os.environ.copy()
        env.update(
            CAPABILITY_PARAMS=json.dumps(context.params, ensure_ascii=False),
            CAPABILITY_CONTEXT=json.dumps(context.to_dict(), ensure_ascii=False),
            CAPABILITY_PACKAGE_DIR=str(package_dir),
            CAPABILITY_EXECUTION_DIR=str(exec_dir),
            # 子电脑控制台代码页多为 GBK：强制子进程统一 UTF-8 输出，
            # 否则捕获到的业务 ERROR 行全是替换符（2026-09-14 实测）。
            PYTHONUTF8="1",
            PYTHONIOENCODING="utf-8",
        )
        if output_dir:
            env["CAPABILITY_OUTPUT_DIR"] = output_dir

        # V1.5 §29: dedicated venv per (capability, version) when the package
        # ships requirements.txt; sys.executable otherwise.
        try:
            python_exe = await self._ensure_environment(context, progress, cancel)
        except _EnvironmentError as exc:
            if exc.code == "CAPABILITY_CANCELLED" or cancel.is_set():
                return CapabilityResult.fail("CAPABILITY_CANCELLED", "cancelled by server")
            return CapabilityResult.fail(exc.code, exc.message)

        # Output goes to temp files, not pipes: polling a piped child with
        # repeated communicate(timeout=...) is broken on Windows (returns
        # early with a stale returncode) and large output can deadlock the
        # child on a full pipe buffer. wait(timeout) + files has neither issue.
        try:
            out_f = tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
            err_f = tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
        except OSError as exc:
            return CapabilityResult.fail("CAPABILITY_EXECUTION_FAILED", f"output capture failed: {exc}")
        try:
            try:
                # Phase 7: Popen does blocking process creation - keep it off
                # the event loop (Windows CreateProcess can take 100ms+).
                proc = await asyncio.to_thread(
                    subprocess.Popen,
                    [python_exe, str(script)],
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

        # V1.5 diagnostics: persist the FULL child output for post-mortem —
        # the reported message only carries the last 800 chars, and warning
        # noise (calamine dtype fallback etc.) can bury the real crash.
        # Phase 7: output capture files can hold GBs - write off the loop.
        try:
            await asyncio.to_thread(
                (exec_dir / "stdout.log").write_text, stdout or "", "utf-8"
            )
            await asyncio.to_thread(
                (exec_dir / "stderr.log").write_text, stderr or "", "utf-8"
            )
        except OSError:
            pass

        if proc.returncode != 0:
            # 业务失败走 stdout（log=print），运行时警告走 stderr —— 只取其一
            # 会把真因埋掉，两段 tail 都带上（stdout 多给些空间放业务 ERROR 行）。
            tail = (stderr or "")[-300:]
            out_tail = (stdout or "")[-700:]
            detail = tail if not out_tail else f"{tail}\n--- stdout ---\n{out_tail}"
            return CapabilityResult.fail(
                "CAPABILITY_EXECUTION_FAILED",
                f"exit {proc.returncode}: {detail.strip() or 'no output'}",
            )

        payload = self._collect_result(exec_dir, stdout)
        files = _artifact_files(payload.pop("artifacts", None), exec_dir)
        if not files:
            # V1.5 §35: no explicit artifact list -> scan the declared output
            # directory (default output/) and upload everything found.
            # Phase 7: recursive directory scan runs on a thread.
            files = await asyncio.to_thread(_scan_output_dirs, exec_dir, context.manifest)
        return CapabilityResult.ok(data=payload, artifact_files=files)

    # ------------------------------------------------------------- python env

    async def _ensure_environment(self, context: ExecutionContext, progress, cancel: asyncio.Event) -> str:
        """V1.5 §29/§30: venv + pip install -r requirements.txt.

        Cached per (capability, version) under <work>/envs; the .deps_ok
        marker stores the requirements hash so dependency changes re-install.
        Returns the interpreter path to run the entrypoint with."""
        requirements = Path(context.package_dir) / "requirements.txt"
        if not requirements.is_file():
            return sys.executable
        try:
            req_hash = hashlib.sha256(requirements.read_bytes()).hexdigest()
        except OSError as exc:
            raise _EnvironmentError("PYTHON_ENV_CREATE_FAILED", f"unreadable requirements.txt: {exc}") from exc

        env_dir = work_root() / "envs" / context.capability / context.version
        python_exe = env_dir / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        marker = env_dir / ".deps_ok"
        if python_exe.is_file():
            try:
                if marker.read_text(encoding="utf-8").strip() == req_hash:
                    return str(python_exe)
            except OSError:
                pass

        await progress(10, "creating python environment")
        if cancel.is_set():
            raise _EnvironmentError("CAPABILITY_CANCELLED", "cancelled by server")
        try:
            await self._run_child(
                [sys.executable, "-m", "venv", str(env_dir)], cancel, "PYTHON_ENV_CREATE_FAILED"
            )
        except _ChildFailed as exc:
            raise _EnvironmentError(exc.code, exc.message) from exc

        await progress(15, "installing dependencies")
        try:
            await self._run_child(
                [
                    str(python_exe), "-m", "pip", "install",
                    "--disable-pip-version-check", "-r", str(requirements),
                ],
                cancel,
                "DEPENDENCY_INSTALL_FAILED",
            )
        except _ChildFailed as exc:
            raise _EnvironmentError(exc.code, exc.message) from exc
        if cancel.is_set():
            raise _EnvironmentError("CAPABILITY_CANCELLED", "cancelled by server")

        try:
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(req_hash, encoding="utf-8")
        except OSError as exc:
            raise _EnvironmentError("PYTHON_ENV_CREATE_FAILED", f"marker write failed: {exc}") from exc
        logger.info("python env ready: %s", env_dir)
        return str(python_exe)

    async def _run_child(self, cmd: list[str], cancel: asyncio.Event, code: str) -> None:
        """Cancel-aware blocking child (venv/pip); output captured to temp files."""
        try:
            out_f = tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
            err_f = tempfile.TemporaryFile("w+", encoding="utf-8", errors="replace")
        except OSError as exc:
            raise _ChildFailed(code, f"output capture failed: {exc}") from exc
        try:
            try:
                proc = subprocess.Popen(
                    cmd, stdout=out_f, stderr=err_f, text=True, encoding="utf-8",
                )
            except OSError as exc:
                raise _ChildFailed(code, f"process start failed: {exc}") from exc

            def _wait() -> str:
                while True:
                    if cancel.is_set():
                        return "cancelled"
                    try:
                        proc.wait(timeout=_POLL_INTERVAL)
                        return "exited"
                    except subprocess.TimeoutExpired:
                        continue

            verdict = await asyncio.to_thread(_wait)
            if verdict == "cancelled":
                _kill(proc)
                raise _ChildFailed("CAPABILITY_CANCELLED", "cancelled by server")
            if proc.returncode != 0:
                out_f.seek(0)
                err_f.seek(0)
                tail = (err_f.read() or out_f.read() or "")[-800:] or f"exit {proc.returncode}"
                raise _ChildFailed(code, tail)
        finally:
            out_f.close()
            err_f.close()

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


def _scan_output_dirs(exec_dir: Path, manifest: Manifest) -> list[tuple[str, Path]]:
    """V1.5 §35: collect every file from the declared artifact_directory
    outputs (default output/) - the no-result.json fallback."""
    subdirs: list[str] = []
    for spec in (manifest.outputs or {}).values():
        if isinstance(spec, dict) and spec.get("type") == "artifact_directory":
            subdirs.append(Path(str(spec.get("path") or "output")).name)
    if not subdirs:
        subdirs = ["output"]
    files: list[tuple[str, Path]] = []
    for subdir in subdirs:
        base = exec_dir / subdir
        if not base.is_dir():
            continue
        for path in sorted(base.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(base)
            if path.name.startswith(("~$", "__tmp_", ".")) or "backup" in rel.parts:
                continue
            files.append((path.name, path))
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
