"""TaskManager: worker-side task lifecycle (PDF §37-§38/§74-§77/§108-§110).

Responsibilities:
- receive task.dispatch AND capability.execute (V1.4), dedup by
  (task_id, step_id) in-memory AND by attempt_id in the local ExecutionLedger
  (SQLite) - idempotent execution
- concurrency control: one task at a time (max_concurrency = 1)
- task.* / capability.* accept/running/progress/result reporting with
  attempt_id echoed back on every envelope
- cancel handling (terminate subprocesses, cleanup, report)
- recovery: unacknowledged terminal results are re-reported after reconnect;
  attempts left RUNNING by a dead process are parked FAILED at startup

V1.4 capability path (§43/§51/§65): on capability.execute the manager first
guarantees the package via CapabilityManager.ensure (Lazy Pull, per-version
package lock), then resolves the runtime executor from the manifest and runs
with an ExecutionContext; artifact files are uploaded and replaced by
artifact references before capability.result is reported (§45). Retry/
timeout/cancel policy stays with the server Task Engine (§67/§68/§69).

The WebSocket loop is NEVER blocked: executions run on a single consumer task
fed by an asyncio.Queue, subprocess work happens in executor code that polls
the cancel event. RPA lifetime is bound to the Worker PROCESS, not to the WS
connection (PDF §108-§109).
"""

import asyncio
import logging
import shutil
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import protocol
from worker.capability.context import ExecutionContext
from worker.capability.executors import create_executor
from worker.capability.manager import CapabilityInstallError
from worker.capability.result import CapabilityResult
from worker.capability.uploader import ArtifactUploadFailed
from worker.capability.downloader import (
    ArtifactChecksumMismatch,
    ArtifactDownloadCancelled,
    ArtifactDownloadFailed,
    ArtifactDownloadStalled,
    ArtifactDownloadTimeout,
)
from worker.executor import ExecutionError
from worker.ledger import ExecutionLedger
from worker.registry import build_command_registry

logger = logging.getLogger(__name__)


@dataclass
class _Outcome:
    """Normalized execution outcome feeding ledger + result report."""

    terminal: str  # "success" | "failed" | "cancelled"
    ledger_status: str = "FAILED"  # SUCCESS | FAILED | TIMEOUT | CANCELLED
    result: dict | None = None
    error: dict | None = None
    metrics: dict = field(default_factory=dict)


class _CapabilityInputError(Exception):
    """Input artifact preparation failure carrying a §33 error code."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class TaskManager:
    def __init__(
        self,
        max_concurrency: int = 1,
        ledger: ExecutionLedger | None = None,
        capability_manager=None,
        artifact_uploader=None,
        artifact_downloader=None,
    ) -> None:
        self.registry = build_command_registry()
        self.ledger = ledger or ExecutionLedger()
        self.capability_manager = capability_manager
        self.artifact_uploader = artifact_uploader
        self.artifact_downloader = artifact_downloader
        self.worker_id = ""
        self.max_concurrency = max(1, max_concurrency)
        self._active: dict[tuple[str, str], dict[str, Any]] = {}  # (task_id, step_id) -> state
        self._queue: asyncio.Queue = asyncio.Queue()
        self._consumer: asyncio.Task | None = None
        self._flush_task: asyncio.Task | None = None
        self._ws_client = None
        # Startup recovery (V1.6 P0 0.3): RUNNING attempts from a previous
        # process cannot be tracked anymore - park them FAILED so reconnect
        # reports the truth instead of blocking the ledger forever. ACCEPTED
        # attempts never executed: their claims are dropped so the server's
        # liveness scan converges them (retryable), never a false FAILED.
        parked = self.ledger.fail_running()
        if parked:
            logger.warning("parked %s orphaned attempt(s) from a previous run", parked)
        dropped = self.ledger.drop_unexecuted()
        if dropped:
            logger.info("dropped %s never-executed claim(s) from a previous run", dropped)

    # ------------------------------------------------------------------ wiring

    def bind(self, ws_client) -> None:
        """(Re)bind the current WebSocketClient - called on every connect.
        Reconnection is the moment to flush unacknowledged results."""
        self._ws_client = ws_client
        # Strong ref: an unnamed fire-and-forget task can be GC'd mid-flight.
        self._flush_task = asyncio.create_task(self._flush_unreported())

    def ensure_consumer(self) -> None:
        if self._consumer is None or self._consumer.done():
            self._consumer = asyncio.create_task(self._consumer_loop())

    async def _flush_unreported(self) -> None:
        try:
            pending = self.ledger.unreported_results()
        except Exception:
            logger.exception("ledger scan failed")
            return
        for row in pending:
            # Capability rows are stored as "capability:<name>@<version>".
            kind = "capability" if str(row["command"]).startswith("capability:") else "task"
            await self._report_result(
                task_id=row["task_id"],
                step_id=row["step_id"],
                attempt_id=row["attempt_id"],
                status=_ledger_status_to_result(row["status"]),
                result=json_or_none(row["result"]),
                error_code=row["error_code"],
                error_message=row["error_message"],
                kind=kind,
                ledger_row_id=row["attempt_id"],
            )

    # ------------------------------------------------------------------ intake

    async def on_dispatch(self, envelope: dict) -> None:
        await self._intake(envelope, kind="task")

    async def on_capability_execute(self, envelope: dict) -> None:
        """V1.4 §65: capability.execute shares the Task state machine."""
        await self._intake(envelope, kind="capability")

    async def _intake(self, envelope: dict, kind: str) -> None:
        data = envelope.get("data", {})
        task_id = str(data.get("task_id", ""))
        step_id = str(data.get("step_id", ""))
        attempt_id = str(data.get("attempt_id", ""))
        params = data.get("params") or {}
        if kind == "capability":
            capability = str(data.get("capability", ""))
            version = str(data.get("version", ""))
            # Ledger command column carries the capability identity (used by
            # the reconnect flush to pick the result envelope type).
            command = f"capability:{capability}@{version}"
            display = command.removeprefix("capability:")
        else:
            capability = version = ""
            command = str(data.get("command", ""))
            display = command
        if not task_id or not step_id:
            return
        if not attempt_id:
            # Server always sends attempt_id in V1.0+; tolerate legacy senders
            # by falling back to the step key.
            attempt_id = f"legacy-{task_id}-{step_id}"

        # V1.1 §12/§55.9: the active-execution check MUST run before the
        # ledger claim - claiming a different attempt while one is live would
        # create an orphan ledger row and an "ACCEPTED but never runs" state.
        active = self._active.get((task_id, step_id))
        if active is not None:
            if attempt_id and active["attempt_id"] != attempt_id:
                # A new attempt arrived while the old one still runs (e.g. the
                # server watchdog fired mid-execution). Never claim/execute it:
                # echo the ACTIVE attempt so the server keeps its real context.
                logger.warning(
                    "dispatch %s ignored: %s/%s still active as attempt %s",
                    attempt_id, task_id, step_id, active["attempt_id"],
                )
                await self._report(
                    f"{active['kind']}.running",
                    {"task_id": task_id, "step_id": step_id, "attempt_id": active["attempt_id"]},
                )
                return
            # Duplicate of the active attempt -> idempotent report, no re-run.
            logger.warning(
                "duplicate dispatch for %s/%s (already %s)", task_id, step_id, active["status"]
            )
            await self._report(
                self._echo_type(active), {"task_id": task_id, "step_id": step_id,
                                          "attempt_id": active["attempt_id"]},
            )
            return

        previous = self.ledger.claim(task_id, step_id, attempt_id, command)
        if previous is not None:
            # Idempotency (PDF §75/§119): duplicate dispatch -> never re-execute.
            logger.warning("duplicate dispatch for attempt %s (already %s)", attempt_id, previous)
            if previous in ("SUCCESS", "FAILED", "TIMEOUT", "CANCELLED"):
                row = self.ledger.get(attempt_id) or {}
                await self._report_result(
                    task_id, step_id, attempt_id, _ledger_status_to_result(previous),
                    result=json_or_none(row.get("result")),
                    error_code=row.get("error_code"),
                    error_message=row.get("error_message"),
                    kind=kind,
                    ledger_row_id=attempt_id,
                )
            else:
                # Non-terminal ledger row: the attempt was claimed but is still
                # queued (or mid-run). Echo the truthful phase - ACCEPTED means
                # received, never "executing".
                msg = f"{kind}.running" if previous == "RUNNING" else f"{kind}.accept"
                await self._report(
                    msg,
                    {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id},
                )
            return

        state: dict[str, Any] = {
            "task_id": task_id,
            "step_id": step_id,
            "attempt_id": attempt_id,
            "command": command,
            "params": params,
            "timeout": data.get("timeout"),
            "kind": kind,
            "cancel": asyncio.Event(),
            "status": "queued",
            "workflow_run_id": data.get("workflow_run_id"),
            "step_run_id": data.get("step_run_id"),
        }
        if kind == "task":
            executor = self.registry.get(command)
            if executor is None:
                logger.warning("no local executor for command %s", command)
                self.ledger.mark_finished(
                    attempt_id, "FAILED", error_code="COMMAND_NOT_FOUND",
                    error_message=f"worker has no executor for {command}",
                )
                await self._report_result(
                    task_id, step_id, attempt_id, "failed",
                    error_code="COMMAND_NOT_FOUND",
                    error_message=f"worker has no executor for {command}",
                    ledger_row_id=attempt_id,
                )
                return
            try:
                executor.validate(params)
            except ValueError as exc:
                self.ledger.mark_finished(
                    attempt_id, "FAILED", error_code="INVALID_PARAMS", error_message=str(exc)
                )
                await self._report_result(
                    task_id, step_id, attempt_id, "failed",
                    error_code="INVALID_PARAMS", error_message=str(exc),
                    ledger_row_id=attempt_id,
                )
                return
            state["executor"] = executor
        else:
            state.update(
                capability=str(data.get("capability", "")),
                version=str(data.get("version", "")),
                package_id=str(data.get("package_id", "")),
                checksum=str(data.get("checksum") or "") or None,
                input_artifacts=data.get("input_artifacts") or [],
            )
            if not state["capability"] or not state["version"]:
                self.ledger.mark_finished(
                    attempt_id, "FAILED", error_code="INVALID_PARAMS",
                    error_message="capability.execute missing capability/version",
                )
                await self._report_result(
                    task_id, step_id, attempt_id, "failed",
                    error_code="INVALID_PARAMS",
                    error_message="capability.execute missing capability/version",
                    kind=kind,
                    ledger_row_id=attempt_id,
                )
                return

        self._active[(task_id, step_id)] = state
        await self._report(
            f"{kind}.accept", {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id}
        )
        self.ensure_consumer()
        await self._queue.put((task_id, step_id))

    async def on_cancel(self, data: dict) -> None:
        task_id = str(data.get("task_id", ""))
        step_id = data.get("step_id")
        attempt_id = str(data.get("attempt_id", "")) or None
        for key, state in list(self._active.items()):
            if key[0] != task_id:
                continue
            if step_id and key[1] != step_id:
                continue
            if attempt_id and state["attempt_id"] != attempt_id:
                continue
            logger.warning("cancel requested for %s/%s", task_id, key[1])
            state["status"] = "cancelling"
            state["cancel"].set()

    @staticmethod
    def _echo_type(state: dict) -> str:
        """Envelope type echoing the TRUE phase of an active attempt:
        queued attempts report accept - they have not started executing."""
        return f"{state['kind']}.running" if state["status"] == "running" else f"{state['kind']}.accept"

    # ---------------------------------------------------------------- consumer

    async def _consumer_loop(self) -> None:
        while True:
            key = await self._queue.get()
            state = self._active.get(key)
            if state is None:  # cancelled before start
                self._queue.task_done()
                continue
            if state["cancel"].is_set():
                # Cancelled while queued: never execute. The attempt closes as
                # CANCELLED here - otherwise a cancel arriving between intake
                # and dequeue leaves an ACCEPTED task that silently never runs
                # (Phase 2: every non-terminal state must have an exit).
                state["status"] = "cancelled"
                self.ledger.mark_finished(state["attempt_id"], "CANCELLED")
                await self._report_result(
                    state["task_id"], state["step_id"], state["attempt_id"], "cancelled",
                    kind=state["kind"], ledger_row_id=state["attempt_id"],
                )
                self._active.pop(key, None)
                self._queue.task_done()
                continue
            state["status"] = "running"
            try:
                await self._execute(state)
            finally:
                self._active.pop(key, None)
                self._queue.task_done()

    async def _execute(self, state: dict) -> None:
        task_id, step_id, attempt_id = state["task_id"], state["step_id"], state["attempt_id"]
        self.ledger.mark_running(attempt_id)

        async def progress(pct: int, message: str) -> None:
            # Phase 8: per-attempt monotonic seq - the server drops any
            # progress event whose seq would move the snapshot backwards.
            state["progress_seq"] = int(state.get("progress_seq") or 0) + 1
            await self._report(
                f"{state['kind']}.progress",
                {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id,
                 "progress": int(pct), "message": message,
                 "seq": state["progress_seq"]},
            )

        await self._report(
            f"{state['kind']}.running", {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id}
        )
        cancel: asyncio.Event = state["cancel"]
        try:
            if state["kind"] == "capability":
                outcome = await self._run_capability(state, progress)
            else:
                outcome = await self._run_task(state, progress)
        except Exception as exc:  # defensive: never let the consumer die
            logger.exception("executor crashed")
            outcome = _Outcome("failed", error={"code": "EXECUTOR_FAILED", "message": str(exc)[:500]})

        if cancel.is_set() and outcome.terminal == "success":
            # Executor returned success but a cancel raced in: honour cancel.
            outcome = _Outcome("cancelled")

        if outcome.terminal == "success":
            self.ledger.mark_finished(attempt_id, "SUCCESS", result=outcome.result)
            await self._report_result(
                task_id, step_id, attempt_id, "success", result=outcome.result,
                kind=state["kind"], ledger_row_id=attempt_id,
            )
            logger.info("task %s/%s SUCCESS", task_id, step_id)
        elif outcome.terminal == "cancelled":
            self.ledger.mark_finished(attempt_id, "CANCELLED")
            await self._report_result(
                task_id, step_id, attempt_id, "cancelled", kind=state["kind"], ledger_row_id=attempt_id
            )
        else:
            error = outcome.error or {"code": "EXECUTOR_FAILED", "message": "execution failed"}
            ledger_status = "TIMEOUT" if error.get("code") == "EXECUTOR_TIMEOUT" else "FAILED"
            self.ledger.mark_finished(
                attempt_id, ledger_status, error_code=error.get("code"), error_message=error.get("message")
            )
            await self._report_result(
                task_id, step_id, attempt_id, "failed",
                error_code=error.get("code"), error_message=error.get("message"),
                kind=state["kind"], ledger_row_id=attempt_id,
            )
            logger.error("task %s/%s %s (%s)", task_id, step_id, ledger_status, error.get("code"))

    # ------------------------------------------------------------- legacy task

    async def _run_task(self, state: dict, progress) -> _Outcome:
        cancel = state["cancel"]
        config = {"__command__": state["command"], "timeout": state.get("timeout")}
        try:
            result = await state["executor"].execute(state["params"], config, progress, cancel)
            if cancel.is_set():
                return _Outcome("cancelled", ledger_status="CANCELLED")
            return _Outcome("success", ledger_status="SUCCESS", result=result)
        except ExecutionError as exc:
            if exc.code == "EXECUTOR_CANCELLED" or cancel.is_set():
                return _Outcome("cancelled", ledger_status="CANCELLED")
            if exc.code == "EXECUTOR_TIMEOUT":
                return _Outcome("failed", ledger_status="TIMEOUT",
                                error={"code": exc.code, "message": exc.message})
            return _Outcome("failed", ledger_status="FAILED",
                            error={"code": exc.code, "message": exc.message})

    # ------------------------------------------------------ capability (V1.4)

    async def _run_capability(self, state: dict, progress) -> _Outcome:
        """§43 Worker 本地执行流程 (V1.5 §16): prepare package -> prepare
        inputs -> run -> upload -> normalize."""
        name, version = state["capability"], state["version"]
        if self.capability_manager is None:
            return _Outcome("failed", error={
                "code": "CAPABILITY_RUNTIME_UNAVAILABLE",
                "message": "capability manager not configured on this worker",
            })
        try:
            await progress(5, f"preparing {name}@{version}")
            installed = await self.capability_manager.ensure(
                name, version, state["package_id"], state["checksum"]
            )
            await progress(20, "package ready")
        except CapabilityInstallError as exc:
            return _Outcome("failed", error={"code": exc.code, "message": str(exc)[:500]})

        context = ExecutionContext(
            execution_id=state["attempt_id"],  # §41: execution_id == attempt_id
            task_id=state["task_id"],
            step_id=state["step_id"],
            attempt_id=state["attempt_id"],
            capability=name,
            version=version,
            worker_id=self.worker_id,
            params=state["params"],
            manifest=installed.manifest,
            package_dir=installed.path,
            workflow_run_id=state.get("workflow_run_id"),
            step_run_id=state.get("step_run_id"),
            timeout=int(state.get("timeout") or 600),
        )

        # V1.5 §16: download input artifacts into the workspace BEFORE
        # validation - artifact_directory inputs become local dirs injected
        # into params (role == manifest input name).
        try:
            await self._prepare_inputs(state, context, installed.manifest, progress)
        except _CapabilityInputError as exc:
            return _Outcome("failed", error={"code": exc.code, "message": str(exc)[:500]})

        try:
            executor = create_executor(installed.manifest.runtime)
            executor.validate(installed.manifest, state["params"])
        except ValueError as exc:
            return _Outcome("failed", error={"code": "INVALID_PARAMS", "message": str(exc)[:500]})

        result = await executor.execute(context, progress, state["cancel"])
        if not isinstance(result, CapabilityResult):  # defensive against runtimes
            result = CapabilityResult.ok(data={"output": str(result)[:2000]})

        if result.success and result.artifact_files:
            await progress(95, "uploading artifacts")
            refs = await self._upload_artifacts(state, result.artifact_files)
            if isinstance(refs, _Outcome):  # upload failed
                return refs
            result.artifacts = refs

        payload = result.to_payload()
        if result.success:
            return _Outcome("success", ledger_status="SUCCESS", result=payload)
        if payload.get("error_code") == "CAPABILITY_CANCELLED" or state["cancel"].is_set():
            return _Outcome("cancelled", ledger_status="CANCELLED")
        return _Outcome("failed", error={
            "code": payload.get("error_code") or "CAPABILITY_EXECUTION_FAILED",
            # V1.5: keep the TAIL (traceback / business ERROR lines) — a head
            # truncation here used to cut the traceback off the report.
            "message": str(payload.get("message", ""))[-1000:],
            # V1.6 P0 0.14: worker hint for the server's risk-aware retry
            # policy - it may only assist READ tasks, never override WRITE/ACTION.
            "retryable": bool(payload.get("retryable")),
        })

    async def _prepare_inputs(self, state: dict, context: ExecutionContext, manifest, progress) -> None:
        """V1.5 §16/§31/§54: download input artifacts into the execution
        workspace and inject the resolved directories into params.

        role == manifest input name (server stores the reference under that
        role, §15). The manifest spec may name the workspace subdir explicitly
        ({"path": "input"}); default is the role name itself."""
        entries = state.get("input_artifacts") or []
        if not entries:
            return
        if self.artifact_downloader is None:
            raise _CapabilityInputError(
                "ARTIFACT_DOWNLOAD_FAILED", "artifact downloader not configured on this worker"
            )
        specs = {
            str(input_name): spec
            for input_name, spec in (manifest.inputs or {}).items()
            if isinstance(spec, dict) and spec.get("type") == "artifact_directory"
        }
        exec_dir = context.execution_dir()
        params = state["params"]
        for index, entry in enumerate(entries):
            if not isinstance(entry, dict) or not str(entry.get("artifact_id", "")).strip():
                raise _CapabilityInputError(
                    "INVALID_PARAMS", f"input_artifacts[{index}] missing artifact_id"
                )
            if state["cancel"].is_set():
                return
            artifact_id = str(entry["artifact_id"])
            role = str(entry.get("role") or "input")
            spec = specs.get(role) or {}
            subdir = Path(str(spec.get("path") or role)).name  # no traversal
            dest_dir = exec_dir / subdir
            dest_dir.mkdir(parents=True, exist_ok=True)
            try:
                cached = await self.artifact_downloader.download(
                    artifact_id, checksum=entry.get("checksum"), cancel=state["cancel"]
                )
            except ArtifactDownloadCancelled as exc:
                raise _CapabilityInputError("CAPABILITY_CANCELLED", str(exc)) from exc
            except ArtifactChecksumMismatch as exc:
                raise _CapabilityInputError("ARTIFACT_CHECKSUM_MISMATCH", str(exc)) from exc
            except ArtifactDownloadStalled as exc:
                # V1.6 P0 0.15: connect / stall / total are distinct facts -
                # never collapse them into one TIMEOUT (readability §3.4).
                raise _CapabilityInputError("ARTIFACT_DOWNLOAD_STALLED", str(exc)) from exc
            except ArtifactDownloadTimeout as exc:
                raise _CapabilityInputError("ARTIFACT_DOWNLOAD_TIMEOUT", str(exc)) from exc
            except ArtifactDownloadFailed as exc:
                raise _CapabilityInputError("ARTIFACT_DOWNLOAD_FAILED", str(exc)) from exc
            filename = Path(str(entry.get("name") or "").strip() or f"{artifact_id}.bin").name
            dest = dest_dir / filename
            if dest.exists():
                dest = dest_dir / f"{artifact_id[:8]}_{filename}"
            await asyncio.to_thread(shutil.copyfile, cached, dest)
            # §21: the capability receives paths, never artifact ids.
            if role in specs:
                params[role] = str(dest_dir)
            logger.info("input artifact %s (%s) -> %s", artifact_id, filename, dest)
            await progress(30, f"input ready: {filename}")

    async def _upload_artifacts(self, state: dict, files) -> list[dict] | _Outcome:
        """Upload produced files (§45: the result only carries artifact IDs)."""
        if self.artifact_uploader is None:
            return _Outcome("failed", error={
                "code": "ARTIFACT_UPLOAD_FAILED",
                "message": "artifact uploader not configured on this worker",
            })
        refs: list[dict] = []
        for upload_name, path in files:
            try:
                uploaded = await self.artifact_uploader.upload(
                    path,
                    name=upload_name,
                    task_id=state["task_id"],
                    workflow_run_id=state.get("workflow_run_id"),
                    step_run_id=state.get("step_run_id"),
                )
            except ArtifactUploadFailed as exc:
                return _Outcome("failed", error={
                    "code": "ARTIFACT_UPLOAD_FAILED", "message": str(exc)[:500],
                })
            refs.append({
                "artifact_id": uploaded.get("artifact_id", ""),
                "name": uploaded.get("name", upload_name),
                "type": uploaded.get("type", "file"),
            })
        return refs

    # ------------------------------------------------------------------ report

    async def _report(self, msg_type: str, data: dict) -> bool:
        if self._ws_client is None:
            logger.error("cannot report %s: no websocket connection", msg_type)
            return False
        envelope = protocol.build_envelope(msg_type, data)
        try:
            await self._ws_client.send(envelope)
            return True
        except Exception as exc:
            print(f"[worker] report {msg_type} failed: {exc}", file=sys.stderr)
            return False

    async def _report_result(
        self,
        task_id: str,
        step_id: str,
        attempt_id: str,
        status: str,
        *,
        result: dict | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
        kind: str = "task",
        ledger_row_id: str | None = None,
    ) -> None:
        """task.result / capability.result with attempt_id (PDF §41, V1.4 §65).
        The ledger 'reported' flag is only set after a successful send, so
        reconnect re-reports lost ones."""
        data: dict = {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id, "status": status}
        if result is not None:
            data["result"] = result
        if error_code:
            data["error"] = {"code": error_code, "message": error_message or ""}
        sent = await self._report(f"{kind}.result", data)
        if sent and ledger_row_id:
            try:
                self.ledger.mark_reported(ledger_row_id)
            except Exception:
                logger.exception("ledger mark_reported failed for %s", ledger_row_id)


def json_or_none(raw) -> dict | None:
    import json

    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        return None


def _ledger_status_to_result(status: str) -> str:
    # Ledger terminal status -> result status word.
    if status == "SUCCESS":
        return "success"
    if status == "CANCELLED":
        return "cancelled"
    return "failed"
