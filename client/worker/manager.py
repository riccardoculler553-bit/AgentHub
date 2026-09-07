"""TaskManager: worker-side task lifecycle (PDF §37-§38/§74-§77/§108-§110).

Responsibilities:
- receive task.dispatch, dedup by (task_id, step_id) in-memory AND by
  attempt_id in the local ExecutionLedger (SQLite) - idempotent execution
- concurrency control: one task at a time (max_concurrency = 1)
- task.accept / task.running / task.progress / task.result reporting with
  attempt_id echoed back on every envelope
- cancel handling (terminate subprocesses, cleanup, report)
- recovery: unacknowledged terminal results are re-reported after reconnect;
  attempts left RUNNING by a dead process are parked FAILED at startup

The WebSocket loop is NEVER blocked: executions run on a single consumer task
fed by an asyncio.Queue, subprocess work happens in executor code that polls
the cancel event. RPA lifetime is bound to the Worker PROCESS, not to the WS
connection (PDF §108-§109).
"""

import asyncio
import logging
import sys
from typing import Any

import protocol
from worker.executor import ExecutionError
from worker.ledger import ExecutionLedger
from worker.registry import build_command_registry

logger = logging.getLogger(__name__)


class TaskManager:
    def __init__(self, max_concurrency: int = 1, ledger: ExecutionLedger | None = None) -> None:
        self.registry = build_command_registry()
        self.ledger = ledger or ExecutionLedger()
        self.max_concurrency = max(1, max_concurrency)
        self._active: dict[tuple[str, str], dict[str, Any]] = {}  # (task_id, step_id) -> state
        self._queue: asyncio.Queue = asyncio.Queue()
        self._consumer: asyncio.Task | None = None
        self._ws_client = None
        # Startup recovery: RUNNING attempts from a previous process cannot be
        # tracked anymore - park them FAILED so reconnect reports the truth
        # instead of blocking the ledger forever.
        parked = self.ledger.fail_running()
        if parked:
            logger.warning("parked %s orphaned attempt(s) from a previous run", parked)

    # ------------------------------------------------------------------ wiring

    def bind(self, ws_client) -> None:
        """(Re)bind the current WebSocketClient - called on every connect.
        Reconnection is the moment to flush unacknowledged results."""
        self._ws_client = ws_client
        asyncio.create_task(self._flush_unreported())

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
            await self._report_result(
                task_id=row["task_id"],
                step_id=row["step_id"],
                attempt_id=row["attempt_id"],
                status=_ledger_status_to_result(row["status"]),
                result=json_or_none(row["result"]),
                error_code=row["error_code"],
                error_message=row["error_message"],
                ledger_row_id=row["attempt_id"],
            )

    # ------------------------------------------------------------------ intake

    async def on_dispatch(self, envelope: dict) -> None:
        data = envelope.get("data", {})
        task_id = str(data.get("task_id", ""))
        step_id = str(data.get("step_id", ""))
        attempt_id = str(data.get("attempt_id", ""))
        command = str(data.get("command", ""))
        params = data.get("params") or {}
        if not task_id or not step_id:
            return
        if not attempt_id:
            # Server always sends attempt_id in V1.0+; tolerate legacy senders
            # by falling back to the step key.
            attempt_id = f"legacy-{task_id}-{step_id}"

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
                    ledger_row_id=attempt_id,
                )
            else:
                await self._report(
                    "task.running",
                    {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id},
                )
            return

        if (task_id, step_id) in self._active:
            state = self._active[(task_id, step_id)]
            logger.warning("duplicate dispatch for %s/%s (already %s)", task_id, step_id, state["status"])
            await self._report(
                "task.running",
                {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id},
            )
            return

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

        self._active[(task_id, step_id)] = {
            "task_id": task_id,
            "step_id": step_id,
            "attempt_id": attempt_id,
            "command": command,
            "params": params,
            "timeout": data.get("timeout"),
            "executor": executor,
            "cancel": asyncio.Event(),
            "status": "queued",
        }
        await self._report(
            "task.accept", {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id}
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

    # ---------------------------------------------------------------- consumer

    async def _consumer_loop(self) -> None:
        while True:
            key = await self._queue.get()
            state = self._active.get(key)
            if state is None:  # cancelled before start
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
            await self._report(
                "task.progress",
                {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id,
                 "progress": int(pct), "message": message},
            )

        await self._report(
            "task.running", {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id}
        )
        cancel: asyncio.Event = state["cancel"]
        config = {"__command__": state["command"], "timeout": state.get("timeout")}
        try:
            result = await state["executor"].execute(state["params"], config, progress, cancel)
            if cancel.is_set():
                self.ledger.mark_finished(attempt_id, "CANCELLED")
                await self._report_result(task_id, step_id, attempt_id, "cancelled", ledger_row_id=attempt_id)
            else:
                self.ledger.mark_finished(attempt_id, "SUCCESS", result=result)
                await self._report_result(
                    task_id, step_id, attempt_id, "success", result=result, ledger_row_id=attempt_id
                )
                logger.info("task %s/%s SUCCESS", task_id, step_id)
        except ExecutionError as exc:
            if exc.code == "EXECUTOR_CANCELLED" or cancel.is_set():
                self.ledger.mark_finished(attempt_id, "CANCELLED")
                await self._report_result(task_id, step_id, attempt_id, "cancelled", ledger_row_id=attempt_id)
            elif exc.code == "EXECUTOR_TIMEOUT":
                self.ledger.mark_finished(attempt_id, "TIMEOUT", error_code=exc.code, error_message=exc.message)
                await self._report_result(
                    task_id, step_id, attempt_id, "failed",
                    error_code=exc.code, error_message=exc.message, ledger_row_id=attempt_id,
                )
                logger.error("task %s/%s TIMEOUT", task_id, step_id)
            else:
                self.ledger.mark_finished(attempt_id, "FAILED", error_code=exc.code, error_message=exc.message)
                await self._report_result(
                    task_id, step_id, attempt_id, "failed",
                    error_code=exc.code, error_message=exc.message, ledger_row_id=attempt_id,
                )
                logger.error("task %s/%s FAILED (%s)", task_id, step_id, exc.code)
        except Exception as exc:  # defensive: never let the consumer die
            logger.exception("executor crashed")
            self.ledger.mark_finished(attempt_id, "FAILED", error_code="EXECUTOR_FAILED", error_message=str(exc)[:500])
            await self._report_result(
                task_id, step_id, attempt_id, "failed",
                error_code="EXECUTOR_FAILED", error_message=str(exc)[:500], ledger_row_id=attempt_id,
            )

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
        ledger_row_id: str | None = None,
    ) -> None:
        """task.result with attempt_id (PDF §41). The ledger 'reported' flag is
        only set after a successful send, so reconnect re-reports lost ones."""
        data: dict = {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id, "status": status}
        if result is not None:
            data["result"] = result
        if error_code:
            data["error"] = {"code": error_code, "message": error_message or ""}
        sent = await self._report("task.result", data)
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
    # Ledger terminal status -> task.result status word.
    if status == "SUCCESS":
        return "success"
    if status == "CANCELLED":
        return "cancelled"
    return "failed"
