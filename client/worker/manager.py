"""TaskManager: worker-side task lifecycle (PDF §46-50).

Responsibilities:
- receive task.dispatch, dedup by (task_id, step_id) - idempotent execution
- concurrency control: one task at a time (V1.0 max_concurrency = 1)
- task.accept / task.running / task.progress / task.result reporting
- cancel handling (terminate subprocesses, cleanup, report)

The WebSocket loop is NEVER blocked: executions run on a single consumer task
fed by an asyncio.Queue, subprocess work happens in executor code that polls
the cancel event.

NOTE (V1.0 limitation): if the connection dies mid-execution, reports are
lost; the server-side TaskMonitor watchdog eventually TIMEOUTs the task.
"""

import asyncio
import logging
import sys
from typing import Any

import protocol
from worker.executor import ExecutionError
from worker.registry import build_command_registry

logger = logging.getLogger(__name__)


class TaskManager:
    def __init__(self, max_concurrency: int = 1) -> None:
        self.registry = build_command_registry()
        self.max_concurrency = max(1, max_concurrency)
        self._active: dict[tuple[str, str], dict[str, Any]] = {}  # (task_id, step_id) -> state
        self._queue: asyncio.Queue = asyncio.Queue()
        self._consumer: asyncio.Task | None = None
        self._ws_client = None

    # ------------------------------------------------------------------ wiring

    def bind(self, ws_client) -> None:
        """(Re)bind the current WebSocketClient - called on every connect."""
        self._ws_client = ws_client

    def ensure_consumer(self) -> None:
        if self._consumer is None or self._consumer.done():
            self._consumer = asyncio.create_task(self._consumer_loop())

    # ------------------------------------------------------------------ intake

    async def on_dispatch(self, envelope: dict) -> None:
        data = envelope.get("data", {})
        task_id = str(data.get("task_id", ""))
        step_id = str(data.get("step_id", ""))
        command = str(data.get("command", ""))
        params = data.get("params") or {}
        if not task_id or not step_id:
            return

        key = (task_id, step_id)
        if key in self._active:
            # Idempotency (PDF §50): duplicate dispatch -> report current state only.
            state = self._active[key]
            logger.warning("duplicate dispatch for %s/%s (already %s)", task_id, step_id, state["status"])
            await self._report("task.running", {"task_id": task_id, "step_id": step_id})
            return

        executor = self.registry.get(command)
        if executor is None:
            logger.warning("no local executor for command %s", command)
            await self._report(
                "task.result",
                {
                    "task_id": task_id, "step_id": step_id, "status": "failed",
                    "error": {"code": "COMMAND_NOT_FOUND", "message": f"worker has no executor for {command}"},
                },
            )
            return

        try:
            executor.validate(params)
        except ValueError as exc:
            await self._report(
                "task.result",
                {
                    "task_id": task_id, "step_id": step_id, "status": "failed",
                    "error": {"code": "INVALID_PARAMS", "message": str(exc)},
                },
            )
            return

        self._active[key] = {
            "task_id": task_id,
            "step_id": step_id,
            "command": command,
            "params": params,
            "executor": executor,
            "cancel": asyncio.Event(),
            "status": "queued",
        }
        await self._report("task.accept", {"task_id": task_id, "step_id": step_id})
        self._active[key]["status"] = "queued"
        await self._queue.put(key)

    async def on_cancel(self, data: dict) -> None:
        task_id = str(data.get("task_id", ""))
        step_id = data.get("step_id")
        for key, state in list(self._active.items()):
            if key[0] != task_id:
                continue
            if step_id and key[1] != step_id:
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
        task_id, step_id = state["task_id"], state["step_id"]

        async def progress(pct: int, message: str) -> None:
            await self._report(
                "task.progress",
                {"task_id": task_id, "step_id": step_id, "progress": int(pct), "message": message},
            )

        await self._report("task.running", {"task_id": task_id, "step_id": step_id})
        cancel: asyncio.Event = state["cancel"]
        try:
            result = await state["executor"].execute(
                state["params"], {"__command__": state["command"]}, progress, cancel
            )
            if cancel.is_set():
                await self._report(
                    "task.result",
                    {"task_id": task_id, "step_id": step_id, "status": "cancelled"},
                )
            else:
                await self._report(
                    "task.result",
                    {"task_id": task_id, "step_id": step_id, "status": "success", "result": result},
                )
                logger.info("task %s/%s SUCCESS", task_id, step_id)
        except ExecutionError as exc:
            if exc.code == "EXECUTOR_CANCELLED" or cancel.is_set():
                await self._report(
                    "task.result", {"task_id": task_id, "step_id": step_id, "status": "cancelled"}
                )
            else:
                await self._report(
                    "task.result",
                    {
                        "task_id": task_id, "step_id": step_id, "status": "failed",
                        "error": {"code": exc.code, "message": exc.message},
                    },
                )
                logger.error("task %s/%s FAILED (%s)", task_id, step_id, exc.code)
        except Exception as exc:  # defensive: never let the consumer die
            logger.exception("executor crashed")
            await self._report(
                "task.result",
                {
                    "task_id": task_id, "step_id": step_id, "status": "failed",
                    "error": {"code": "EXECUTOR_FAILED", "message": str(exc)[:500]},
                },
            )

    # ------------------------------------------------------------------ report

    async def _report(self, msg_type: str, data: dict) -> None:
        if self._ws_client is None:
            logger.error("cannot report %s: no websocket connection", msg_type)
            return
        envelope = protocol.build_envelope(msg_type, data)
        try:
            await self._ws_client.send(envelope)
        except Exception as exc:
            print(f"[worker] report {msg_type} failed: {exc}", file=sys.stderr)
