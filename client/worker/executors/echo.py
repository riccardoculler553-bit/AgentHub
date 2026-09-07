"""EchoExecutor: proves the full chain Agent -> Task -> Dispatcher -> Worker.

Nothing fancy: validate a string param, optionally report progress, echo back.
"""

import asyncio

from worker.executor import Executor


class EchoExecutor(Executor):
    name = "echo"

    def validate(self, params: dict) -> None:
        if not isinstance(params.get("message"), str):
            raise ValueError("param 'message' must be a string")

    async def execute(self, params: dict, config: dict, progress, cancel: asyncio.Event) -> dict:
        await progress(50, "echoing")
        if cancel.is_set():
            from worker.executor import ExecutionError

            raise ExecutionError("EXECUTOR_CANCELLED", "cancelled before echo")
        await asyncio.sleep(0.1)
        await progress(100, "done")
        return {"echo": params.get("message", "")}