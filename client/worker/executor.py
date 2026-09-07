"""Executor base interface + shared execution error.

Every executor follows the same lifecycle:
    validate(params) -> execute(params, config, progress, cancel) -> result dict

Executors are the ONLY place where real local commands are constructed
(subprocess / ShadowBot.exe / Excel API). The Main Agent never sees them.
"""

import asyncio
from collections.abc import Awaitable, Callable

ProgressCallback = Callable[[int, str], Awaitable[None]]


class ExecutionError(Exception):
    """Structured execution failure (error code understood by the Main Agent)."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        self.message = message
        super().__init__(f"{code}: {message}")


class Executor:
    name = "base"

    def configure(self, config: dict) -> None:
        """Receive the per-capability config block from capabilities.json."""

    def validate(self, params: dict) -> None:
        """Raise ValueError on invalid params."""

    async def execute(
        self,
        params: dict,
        config: dict,
        progress: ProgressCallback,
        cancel: asyncio.Event,
    ) -> dict:
        """Run the actual work. Returns a JSON-serializable result dict.

        Raises ExecutionError on structured failure. Must honor `cancel`."""
        raise NotImplementedError
