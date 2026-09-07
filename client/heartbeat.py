"""HeartbeatManager: business heartbeat every N seconds, with ack tracking."""

import asyncio
import sys

import protocol

DEFAULT_INTERVAL = 15.0


class HeartbeatManager:
    def __init__(self, ws_client, interval: float = DEFAULT_INTERVAL) -> None:
        self.ws_client = ws_client
        self.interval = interval
        self.seq = 0
        self.pending: set[str] = set()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(self.interval)
            if self.ws_client.ws is None:
                continue
            # If more than 2 heartbeats are unacked, the connection is probably dead
            if len(self.pending) >= 3:
                print("[heartbeat] 3 unacked heartbeats, forcing reconnect", file=sys.stderr)
                await self.ws_client.close()
                return
            self.seq += 1
            envelope = protocol.build_envelope("heartbeat", {"seq": self.seq})
            self.pending.add(envelope["id"])
            try:
                await self.ws_client.send(envelope)
            except Exception:
                return

    def handle_ack(self, envelope: dict) -> None:
        self.pending.discard(envelope.get("id"))
