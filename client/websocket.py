"""WebSocketClient: manage one device WebSocket session."""

import asyncio
import json
from typing import Awaitable, Callable

import websockets

import protocol

# Close codes that mean "authentication failed permanently"
AUTH_CLOSE_CODES = {4401, 4403}

Handler = Callable[[dict], Awaitable[None]]


class WebSocketClient:
    def __init__(self, server_url: str, token_manager, on_envelope: Handler) -> None:
        http_url = server_url.rstrip("/")
        self.url = http_url.replace("http://", "ws://").replace("https://", "wss://") + "/api/ws/device"
        self.token_manager = token_manager
        self.on_envelope = on_envelope
        self.ws = None
        self._recv_task: asyncio.Task | None = None
        self.closed_event = asyncio.Event()
        self.close_code: int | None = None

    async def connect(self) -> None:
        self.ws = await websockets.connect(
            self.url,
            additional_headers=self.token_manager.auth_headers(),
            ping_interval=20,
            ping_timeout=10,
            close_timeout=2,
            open_timeout=30,
        )
        self.close_code = None
        self.closed_event.clear()
        self._recv_task = asyncio.create_task(self._recv_loop())

    async def _recv_loop(self) -> None:
        try:
            async for raw in self.ws:
                try:
                    envelope = protocol.parse_envelope(raw)
                except protocol.ProtocolError:
                    continue
                await self.on_envelope(envelope)
        except websockets.ConnectionClosed as exc:
            self.close_code = exc.rcvd.code if exc.rcvd else None
        finally:
            self.closed_event.set()

    async def send(self, envelope: dict) -> None:
        if self.ws is None:
            raise RuntimeError("websocket is not connected")
        await self.ws.send(protocol.build_envelope(envelope["type"], envelope.get("data"), envelope.get("id"))["__raw__"]
                           if False else __import__("json").dumps(envelope))

    async def close(self) -> None:
        if self._recv_task:
            self._recv_task.cancel()
        if self.ws is not None:
            try:
                await self.ws.close()
            except Exception:
                pass
            self.ws = None
        self.closed_event.set()

    async def wait_closed(self) -> None:
        await self.closed_event.wait()
