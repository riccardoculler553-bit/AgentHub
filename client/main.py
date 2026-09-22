"""DeviceClient entrypoint.

Usage:
    python client/main.py --server http://localhost:8000
    python client/main.py --server http://localhost:8000 --code DL-XXXX-XXXX --name "办公室电脑01"

First run registers (asks for a code if --code is absent); later runs reuse the
saved identity and reconnect automatically.
"""

import argparse
import asyncio
import json
import logging
import logging.handlers
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

_LOG_DIR = Path(__file__).resolve().parent / "logs"


def _setup_logging() -> None:
    """Execution logs (manager/executors) use module loggers; without this
    config Python's default handler swallows everything below WARNING."""
    _LOG_DIR.mkdir(exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(fmt)
    logfile = logging.handlers.RotatingFileHandler(
        _LOG_DIR / "worker.log", maxBytes=2_000_000, backupCount=3, encoding="utf-8"
    )
    logfile.setFormatter(fmt)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.addHandler(console)
    root.addHandler(logfile)


_setup_logging()

import auth as auth_module
import heartbeat as heartbeat_module
import protocol
import registration as registration_module
import reconnect as reconnect_module
import storage
from auth import TokenManager
from heartbeat import HeartbeatManager
from identity import DeviceIdentity, IdentityManager
from reconnect import ReconnectManager
from websocket import AUTH_CLOSE_CODES, WebSocketClient
from worker.capability.cache import CapabilityCache
from worker.capability.downloader import ArtifactDownloader
from worker.capability.local_registry import scan_installed
from worker.capability.manager import CapabilityManager
from worker.capability.puller import PackagePuller
from worker.capability.uploader import ArtifactUploader
from worker.manager import TaskManager
from worker.process import ProcessSupervisor
from worker.registry import reportable_capabilities


class DeviceClient:
    def __init__(self, server_url: str, device_name: str | None = None, registration_code: str | None = None) -> None:
        self.server_url = server_url
        self.device_name = device_name
        self.registration_code = registration_code
        self.identities = IdentityManager()
        self.identity: DeviceIdentity | None = None
        self.reconnector = ReconnectManager()
        # V1.4 Lazy Pull (§18/§51): shared by worker.capabilities reporting and
        # the TaskManager capability path. Token is read lazily because the
        # identity loads before connect.
        self.capability_manager = CapabilityManager(
            CapabilityCache(),
            PackagePuller(server_url, lambda: self.identity.token if self.identity else ""),
        )
        self.task_manager = TaskManager(
            capability_manager=self.capability_manager,
            artifact_uploader=ArtifactUploader(
                server_url, lambda: self.identity.token if self.identity else ""
            ),
            artifact_downloader=ArtifactDownloader(
                server_url, lambda: self.identity.token if self.identity else ""
            ),
        )
        # V1.7 §13: persistent-process supervisor - shares the Lazy Pull
        # pipeline; statuses go out as process.status envelopes.
        self.process_supervisor = ProcessSupervisor(
            self.capability_manager, self._send_process_status
        )
        self._capabilities = reportable_capabilities()
        self._caps_reported = False
        self._stop = asyncio.Event()

    async def _send_process_status(self, payload: dict) -> None:
        """Supervisor -> server status report (data plane: WS control, §14)."""
        if self.ws_client is not None:
            await self.ws_client.send(protocol.build_envelope("process.status", payload["data"]))

    def register(self, code: str | None = None) -> DeviceIdentity:
        if not code:
            code = input("Enter registration code (e.g. DL-7K9X-2MQP): ").strip()
        print(f"[register] registering with code {code} ...")
        result = registration_module.register_device(self.server_url, code, self.device_name)
        self.identity = DeviceIdentity(device_id=result["device_id"], token=result["device_token"])
        self.identities.save(self.identity)
        print(f"[register] registered, device_id={self.identity.device_id}")
        print(f"[register] identity saved to {storage.identity_path()}")
        return self.identity

    async def _on_envelope(self, envelope: dict) -> None:
        msg_type = envelope.get("type")
        if msg_type == "heartbeat_ack":
            self.heartbeats.handle_ack(envelope)
        elif msg_type == "message":
            content = envelope.get("data", {}).get("content", "")
            print(f"[message] received: {content!r} (id={envelope.get('id')})")
            ack = protocol.build_envelope(
                "message_ack", {"success": True}, msg_id=envelope.get("id")
            )
            await self.ws_client.send(ack)
        elif msg_type == "message_ack":
            pass  # transport ack for our own reports
        elif msg_type == "device.connected":
            print(f"[ws] connected to server (device_id={envelope.get('data', {}).get('device_id')})")
            if not self._caps_reported and self._capabilities:
                caps = protocol.build_envelope(
                    "device.capabilities", {"capabilities": self._capabilities}
                )
                await self.ws_client.send(caps)
                self._caps_reported = True
                print(f"[worker] capabilities reported: {[c['name'] for c in self._capabilities]}")
            # V1.4 §17: installed automation capability packages (every connect -
            # Lazy Pull may have changed the local set since the last report).
            installed = scan_installed()
            await self.ws_client.send(
                protocol.build_envelope("worker.capabilities", {"capabilities": installed})
            )
            if installed:
                packages = ", ".join(f"{c['name']}@{c['version']}" for c in installed)
                print(f"[worker] capability packages reported: {packages}")
            # V1.7 §22: environment snapshot (machine/runtime/automation/worker)
            # on every connect - the server diffs fingerprints for drift.
            try:
                from worker.environment import collect

                await self.ws_client.send(
                    protocol.build_envelope("worker.environment", {"environment": collect()})
                )
            except Exception:
                print("[worker] environment report failed")
        elif msg_type == "task.dispatch":
            await self.task_manager.on_dispatch(envelope)
        elif msg_type == "task.cancel":
            await self.task_manager.on_cancel(envelope.get("data", {}))
        elif msg_type == "capability.execute":
            await self.task_manager.on_capability_execute(envelope)
        elif msg_type in ("process.start", "process.stop", "process.restart"):
            # V1.7 §12: persistent-process control plane
            await self._handle_process_message(msg_type, envelope)
        elif msg_type == "error":
            print(f"[ws] server error envelope: {json.dumps(envelope.get('data', {}), ensure_ascii=False)}")
        else:
            print(f"[ws] {msg_type}: {envelope.get('data')}")

    async def _handle_process_message(self, msg_type: str, envelope: dict) -> None:
        data = envelope.get("data", {})
        process_id = str(data.get("process_id", ""))
        try:
            if msg_type == "process.start":
                brief = await self.process_supervisor.start(data)
            elif msg_type == "process.stop":
                brief = await self.process_supervisor.stop(process_id)
            else:
                brief = await self.process_supervisor.restart(process_id)
            print(f"[process] {msg_type} -> {brief.get('status')} ({process_id})")
        except Exception as exc:  # noqa: BLE001 - report the failure, keep the loop alive
            print(f"[process] {msg_type} failed: {exc}")
            await self._send_process_status({
                "data": {"process_id": process_id, "status": "FAILED", "error": str(exc)[:500]}
            })
        finally:
            ack = protocol.build_envelope("message_ack", {"success": True}, msg_id=envelope.get("id"))
            await self.ws_client.send(ack)

    async def run(self) -> None:
        self.identity = self.identities.load()
        if self.identity is None:
            print("[identity] no local identity found, registration required")
            self.register(self.registration_code)
        else:
            print(f"[identity] loaded device_id={self.identity.device_id}")
        self.task_manager.worker_id = self.identity.device_id  # §41 Execution Context

        self.task_manager.ensure_consumer()

        while not self._stop.is_set():
            token_manager = TokenManager(self.identity.token)
            self.ws_client = WebSocketClient(self.server_url, token_manager, self._on_envelope)
            self.task_manager.bind(self.ws_client)
            self._caps_reported = False
            self.heartbeats = HeartbeatManager(self.ws_client, interval=self.heartbeat_interval)

            try:
                await self.ws_client.connect()
            except Exception as exc:
                if token_manager.invalid:
                    print("[ws] authentication failed permanently, stopping", file=sys.stderr)
                    return
                delay = await self.reconnector.wait()
                print(f"[ws] connect failed ({exc}); retrying in {delay:.1f}s (attempt {self.reconnector.attempt})")
                continue

            self.reconnector.reset()
            await self.heartbeats.start()
            print(f"[ws] websocket established: {self.ws_client.url}")
            await self.ws_client.wait_closed()
            await self.heartbeats.stop()

            close_code = self.ws_client.close_code
            if close_code in AUTH_CLOSE_CODES or token_manager.invalid:
                print(f"[ws] closed with auth code {close_code}; not reconnecting", file=sys.stderr)
                return
            delay = await self.reconnector.wait()
            print(f"[ws] connection lost (code={close_code}); reconnecting in {delay:.1f}s")

    heartbeat_interval = heartbeat_module.DEFAULT_INTERVAL

    def request_stop(self) -> None:
        self._stop.set()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="DeviceLink device client")
    parser.add_argument("--server", default="http://localhost:8000", help="server base URL")
    parser.add_argument("--code", default=None, help="one-time registration code (first run)")
    parser.add_argument("--name", default=None, help="device name (registration only)")
    parser.add_argument("--register-only", action="store_true", help="register and exit")
    parser.add_argument("--heartbeat-interval", type=float, default=heartbeat_module.DEFAULT_INTERVAL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    client = DeviceClient(args.server, args.name, args.code)
    client.heartbeat_interval = args.heartbeat_interval

    if args.register_only:
        identity = client.identities.load()
        if identity is not None:
            print(f"[identity] already registered, device_id={identity.device_id}")
            return
        client.register(args.code)
        return

    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        print("\n[client] stopped by user")


if __name__ == "__main__":
    main()
