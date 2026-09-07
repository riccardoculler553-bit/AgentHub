"""Shared fake device worker for AgentHub integration tests.

Runs a device WebSocket session inside a thread (TestClient portal pattern),
reports capabilities, and auto-answers task.dispatch envelopes.
"""

import threading

import anyio

# anyio raises EndOfStream when the session streams close; older versions may
# raise ClosedResourceError instead.
_END_OF_STREAM: tuple[type[Exception], ...] = (
    anyio.EndOfStream,
    anyio.ClosedResourceError,
)


class FakeWorker:
    """behaviour: "success" | "fail" | "silent" | "caps_only"."""

    def __init__(
        self,
        client,
        token: str,
        capabilities=("echo",),
        behaviour: str = "success",
        max_dispatches: int = 8,
    ) -> None:
        self.client = client
        self.token = token
        self.capabilities = list(capabilities)
        self.behaviour = behaviour
        self.max_dispatches = max_dispatches
        self.received: list[dict] = []
        self.errors: list[Exception] = []
        self._session = None
        self.thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def join(self, timeout: float = 10) -> None:
        self.thread.join(timeout)

    def stop(self) -> None:
        """Tear down the session so a blocked receive_json() unblocks.

        starlette's WebSocketTestSession.close() only sends websocket.disconnect
        to the app; the session's background task then parks on
        anyio.sleep_forever() and the streams stay open, so receive() would
        block forever. session.__exit__() unwinds the full ExitStack: it sends
        the disconnect, cancels the background task (closing the streams ->
        blocked receives raise EndOfStream) and joins the portal.
        """
        session = self._session
        if session is not None:
            try:
                session.__exit__(None, None, None)
            except Exception:
                pass
        self.join()

    @property
    def dispatches(self) -> list[dict]:
        return [m for m in self.received if m.get("type") == "task.dispatch"]

    def _run(self) -> None:
        from starlette.testclient import WebSocketDisconnect

        session = self.client.websocket_connect(
            "/api/ws/device", headers={"Authorization": f"Bearer {self.token}"}
        )
        self._session = session
        try:
            session.__enter__()
            self.received.append(session.receive_json())  # device.connected
            session.send_json(
                {
                    "id": "cap_1",
                    "type": "device.capabilities",
                    "version": 1,
                    "timestamp": 1,
                    "data": {"capabilities": [{"name": c, "version": "1.0"} for c in self.capabilities]},
                }
            )
            self.received.append(session.receive_json())  # message_ack
            if self.behaviour == "caps_only":
                return
            for _ in range(self.max_dispatches):
                msg = session.receive_json()
                self.received.append(msg)
                if msg.get("type") != "task.dispatch" or self.behaviour == "silent":
                    continue
                self._answer(session, msg["data"])
        except WebSocketDisconnect:
            pass  # expected when stop() closes the session
        except _END_OF_STREAM as exc:
            pass  # streams closed by stop() -> session teardown
        except Exception as exc:
            self.errors.append(exc)
        finally:
            try:
                session.__exit__(None, None, None)
            except Exception:
                pass

    def _answer(self, session, data: dict) -> None:
        task_id, step_id = data["task_id"], data["step_id"]
        attempt_id = data.get("attempt_id", "")

        def send(msg_id: str, msg_type: str, payload: dict) -> None:
            session.send_json({"id": msg_id, "type": msg_type, "version": 1, "timestamp": 1, "data": payload})
            self.received.append(session.receive_json())  # message_ack

        send("a", "task.accept", {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id})
        send("r", "task.running", {"task_id": task_id, "step_id": step_id, "attempt_id": attempt_id})
        if self.behaviour == "fail":
            send(
                "x",
                "task.result",
                {
                    "task_id": task_id, "step_id": step_id, "attempt_id": attempt_id, "status": "failed",
                    "error": {"code": "EXECUTOR_FAILED", "message": "boom"},
                },
            )
        else:
            send(
                "x",
                "task.result",
                {
                    "task_id": task_id, "step_id": step_id, "attempt_id": attempt_id, "status": "success",
                    "result": {"echo": data.get("params", {}).get("message")},
                },
            )


def register_device(client, name: str) -> dict:
    """Create a registration code and register a device; returns the token payload.

    Sends the admin token when one is configured (tests that harden admin auth)."""
    from app.core.config import settings

    headers = {"X-Admin-Token": settings.admin_token} if settings.admin_token else {}
    code = client.post(
        "/api/device-registration", json={"device_name": name}, headers=headers
    ).json()["code"]
    res = client.post(
        "/api/devices/register",
        json={
            "registration_code": code,
            "device_name": name,
            "hostname": "ACCEPT-TEST",
            "platform": "windows",
            "client_version": "1.0.0",
        },
    )
    assert res.status_code == 201, res.text
    return res.json()


def wait_for_capabilities(
    client, device_id: str, names: tuple[str, ...] = ("echo",), timeout: float = 10
) -> bool:
    """Wait until the device's capability report is committed server-side.

    The worker reports capabilities asynchronously; creating a task before the
    report lands fails task validation ("does not report capability").
    """

    def ready() -> bool:
        caps = client.get(f"/api/devices/{device_id}/capabilities").json()
        got = {c["name"] for c in caps.get("capabilities", [])}
        return set(names) <= got

    return wait_until(ready, timeout)


def wait_until(predicate, timeout: float = 12, interval: float = 0.3) -> bool:
    deadline = __import__("time").monotonic() + timeout
    while __import__("time").monotonic() < deadline:
        if predicate():
            return True
        __import__("time").sleep(interval)
    return False
