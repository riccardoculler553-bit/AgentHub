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
    """behaviour: "success" | "fail" | "silent" | "caps_only".

    V1.4: capability.execute envelopes are auto-answered with the capability
    lifecycle (accept -> running -> result); when capability_artifact is set,
    a fake artifact is uploaded over HTTP before the result is reported.
    """

    def __init__(
        self,
        client,
        token: str,
        capabilities=("echo",),
        behaviour: str = "success",
        max_dispatches: int = 8,
        capability_artifact: bytes | None = None,
        installed_capabilities: list[dict] | None = None,
        environment: dict | None = None,
    ) -> None:
        self.client = client
        self.token = token
        self.capabilities = list(capabilities)
        self.behaviour = behaviour
        self.max_dispatches = max_dispatches
        self.capability_artifact = capability_artifact
        # V1.4 §17: installed automation capability packages to report via
        # worker.capabilities, e.g. [{"name": "a.b.c", "version": "1.0.0"}]
        self.installed_capabilities = installed_capabilities
        # V1.7 §22: environment snapshot to report via worker.environment
        self.environment = environment
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
            if self.installed_capabilities is not None:
                session.send_json(
                    {
                        "id": "wcap_1",
                        "type": "worker.capabilities",
                        "version": 1,
                        "timestamp": 1,
                        "data": {"capabilities": list(self.installed_capabilities)},
                    }
                )
                self.received.append(session.receive_json())  # message_ack
            if self.environment is not None:
                session.send_json(
                    {
                        "id": "env_1",
                        "type": "worker.environment",
                        "version": 1,
                        "timestamp": 1,
                        "data": {"environment": dict(self.environment)},
                    }
                )
                self.received.append(session.receive_json())  # message_ack
            if self.behaviour == "caps_only":
                return
            for _ in range(self.max_dispatches):
                msg = session.receive_json()
                self.received.append(msg)
                if self.behaviour == "silent":
                    continue
                if msg.get("type") == "task.dispatch":
                    self._answer(session, msg["data"])
                elif msg.get("type") == "capability.execute":
                    self._answer_capability(session, msg["data"])
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

    def _answer_capability(self, session, data: dict) -> None:
        """V1.4 §65: capability lifecycle over the Task state machine."""
        task_id, step_id = data["task_id"], data["step_id"]
        attempt_id = data.get("attempt_id", "")
        capability = data.get("capability", "")

        def send(msg_id: str, msg_type: str, payload: dict) -> None:
            session.send_json({"id": msg_id, "type": msg_type, "version": 1, "timestamp": 1, "data": payload})
            self.received.append(session.receive_json())  # message_ack

        accept = {
            "task_id": task_id, "step_id": step_id, "attempt_id": attempt_id,
            "capability": capability, "version": data.get("version", ""),
        }
        send("c1", "capability.accept", accept)
        send("c2", "capability.running", dict(accept))
        if self.behaviour == "fail":
            send(
                "c3",
                "capability.result",
                {
                    "task_id": task_id, "step_id": step_id, "attempt_id": attempt_id, "status": "failed",
                    "error": {"code": "CAPABILITY_EXECUTION_FAILED", "message": "capability boom"},
                },
            )
            return
        artifacts = []
        if self.capability_artifact is not None:
            upload = self.client.post(
                "/api/artifacts",
                files={"file": ("report.txt", self.capability_artifact, "text/plain")},
                data={
                    "name": "report.txt",
                    "type": "file",
                    "task_id": task_id,
                    "workflow_run_id": data.get("workflow_run_id") or "",
                    "step_run_id": data.get("step_run_id") or "",
                },
                headers={"Authorization": f"Bearer {self.token}"},
            )
            if upload.status_code == 201:
                body = upload.json()
                artifacts.append({"artifact_id": body["artifact_id"], "name": body["name"], "type": body["type"]})
            else:
                self.errors.append(RuntimeError(f"artifact upload failed: HTTP {upload.status_code}"))
        send(
            "c3",
            "capability.result",
            {
                "task_id": task_id, "step_id": step_id, "attempt_id": attempt_id, "status": "success",
                "result": {"capability": capability, "artifacts": artifacts},
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


def wait_for_worker_capabilities(
    client, device_id: str, names: tuple[str, ...], timeout: float = 10
) -> bool:
    """V1.6 P0 0.10: the resolver reads the NEW worker_capabilities plane and
    ignores arbitrary-online fallbacks - tasks may only be created after the
    installed-capability report is committed."""

    def ready() -> bool:
        for item in client.get("/api/worker-capabilities").json():
            if item["worker_id"] != device_id:
                continue
            got = {c["name"] for c in item["capabilities"]}
            if set(names) <= got:
                return True
        return False

    return wait_until(ready, timeout)


def wait_until(predicate, timeout: float = 12, interval: float = 0.3) -> bool:
    deadline = __import__("time").monotonic() + timeout
    while __import__("time").monotonic() < deadline:
        if predicate():
            return True
        __import__("time").sleep(interval)
    return False
