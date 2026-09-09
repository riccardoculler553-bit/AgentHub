"""Worker Capability Runtime executor + TaskManager capability path tests
(V1.4 Phase 4, §12-§16/§41-§45/§65).

Pure client-side tests: real python/yingdao/http runtime executors driven in
process; the TaskManager runs against fake WS/puller/uploader seams.
"""

import asyncio
import json
import threading
import uuid
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlparse

import pytest

import protocol
from worker.capability.cache import CapabilityCache, sha256_bytes
from worker.capability.context import ExecutionContext
from worker.capability.executors import (
    HttpCapabilityExecutor,
    PythonCapabilityExecutor,
    YingdaoCapabilityExecutor,
    create_executor,
)
from worker.capability.manifest import parse_manifest
from worker.capability.manager import CapabilityManager
from worker.ledger import ExecutionLedger
from worker.manager import TaskManager

from .test_worker_capability import FakePuller, build_package


async def _progress(pct: int, message: str) -> None:  # noqa: ARG001
    return None


class FakeWS:
    def __init__(self) -> None:
        self.sent: list[dict] = []

    async def send(self, envelope: dict) -> None:
        self.sent.append(envelope)


class FakeUploader:
    def __init__(self) -> None:
        self.uploaded: list[tuple[str, str]] = []

    async def upload(self, path, *, name, artifact_type="file", task_id="",
                     workflow_run_id=None, step_run_id=None) -> dict:
        self.uploaded.append((name, str(path)))
        return {"artifact_id": f"art_{len(self.uploaded)}", "name": name, "type": artifact_type}


def make_context(tmp_path, *, runtime: str, config: dict | None = None,
                 params: dict | None = None, files: dict | None = None) -> ExecutionContext:
    """Materialize a fake installed package + execution context."""
    exec_id = f"exec_{uuid.uuid4().hex[:8]}"
    package_dir = tmp_path / "pkg" / "x.y.z" / "1.0.0"
    package_dir.mkdir(parents=True, exist_ok=True)
    for fname, content in (files or {}).items():
        (package_dir / fname).write_text(content, encoding="utf-8")
    manifest = parse_manifest({
        "name": "x.y.z", "version": "1.0.0", "runtime": runtime,
        "entrypoint": "main", "config": config or {},
        "inputs": {"count": {"required": True}} if runtime == "python" else {},
    })
    return ExecutionContext(
        execution_id=exec_id, task_id="task_1", step_id="step_1", attempt_id=exec_id,
        capability="x.y.z", version="1.0.0", worker_id="worker-1",
        params={"count": 5} if params is None else params, manifest=manifest, package_dir=package_dir, timeout=60,
    )


# ------------------------------------------------------------------- executors


@pytest.mark.anyio
async def test_python_executor_end_to_end_with_artifact(tmp_path):
    script = (
        "import json, os\n"
        "params = json.loads(os.environ['CAPABILITY_PARAMS'])\n"
        "exec_dir = os.environ['CAPABILITY_EXECUTION_DIR']\n"
        "open(os.path.join(exec_dir, 'orders.txt'), 'w').write(f\"rows={params['count']}\")\n"
        "json.dump({'count': params['count'] + 1, "
        "'artifacts': [{'path': 'orders.txt', 'name': 'orders.txt'}]}, "
        "open(os.path.join(exec_dir, 'result.json'), 'w'))\n"
    )
    context = make_context(tmp_path, runtime="python", files={"main.py": script})
    executor = PythonCapabilityExecutor()
    executor.validate(context.manifest, context.params)

    result = await executor.execute(context, _progress, asyncio.Event())
    assert result.success
    assert result.data == {"count": 6}
    assert len(result.artifact_files) == 1
    name, path = result.artifact_files[0]
    assert name == "orders.txt"
    assert path.is_file() and path.read_text(encoding="utf-8") == "rows=5"


@pytest.mark.anyio
async def test_python_executor_parses_stdout_json(tmp_path):
    script = "import json; print(json.dumps({'count': 9}))\n"
    context = make_context(tmp_path, runtime="python", files={"main.py": script})
    result = await PythonCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert result.success and result.data == {"count": 9}
    assert result.artifact_files == []


@pytest.mark.anyio
async def test_python_executor_nonzero_exit_fails(tmp_path):
    context = make_context(tmp_path, runtime="python", files={"main.py": "raise SystemExit(3)\n"})
    result = await PythonCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert not result.success
    assert result.error_code == "CAPABILITY_EXECUTION_FAILED"
    assert "exit 3" in result.message


@pytest.mark.anyio
async def test_python_executor_cancel_terminates(tmp_path):
    context = make_context(
        tmp_path, runtime="python",
        files={"main.py": (
            "import os, time\n"
            "exec_dir = os.environ['CAPABILITY_EXECUTION_DIR']\n"
            "open(os.path.join(exec_dir, 'started'), 'w').close()\n"
            "time.sleep(30)\n"
        )},
    )
    cancel = asyncio.Event()

    async def cancel_soon():
        try:
            while not (context.execution_dir() / "started").exists():
                await asyncio.sleep(0.02)
        finally:
            cancel.set()

    runner = asyncio.ensure_future(
        PythonCapabilityExecutor().execute(context, _progress, cancel)
    )
    await asyncio.wait_for(cancel_soon(), timeout=15)
    result = await asyncio.wait_for(runner, timeout=15)
    assert not result.success
    assert result.error_code == "CAPABILITY_CANCELLED"


@pytest.mark.anyio
async def test_python_executor_missing_required_param(tmp_path):
    context = make_context(tmp_path, runtime="python", files={"main.py": ""}, params={})
    with pytest.raises(ValueError, match="missing required param: count"):
        PythonCapabilityExecutor().validate(context.manifest, context.params)


@pytest.mark.anyio
async def test_python_executor_artifact_path_escape_is_ignored(tmp_path):
    script = (
        "import json, os\n"
        "exec_dir = os.environ['CAPABILITY_EXECUTION_DIR']\n"
        "json.dump({'artifacts': [{'path': '../../secret.txt'}]}, "
        "open(os.path.join(exec_dir, 'result.json'), 'w'))\n"
    )
    context = make_context(tmp_path, runtime="python", files={"main.py": script})
    result = await PythonCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert result.success and result.artifact_files == []


# ------------------------------------------------------------------------ http


class _EchoHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path.startswith("/error"):
            self._json(500, {"detail": "boom"})
            return
        query = parse_qs(urlparse(self.path).query)
        self._json(200, {"echo": {k: v[0] for k, v in query.items()}})

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", 0))
        payload = json.loads(self.rfile.read(length) or b"{}")
        self._json(200, {"posted": payload})

    def _json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:  # silence test output
        return None


@pytest.fixture
def http_server():
    server = HTTPServer(("127.0.0.1", 0), _EchoHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


def _http_context(tmp_path, url: str, method: str = "GET", params: dict | None = None) -> ExecutionContext:
    context = make_context(tmp_path, runtime="http", config={"url": url, "method": method}, params=params)
    context.manifest = parse_manifest({
        "name": "x.y.z", "version": "1.0.0", "runtime": "http",
        "config": {"url": url, "method": method},
    })
    return context


@pytest.mark.anyio
async def test_http_executor_get_params_as_query(tmp_path, http_server):
    context = _http_context(tmp_path, f"{http_server}/api/inventory", params={"date": "2026-09-09"})
    result = await HttpCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert result.success
    assert result.data["echo"]["date"] == "2026-09-09"
    assert result.metrics["http_status"] == 200


@pytest.mark.anyio
async def test_http_executor_post_params_as_body(tmp_path, http_server):
    context = _http_context(tmp_path, f"{http_server}/api/orders", method="POST", params={"n": 2})
    result = await HttpCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert result.success and result.data["posted"] == {"n": 2}


@pytest.mark.anyio
async def test_http_executor_bad_status_fails(tmp_path, http_server):
    context = _http_context(tmp_path, f"{http_server}/error")
    result = await HttpCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert not result.success
    assert result.error_code == "CAPABILITY_EXECUTION_FAILED"
    assert "HTTP 500" in result.message


@pytest.mark.anyio
async def test_http_executor_validate_rejects_bad_config(tmp_path):
    context = make_context(tmp_path, runtime="http", config={}, params={"count": 1})
    with pytest.raises(ValueError, match="url missing"):
        HttpCapabilityExecutor().validate(context.manifest, context.params)
    context.manifest = parse_manifest({
        "name": "x.y.z", "version": "1.0.0", "runtime": "http",
        "config": {"url": "ftp://erp.internal/api", "method": "GET"},
    })
    with pytest.raises(ValueError, match="illegal http url"):
        HttpCapabilityExecutor().validate(context.manifest, context.params)


# --------------------------------------------------------------------- yingdao


@pytest.mark.anyio
async def test_yingdao_executor_delegates(tmp_path, monkeypatch):
    class FakeInner:
        def configure(self, config: dict) -> None:
            self.config = config

        async def execute(self, params, config, progress, cancel) -> dict:
            return {"robot": self.config["robot_uuid"], "params": params}

    monkeypatch.setattr("worker.capability.executors.YingdaoExecutor", FakeInner)
    context = make_context(
        tmp_path, runtime="yingdao",
        config={"robot_uuid": "RB-1", "shadowbot_path": "C:/ShadowBot.exe"}, params={"count": 1},
    )
    result = await YingdaoCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert result.success and result.data["robot"] == "RB-1"


@pytest.mark.anyio
async def test_yingdao_executor_inner_failure_maps_code(tmp_path, monkeypatch):
    from worker.executor import ExecutionError

    class BusyInner:
        def configure(self, config: dict) -> None:
            pass

        async def execute(self, params, config, progress, cancel) -> dict:
            raise ExecutionError("EXECUTOR_BUSY", "running another program")

    monkeypatch.setattr("worker.capability.executors.YingdaoExecutor", BusyInner)
    context = make_context(
        tmp_path, runtime="yingdao",
        config={"robot_uuid": "RB-1", "shadowbot_path": "C:/ShadowBot.exe"},
    )
    result = await YingdaoCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert not result.success
    assert result.retryable is True


def test_yingdao_executor_validate_requires_config(tmp_path):
    context = make_context(tmp_path, runtime="yingdao", config={}, params={"count": 1})
    with pytest.raises(ValueError, match="robot_uuid"):
        YingdaoCapabilityExecutor().validate(context.manifest, context.params)


# -------------------------------------------------------------------- registry


def test_executor_registry_unknown_runtime():
    with pytest.raises(ValueError, match="unsupported capability runtime"):
        create_executor("shell")


# ------------------------------------------------- TaskManager capability path


@pytest.mark.anyio
async def test_task_manager_capability_success_flow(tmp_path):
    package = build_package("x.y.z", "1.0.0", files={"main.py": (
        "import json, os\n"
        "params = json.loads(os.environ['CAPABILITY_PARAMS'])\n"
        "exec_dir = os.environ['CAPABILITY_EXECUTION_DIR']\n"
        "open(os.path.join(exec_dir, 'out.txt'), 'w').write(str(params['count']))\n"
        "json.dump({'doubled': params['count'] * 2, "
        "'artifacts': [{'path': 'out.txt', 'name': 'out.txt'}]}, "
        "open(os.path.join(exec_dir, 'result.json'), 'w'))\n"
    )})
    checksum = sha256_bytes(package)
    cap_manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), FakePuller(package))
    uploader = FakeUploader()
    tm = TaskManager(
        ledger=ExecutionLedger(tmp_path / "worker.db"),
        capability_manager=cap_manager,
        artifact_uploader=uploader,
    )
    ws = FakeWS()
    tm.bind(ws)
    tm.worker_id = "worker-1"

    envelope = protocol.build_envelope("capability.execute", {
        "task_id": "task_1", "step_id": "step_1", "attempt_id": "attempt_1",
        "execution_id": "attempt_1", "capability": "x.y.z", "version": "1.0.0",
        "params": {"count": 4}, "timeout": 60, "package_id": "pkg_1", "checksum": checksum,
    })
    await tm.on_capability_execute(envelope)
    await tm._queue.join()

    types = [e["type"] for e in ws.sent]
    assert types[0] == "capability.accept"
    assert "capability.running" in types
    assert "capability.progress" in types
    assert types[-1] == "capability.result"

    final = ws.sent[-1]["data"]
    assert final["status"] == "success"
    assert final["result"]["success"] is True
    assert final["result"]["data"] == {"doubled": 8}
    assert final["result"]["artifacts"] == [
        {"artifact_id": "art_1", "name": "out.txt", "type": "file"}
    ]
    assert uploader.uploaded[0][0] == "out.txt"
    assert len(cap_manager.puller.calls) == 1  # Lazy Pull happened exactly once

    row = tm.ledger.get("attempt_1")
    assert row["status"] == "SUCCESS"
    assert row["reported"] == 1


@pytest.mark.anyio
async def test_task_manager_capability_invalid_package_fails(tmp_path):
    cap_manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), FakePuller(b"not a zip"))
    tm = TaskManager(
        ledger=ExecutionLedger(tmp_path / "worker.db"),
        capability_manager=cap_manager,
    )
    ws = FakeWS()
    tm.bind(ws)
    envelope = protocol.build_envelope("capability.execute", {
        "task_id": "task_1", "step_id": "step_1", "attempt_id": "attempt_1",
        "capability": "x.y.z", "version": "1.0.0", "params": {}, "package_id": "pkg_1",
    })
    await tm.on_capability_execute(envelope)
    await tm._queue.join()

    final = ws.sent[-1]
    assert final["type"] == "capability.result"
    assert final["data"]["status"] == "failed"
    assert final["data"]["error"]["code"] == "INVALID_PACKAGE"
    assert tm.ledger.get("attempt_1")["status"] == "FAILED"


@pytest.mark.anyio
async def test_task_manager_capability_missing_identity_fails_fast(tmp_path):
    """capability.execute without capability/version fails without executing."""
    tm = TaskManager(ledger=ExecutionLedger(tmp_path / "worker.db"))
    ws = FakeWS()
    tm.bind(ws)
    envelope = protocol.build_envelope("capability.execute", {
        "task_id": "task_1", "step_id": "step_1", "attempt_id": "attempt_1",
    })
    await tm.on_capability_execute(envelope)
    assert ws.sent[-1]["type"] == "capability.result"
    assert ws.sent[-1]["data"]["error"]["code"] == "INVALID_PARAMS"
    assert tm._queue.empty()


@pytest.mark.anyio
async def test_task_manager_capability_duplicate_redelivery(tmp_path):
    """Idempotency: a re-delivered attempt re-reports, never re-executes."""
    package = build_package("x.y.z", "1.0.0", files={"main.py": "print('{\"n\": 1}')\n"})
    checksum = sha256_bytes(package)
    cap_manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), FakePuller(package))
    tm = TaskManager(
        ledger=ExecutionLedger(tmp_path / "worker.db"), capability_manager=cap_manager
    )
    ws = FakeWS()
    tm.bind(ws)
    envelope = protocol.build_envelope("capability.execute", {
        "task_id": "task_1", "step_id": "step_1", "attempt_id": "attempt_1",
        "capability": "x.y.z", "version": "1.0.0", "params": {},
        "package_id": "pkg_1", "checksum": checksum,
    })
    await tm.on_capability_execute(envelope)
    await tm._queue.join()
    results_after_first = [e for e in ws.sent if e["type"] == "capability.result"]
    assert len(results_after_first) == 1

    await tm.on_capability_execute(envelope)  # duplicate redelivery
    results_after_second = [e for e in ws.sent if e["type"] == "capability.result"]
    assert len(results_after_second) == 2
    assert results_after_second[1]["data"]["status"] == "success"
    assert len(cap_manager.puller.calls) == 1  # never re-pulled
    assert len(cap_manager.states) == 1


@pytest.mark.anyio
async def test_flush_reports_capability_result(tmp_path):
    """Reconnect recovery: capability rows flush as capability.result (§65)."""
    ledger = ExecutionLedger(tmp_path / "worker.db")
    ledger.claim("task_9", "step_9", "attempt_9", "capability:x.y.z@1.0.0")
    ledger.mark_running("attempt_9")
    ledger.mark_finished("attempt_9", "SUCCESS", result={"success": True, "data": {"n": 1}})

    tm = TaskManager(ledger=ledger)
    ws = FakeWS()
    tm.bind(ws)
    await asyncio.sleep(0.1)  # let the flush task run

    flushed = [e for e in ws.sent if e["type"] == "capability.result"]
    assert len(flushed) == 1
    assert flushed[0]["data"]["attempt_id"] == "attempt_9"
    assert flushed[0]["data"]["result"] == {"success": True, "data": {"n": 1}}
