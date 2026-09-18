"""V1.5 input-artifact chain tests (§13/§15/§16/§26/§31/§35/§54).

Worker side: TaskManager downloads input artifacts into the execution
workspace, injects resolved dirs into params, scans output/ as the upload
fallback. Server side: capability.execute carries input_artifacts references
(artifact_id/name/checksum/role) and the download endpoint enforces auth.
"""

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

import protocol
from worker.capability.cache import CapabilityCache, sha256_bytes
from worker.capability.downloader import (
    ArtifactChecksumMismatch,
    ArtifactDownloadFailed,
    ArtifactDownloader,
)
from worker.capability.manager import CapabilityManager
from worker.ledger import ExecutionLedger
from worker.manager import TaskManager

from .test_worker_capability import build_package
from ._worker import FakeWorker, register_device, wait_until

CAP_NAME = "data.input.demo"
CAP_VERSION = "1.0.0"

# Reads injected input dirs, copies the first data file to output/, and does
# NOT write an artifacts list - exercising the §35 output-scan fallback.
_SCRIPT = (
    "import json, os, shutil\n"
    "params = json.loads(os.environ['CAPABILITY_PARAMS'])\n"
    "exec_dir = os.environ['CAPABILITY_EXECUTION_DIR']\n"
    "data = sorted(os.listdir(params['data_dir']))\n"
    "maps = sorted(os.listdir(params['mapping_dir']))\n"
    "shutil.copyfile(os.path.join(params['data_dir'], data[0]), "
    "os.path.join(params['output_dir'], data[0]))\n"
    "json.dump({'data': data, 'maps': maps}, "
    "open(os.path.join(exec_dir, 'result.json'), 'w'))\n"
)

_MANIFEST = {
    "name": CAP_NAME, "version": CAP_VERSION, "runtime": "python", "entrypoint": "main",
    "inputs": {
        "data_dir": {"type": "artifact_directory", "path": "input", "required": True},
        "mapping_dir": {"type": "artifact_directory", "path": "mapping", "required": True},
    },
    "outputs": {"output_dir": {"type": "artifact_directory", "path": "output"}},
}


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


class FakeDownloader:
    """Serves canned bytes; can inject §33 failures."""

    def __init__(self, files: dict[str, bytes] | None = None, fail: Exception | None = None) -> None:
        self.files = files or {}
        self.fail = fail
        self.calls: list[tuple[str, str | None]] = []

    async def download(self, artifact_id: str, checksum: str | None = None, cancel=None):
        self.calls.append((artifact_id, checksum))
        if self.fail is not None:
            raise self.fail
        import tempfile
        from pathlib import Path

        target = Path(tempfile.gettempdir()) / f"v15_{artifact_id}.bin"
        target.write_bytes(self.files[artifact_id])
        return target


def _make_manager(tmp_path, downloader) -> tuple[TaskManager, FakeWS, FakeUploader]:
    package = build_package(CAP_NAME, CAP_VERSION, manifest_override=dict(_MANIFEST),
                            files={"main.py": _SCRIPT})
    cap_manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), FakePullerShim(package))
    uploader = FakeUploader()
    tm = TaskManager(
        ledger=ExecutionLedger(tmp_path / "worker.db"),
        capability_manager=cap_manager,
        artifact_uploader=uploader,
        artifact_downloader=downloader,
    )
    ws = FakeWS()
    tm.bind(ws)
    tm.worker_id = "worker-1"
    return tm, ws, uploader


class FakePullerShim:
    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.calls: list[tuple[str, str | None]] = []

    async def download(self, package_id: str, checksum: str | None = None) -> bytes:
        self.calls.append((package_id, checksum))
        return self.payload


def _execute_envelope(input_artifacts: list[dict], checksums: dict | None = None) -> dict:
    entries = []
    for entry in input_artifacts:
        item = dict(entry)
        if checksums and item["artifact_id"] in checksums:
            item["checksum"] = checksums[item["artifact_id"]]
        entries.append(item)
    return protocol.build_envelope("capability.execute", {
        "task_id": "task_1", "step_id": "step_1", "attempt_id": "attempt_1",
        "execution_id": "attempt_1", "capability": CAP_NAME, "version": CAP_VERSION,
        "params": {}, "timeout": 120, "package_id": "pkg_1", "checksum": None,
        "input_artifacts": entries,
    })


# ------------------------------------------------------------- TaskManager path


@pytest.mark.anyio
async def test_inputs_downloaded_injected_and_output_scanned(tmp_path):
    downloader = FakeDownloader({"art_data": b"DATA", "art_map": b"MAP"})
    tm, ws, uploader = _make_manager(tmp_path, downloader)
    envelope = _execute_envelope([
        {"artifact_id": "art_data", "name": "data.xlsx", "role": "data_dir"},
        {"artifact_id": "art_map", "name": "mapping.xlsx", "role": "mapping_dir"},
    ])
    await tm.on_capability_execute(envelope)
    await tm._queue.join()

    final = ws.sent[-1]
    assert final["type"] == "capability.result"
    assert final["data"]["status"] == "success", final["data"]
    # §16: dirs injected under the manifest input names - the script saw both
    assert final["data"]["result"]["data"] == {
        "data": ["data.xlsx"], "maps": ["mapping.xlsx"],
    }
    # §35: output/ scan uploaded the copied file
    assert uploader.uploaded[0][0] == "data.xlsx"
    assert uploader.uploaded[0][1].replace("\\", "/").endswith("/output/data.xlsx")
    # downloads happened before execution
    assert [c[0] for c in downloader.calls] == ["art_data", "art_map"]

    # workspace layout (§17): input/, mapping/, output/ live under the
    # execution dir; the uploaded path proves the output/ scan location.
    assert uploader.uploaded[0][1].replace("\\", "/").count("/output/") == 1


@pytest.mark.anyio
async def test_checksum_mismatch_fails_task(tmp_path):
    downloader = FakeDownloader(fail=ArtifactChecksumMismatch("sha mismatch"))
    tm, ws, _ = _make_manager(tmp_path, downloader)
    envelope = _execute_envelope([
        {"artifact_id": "art_data", "name": "data.xlsx", "role": "data_dir"},
        {"artifact_id": "art_map", "name": "mapping.xlsx", "role": "mapping_dir"},
    ])
    await tm.on_capability_execute(envelope)
    await tm._queue.join()

    final = ws.sent[-1]
    assert final["data"]["status"] == "failed"
    assert final["data"]["error"]["code"] == "ARTIFACT_CHECKSUM_MISMATCH"  # §54
    assert tm.ledger.get("attempt_1")["status"] == "FAILED"


@pytest.mark.anyio
async def test_download_failure_fails_task(tmp_path):
    downloader = FakeDownloader(fail=ArtifactDownloadFailed("server unreachable"))
    tm, ws, _ = _make_manager(tmp_path, downloader)
    envelope = _execute_envelope([
        {"artifact_id": "art_data", "name": "data.xlsx", "role": "data_dir"},
    ])
    await tm.on_capability_execute(envelope)
    await tm._queue.join()

    final = ws.sent[-1]
    assert final["data"]["status"] == "failed"
    assert final["data"]["error"]["code"] == "ARTIFACT_DOWNLOAD_FAILED"


@pytest.mark.anyio
async def test_missing_input_reference_rejected(tmp_path):
    """An artifact_id-less entry is invalid params, not a crash (§15)."""
    downloader = FakeDownloader()
    tm, ws, _ = _make_manager(tmp_path, downloader)
    envelope = _execute_envelope([{"name": "data.xlsx", "role": "data_dir"}])
    await tm.on_capability_execute(envelope)
    await tm._queue.join()

    final = ws.sent[-1]
    assert final["data"]["status"] == "failed"
    assert final["data"]["error"]["code"] == "INVALID_PARAMS"


# ------------------------------------------------------- ArtifactDownloader HTTP


class _ArtifactHandler(BaseHTTPRequestHandler):
    store: dict[str, bytes] = {}
    requests = 0

    def do_GET(self) -> None:
        type(self).requests += 1
        if self.headers.get("Authorization", "") != "Bearer dev-token":
            self._status(401)
            return
        # path: /api/artifacts/{artifact_id}/download
        parts = self.path.strip("/").split("/")
        artifact_id = parts[2] if len(parts) >= 3 else ""
        data = type(self).store.get(artifact_id)
        if data is None:
            self._status(404)
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _status(self, code: int) -> None:
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args) -> None:  # silence test output
        return None


@pytest.fixture
def artifact_server():
    _ArtifactHandler.store = {"art_ok": b"PAYLOAD-BYTES"}
    _ArtifactHandler.requests = 0
    server = HTTPServer(("127.0.0.1", 0), _ArtifactHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}"
    server.shutdown()


@pytest.mark.anyio
async def test_downloader_cache_hit_skips_http(tmp_path, artifact_server):
    downloader = ArtifactDownloader(
        artifact_server, "dev-token", cache_root=tmp_path / "cache"
    )
    checksum = sha256_bytes(b"PAYLOAD-BYTES")

    first = await downloader.download("art_ok", checksum=checksum)
    assert first.is_file() and first.read_bytes() == b"PAYLOAD-BYTES"
    assert _ArtifactHandler.requests == 1

    # §31: verified cache hit - no second HTTP round-trip
    second = await downloader.download("art_ok", checksum=checksum)
    assert second == first
    assert _ArtifactHandler.requests == 1


@pytest.mark.anyio
async def test_downloader_checksum_mismatch_no_retry(tmp_path, artifact_server):
    downloader = ArtifactDownloader(
        artifact_server, "dev-token", cache_root=tmp_path / "cache"
    )
    with pytest.raises(ArtifactChecksumMismatch):
        await downloader.download("art_ok", checksum="0" * 64)
    # §54: a checksum mismatch does NOT retry (single request)
    assert _ArtifactHandler.requests == 1


@pytest.mark.anyio
async def test_downloader_404_maps_download_failed(tmp_path, artifact_server):
    downloader = ArtifactDownloader(
        artifact_server, "dev-token", cache_root=tmp_path / "cache"
    )
    with pytest.raises(ArtifactDownloadFailed):
        await downloader.download("art_missing")


@pytest.mark.anyio
async def test_downloader_corrupt_cache_refetches(tmp_path, artifact_server):
    downloader = ArtifactDownloader(
        artifact_server, "dev-token", cache_root=tmp_path / "cache"
    )
    checksum = sha256_bytes(b"PAYLOAD-BYTES")
    cached = await downloader.download("art_ok", checksum=checksum)
    cached.write_bytes(b"CORRUPTED")

    refetched = await downloader.download("art_ok", checksum=checksum)
    assert refetched.read_bytes() == b"PAYLOAD-BYTES"
    assert _ArtifactHandler.requests == 2


# ------------------------------------------------------------- server-side flow


def _publish_capability(client, name: str = CAP_NAME, version: str = CAP_VERSION) -> None:
    res = client.post(
        "/api/capabilities",
        json={"name": name, "runtime_type": "PYTHON", "display_name": "输入演示"},
    )
    assert res.status_code == 201, res.text
    package = build_package(name, version, files={"main.py": "print('ok')"})
    res = client.post(
        f"/api/capabilities/{name}/versions",
        files={"file": (f"{name}-{version}.zip", package, "application/zip")},
    )
    assert res.status_code == 201, res.text
    version_id = res.json()["id"]
    res = client.post(f"/api/capability-versions/{version_id}/publish")
    assert res.status_code == 200, res.text


def _upload_input_artifact(client, token: str, name: str, content: bytes) -> dict:
    res = client.post(
        "/api/artifacts",
        files={"file": (name, content, "application/octet-stream")},
        data={"name": name, "type": "file"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res.status_code == 201, res.text
    return res.json()


def test_dispatch_carries_input_artifacts(client):
    """§26: task.input_artifacts -> WebSocket capability.execute references."""
    _publish_capability(client)
    device = register_device(client, "输入链路测试机")
    token = device["device_token"]

    worker = FakeWorker(client, token, behaviour="success")
    worker.start()
    try:
        uploaded = _upload_input_artifact(client, token, "data.xlsx", b"DATA")
        mapping = _upload_input_artifact(client, token, "mapping.xlsx", b"MAP")

        res = client.post("/api/tasks", json={
            "name": "cap-input-flow",
            "execution_type": "CAPABILITY",
            "capability_version": CAP_VERSION,
            "steps": [{"command": CAP_NAME, "params": {}}],
            "input_artifacts": [
                {"artifact_id": uploaded["artifact_id"], "role": "data_dir"},
                {"artifact_id": mapping["artifact_id"], "role": "mapping_dir"},
            ],
        })
        assert res.status_code == 201, res.text
        task_id = res.json()["task_id"]

        assert wait_until(
            lambda: client.get(f"/api/tasks/{task_id}").json()["status"] == "SUCCESS"
        ), client.get(f"/api/tasks/{task_id}").json()
        assert not worker.errors, worker.errors

        execute = next(m for m in worker.received if m.get("type") == "capability.execute")
        refs = execute["data"]["input_artifacts"]
        assert [r["role"] for r in refs] == ["data_dir", "mapping_dir"]
        assert refs[0]["artifact_id"] == uploaded["artifact_id"]
        assert refs[0]["name"] == "data.xlsx"
        assert refs[0]["checksum"]  # worker verifies against the server checksum
    finally:
        worker.stop()


def test_unknown_input_artifact_rejected_at_creation(client):
    """§15: artifact references are validated at task creation (§33)."""
    _publish_capability(client)
    device = register_device(client, "输入校验测试机")
    res = client.post("/api/tasks", json={
        "name": "cap-bad-input",
        "execution_type": "CAPABILITY",
        "capability_version": CAP_VERSION,
        "steps": [{"command": CAP_NAME, "params": {}}],
        "input_artifacts": [{"artifact_id": "art_missing", "role": "data_dir"}],
    })
    assert res.status_code == 422, res.text
    assert "art_missing" in res.text


def test_artifact_download_requires_device_token(client):
    """§13: the data plane accepts the device Bearer token (or admin)."""
    device = register_device(client, "下载认证测试机")
    token = device["device_token"]
    uploaded = _upload_input_artifact(client, token, "data.xlsx", b"DATA")
    artifact_id = uploaded["artifact_id"]

    assert client.get(f"/api/artifacts/{artifact_id}/download").status_code == 401
    ok = client.get(
        f"/api/artifacts/{artifact_id}/download",
        headers={"Authorization": f"Bearer {token}"},
    )
    assert ok.status_code == 200, ok.text
    assert ok.content == b"DATA"


def test_scan_and_register_local_artifacts(client, tmp_path):
    """Dashboard 数据源注册: scan a server-local dir, register a file as Artifact."""
    import hashlib

    data_dir = tmp_path / "数据目录"
    data_dir.mkdir()
    (data_dir / "数据.xlsx").write_bytes(b"XLSX-BYTES")
    (data_dir / "~$lock.xlsx").write_bytes(b"lock")
    (data_dir / ".hidden").write_bytes(b"h")

    # relative dir rejected
    assert client.get("/api/artifacts/scan", params={"dir": "relative/path"}).status_code == 400
    # missing dir 404
    assert client.get("/api/artifacts/scan", params={"dir": str(tmp_path / "nope")}).status_code == 404

    # scan: first-level files only, lock/hidden skipped
    res = client.get("/api/artifacts/scan", params={"dir": str(data_dir)})
    assert res.status_code == 200, res.text
    assert res.json()["files"] == [{"name": "数据.xlsx", "size": 10}]

    # register-local
    res = client.post(
        "/api/artifacts/register-local",
        json={"dir": str(data_dir), "name": "数据.xlsx"},
    )
    assert res.status_code == 201, res.text
    art = res.json()
    assert art["name"] == "数据.xlsx"
    assert art["checksum"] == hashlib.sha256(b"XLSX-BYTES").hexdigest()

    # traversal via name is neutralized to a bare filename (no escape from dir)
    res = client.post(
        "/api/artifacts/register-local",
        json={"dir": str(data_dir), "name": "../数据.xlsx"},
    )
    assert res.status_code == 201
    assert res.json()["name"] == "数据.xlsx"

    # nonexistent file -> 404
    res = client.post(
        "/api/artifacts/register-local",
        json={"dir": str(data_dir), "name": "不存在.xlsx"},
    )
    assert res.status_code == 404

    # row queryable via admin detail endpoint
    detail = client.get(f"/api/artifacts/{art['artifact_id']}")
    assert detail.status_code == 200
    assert detail.json()["artifact_id"] == art["artifact_id"]


def test_capability_task_timeout_override(client):
    """V1.5: 大任务超时覆盖 — create persists timeout_seconds, detail exposes it."""
    _publish_capability(client)
    res = client.post("/api/tasks", json={
        "name": "cap-timeout",
        "execution_type": "CAPABILITY",
        "capability_version": CAP_VERSION,
        "steps": [{"command": CAP_NAME, "params": {}}],
        "timeout_seconds": 7200,
    })
    assert res.status_code == 201, res.text
    detail = client.get(f"/api/tasks/{res.json()['task_id']}").json()
    assert detail["timeout_seconds"] == 7200

    # out of range rejected
    res = client.post("/api/tasks", json={
        "name": "cap-timeout-bad",
        "execution_type": "CAPABILITY",
        "capability_version": CAP_VERSION,
        "steps": [{"command": CAP_NAME, "params": {}}],
        "timeout_seconds": 10,
    })
    assert res.status_code == 422
