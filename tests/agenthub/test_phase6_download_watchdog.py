"""Phase 6/7 regression: download watchdogs + data-plane IO isolation.

Case 8 (2026-09-15 production freeze): GET 200 + body trickling bytes forever
must end in a bounded failure, not an eternal wait. Case 11: large transfers
must never starve the event loop (heartbeat).

Uses tiny watchdog budgets (monkeypatched module constants) so tests stay fast.
"""

import asyncio
import hashlib
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from worker.capability import downloader as dl
from worker.capability import uploader as ul
from worker.capability.downloader import (
    ArtifactChecksumMismatch,
    ArtifactDownloadCancelled,
    ArtifactDownloadStalled,
    ArtifactDownloadTimeout,
    ArtifactDownloader,
    sha256_bytes,
)
from worker.capability.uploader import ArtifactUploadFailed, ArtifactUploader


class _HoldHandler(BaseHTTPRequestHandler):
    """GET: 200 + a first byte, then holds the connection (trickle off)."""

    protocol_version = "HTTP/1.1"

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Length", "100")
        self.end_headers()
        try:
            self.wfile.write(b"x")
            self.wfile.flush()
        except Exception:
            pass
        threading.Event().wait(30)  # hold the socket open, send nothing more

    def log_message(self, *args) -> None:
        return None


class _TrickleHandler(BaseHTTPRequestHandler):
    """GET: 200 + 4 bytes every 20ms forever (keeps stall quiet)."""

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Length", "1000000")
        self.end_headers()
        try:
            for _ in range(100000):
                self.wfile.write(b"ABCD")
                self.wfile.flush()
                threading.Event().wait(0.02)
        except Exception:
            pass

    def log_message(self, *args) -> None:
        return None


class _PostHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        self.rfile.read(length)
        body = b'{"artifact_id": "art_new", "name": "n", "type": "file"}'
        self.send_response(201)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        return None


@pytest.fixture
def http_server():
    handler = _HoldHandler
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", server, handler
    server.shutdown()


def _downloader(url: str, tmp_path) -> ArtifactDownloader:
    return ArtifactDownloader(url, "dev-token", cache_root=tmp_path / "cache")


@pytest.mark.anyio
async def test_download_stall_watchdog_fails_fast(http_server, tmp_path, monkeypatch):
    """Case 8: trickling-dead connection -> bounded ARTIFACT_DOWNLOAD_STALLED
    (subclass of ArtifactDownloadFailed), not an eternal silent wait."""
    url, _, _ = http_server
    monkeypatch.setattr(dl, "STALL_TIMEOUT", 0.4)
    downloader = _downloader(url, tmp_path)
    started = time.monotonic()
    with pytest.raises(ArtifactDownloadStalled):
        await downloader.download("art_stall")
    assert time.monotonic() - started < 10  # bounded, not forever


@pytest.mark.anyio
async def test_download_trickle_hits_total_budget(http_server, tmp_path, monkeypatch):
    url, _, handler = http_server
    # swap in the trickle handler: bytes keep coming (stall quiet), but the
    # body never finishes inside the total budget.
    server = ThreadingHTTPServer(("127.0.0.1", 0), _TrickleHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        monkeypatch.setattr(dl, "TOTAL_TIMEOUT", 0.5)
        downloader = _downloader(f"http://127.0.0.1:{server.server_port}", tmp_path)
        with pytest.raises(ArtifactDownloadTimeout):
            await downloader.download("art_trickle")
    finally:
        server.shutdown()
        del handler  # keep the fixture shape explicit


@pytest.mark.anyio
async def test_download_cancel_mid_stream(http_server, tmp_path, monkeypatch):
    url, _, _ = http_server
    monkeypatch.setattr(dl, "STALL_TIMEOUT", 30)
    downloader = _downloader(url, tmp_path)
    cancel = asyncio.Event()

    async def cancel_soon():
        await asyncio.sleep(0.3)
        cancel.set()

    asyncio.get_running_loop().create_task(cancel_soon())
    with pytest.raises(ArtifactDownloadCancelled):
        await downloader.download("art_cancel", cancel=cancel)


@pytest.mark.anyio
async def test_large_download_checksum_and_loop_responsiveness(tmp_path, monkeypatch):
    """Case 11: a multi-MB transfer streams in chunks, the checksum still
    verifies, and the event loop keeps servicing a 50ms ticker throughout
    (no multi-second loop blocks from hashing/writing)."""
    payload = os.urandom(20 * 1024 * 1024)
    checksum = hashlib.sha256(payload).hexdigest()

    class _BigHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            for i in range(0, len(payload), 1024 * 1024):
                self.wfile.write(payload[i : i + 1024 * 1024])
                self.wfile.flush()

        def log_message(self, *args) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), _BigHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        downloader = _downloader(f"http://127.0.0.1:{server.server_port}", tmp_path)
        gaps: list[float] = []

        async def ticker() -> None:
            last = time.monotonic()
            while True:
                await asyncio.sleep(0.05)
                now = time.monotonic()
                gaps.append(now - last)
                last = now

        ticker_task = asyncio.get_running_loop().create_task(ticker())
        try:
            cached = await downloader.download("art_big", checksum=checksum)
        finally:
            ticker_task.cancel()
        assert cached.read_bytes() == payload
        assert sha256_bytes(cached.read_bytes()) == checksum
        assert max(gaps) < 2.0  # the loop never froze for seconds at a time
    finally:
        server.shutdown()


@pytest.mark.anyio
async def test_upload_runs_off_loop_with_total_budget(tmp_path, monkeypatch):
    server = ThreadingHTTPServer(("127.0.0.1", 0), _PostHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        uploader = ArtifactUploader(f"http://127.0.0.1:{server.server_port}", "dev-token")
        path = tmp_path / "out.bin"
        path.write_bytes(b"RESULT-BYTES")
        result = await uploader.upload(path, name="out.bin", task_id="task_1")
        assert result["artifact_id"] == "art_new"

        # Total-budget fence: a hung upload (server holds the request) fails
        # bounded instead of living forever on per-stage timeouts alone.
        monkeypatch.setattr(ul, "UPLOAD_TOTAL_TIMEOUT", 0.3)

        class _SlowPost(BaseHTTPRequestHandler):
            def do_POST(self) -> None:
                threading.Event().wait(10)

            def log_message(self, *args) -> None:
                return None

        slow = ThreadingHTTPServer(("127.0.0.1", 0), _SlowPost)
        slow_thread = threading.Thread(target=slow.serve_forever, daemon=True)
        slow_thread.start()
        try:
            slow_uploader = ArtifactUploader(f"http://127.0.0.1:{slow.server_port}", "dev-token")
            with pytest.raises(ArtifactUploadFailed, match="total budget"):
                await slow_uploader.upload(path, name="out.bin", task_id="task_1")
        finally:
            slow.shutdown()
    finally:
        server.shutdown()


@pytest.mark.anyio
async def test_checksum_mismatch_still_single_request(tmp_path):
    """§54 intact after the Phase 6 rework: body completes, digest mismatch ->
    ArtifactChecksumMismatch with exactly one HTTP request (no retries)."""

    class _CompleteHandler(BaseHTTPRequestHandler):
        requests = 0

        def do_GET(self) -> None:
            type(self).requests += 1
            self.send_response(200)
            self.send_header("Content-Length", "13")
            self.end_headers()
            self.wfile.write(b"PAYLOAD-BYTES")

        def log_message(self, *args) -> None:
            return None

    server = ThreadingHTTPServer(("127.0.0.1", 0), _CompleteHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        downloader = _downloader(f"http://127.0.0.1:{server.server_port}", tmp_path)
        with pytest.raises(ArtifactChecksumMismatch):
            await downloader.download("art_ok", checksum="0" * 64)
        assert _CompleteHandler.requests == 1
    finally:
        server.shutdown()
