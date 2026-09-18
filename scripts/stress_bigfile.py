"""Phase 10: big-file transfer stress test (100MB / 500MB / 1GB tiers).

Work order §19: dummy binary -> upload -> download -> checksum, observing
duration, throughput and EVENT-LOOP responsiveness (the ticker's max gap is
the heartbeat proxy: if the loop ever froze on data-plane IO, the gap grows).

Standalone (no server/DB needed):
    python scripts/stress_bigfile.py            # 100MB
    python scripts/stress_bigfile.py --size 500 # 500MB
    python scripts/stress_bigfile.py --size 1024# 1GB
"""

import argparse
import asyncio
import hashlib
import os
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys_path_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
import sys  # noqa: E402

sys.path.insert(0, os.path.join(sys_path_root, "client"))

from worker.capability.downloader import ArtifactDownloader  # noqa: E402
from worker.capability.uploader import ArtifactUploader  # noqa: E402


class _Handler(BaseHTTPRequestHandler):
    payload_digest = ""
    payload_len = 0
    payload_path = ""
    received_digest = None

    def do_GET(self) -> None:
        self.send_response(200)
        self.send_header("Content-Length", str(_Handler.payload_len))
        self.end_headers()
        with open(_Handler.payload_path, "rb") as fh:
            for blk in iter(lambda: fh.read(1024 * 1024), b""):
                self.wfile.write(blk)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        remaining = length
        while remaining > 0:
            data = self.rfile.read(min(1024 * 1024, remaining))
            remaining -= len(data)
        # multipart framing adds ~200 bytes of boundaries/headers; the integrity
        # proof is the download-side checksum, this only guards truncation.
        _Handler.received_len = length
        body = b'{"artifact_id": "art_stress", "name": "stress.bin", "type": "file"}'
        self.send_response(201)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args) -> None:
        return None


async def run_tier(size_mb: int, tmp: str) -> bool:
    payload_digest = hashlib.sha256()
    chunk = os.urandom(1024 * 1024)
    path = os.path.join(tmp, f"stress_{size_mb}MB.bin")
    started = time.monotonic()
    with open(path, "wb") as fh:
        for _ in range(size_mb):
            fh.write(chunk)
            payload_digest.update(chunk)
    print(f"[gen] {size_mb}MB dummy file in {time.monotonic() - started:.1f}s")

    _Handler.payload_digest = payload_digest.hexdigest()
    _Handler.payload_len = os.path.getsize(path)
    _Handler.payload_path = path
    _Handler.received_len = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{server.server_port}"
    ok = True
    try:
        # ---- upload (Phase 7: sync client on a worker thread) ----
        uploader = ArtifactUploader(base, "tok")
        t0 = time.monotonic()
        result = await uploader.upload(path, name="stress.bin", task_id="stress")
        up_s = time.monotonic() - t0
        print(f"[upload] {up_s:.1f}s ({size_mb / max(up_s, 0.001):.0f} MB/s) -> {result['artifact_id']}")
        if not (_Handler.payload_len <= _Handler.received_len < _Handler.payload_len + 4096):
            print(f"[upload] RECEIVED LENGTH SUSPECT: {_Handler.received_len} vs payload {_Handler.payload_len}")
            ok = False

        # ---- download (Phase 6: streaming + watchdogs) ----
        downloader = ArtifactDownloader(base, "tok", cache_root=os.path.join(tmp, "cache"))
        gaps: list[float] = []

        async def ticker() -> None:
            last = time.monotonic()
            while True:
                await asyncio.sleep(0.05)
                now = time.monotonic()
                gaps.append(now - last)
                last = now

        ticker_task = asyncio.get_running_loop().create_task(ticker())
        t0 = time.monotonic()
        cached = await downloader.download("stress", checksum=_Handler.payload_digest)
        dl_s = time.monotonic() - t0
        ticker_task.cancel()
        print(f"[download] {dl_s:.1f}s ({size_mb / max(dl_s, 0.001):.0f} MB/s)")
        verify = hashlib.sha256()
        with open(cached, "rb") as fh:
            for blk in iter(lambda: fh.read(1024 * 1024), b""):
                verify.update(blk)
        if verify.hexdigest() != _Handler.payload_digest:
            print("[download] CHECKSUM MISMATCH")
            ok = False
        worst_gap = max(gaps) if gaps else 0.0
        print(f"[loop] max event-loop gap during download: {worst_gap * 1000:.0f}ms")
        if worst_gap > 2.0:
            print("[loop] EVENT LOOP STARVED (>2s gap) - data plane leaked onto control plane")
            ok = False
        print(f"[{'PASS' if ok else 'FAIL'}] {size_mb}MB tier")
        return ok
    finally:
        server.shutdown()
        os.remove(path)


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=100, help="payload size in MB")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="dl-stress-") as tmp:
        ok = await run_tier(args.size, tmp)
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    asyncio.run(main())
