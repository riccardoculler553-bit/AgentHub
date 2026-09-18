"""ArtifactDownloader: Worker <- Server artifact download over HTTP (V1.5 §13/§31/§54; Phase 6/7 rework).

Control plane stays async and cancel-aware; the data plane is fenced by THREE
independent limits, because a bare per-read timeout cannot see a trickling-dead
connection (2026-09-15: two sub-computers froze forever on GET 200 + body
dripping bytes at ~0 KB/s):

1. httpx stage timeouts (connect/read/write/pool)
2. stall watchdog: no chunk within DEVICELINK_DL_STALL seconds
   -> ArtifactDownloadStalled
3. total watchdog: the whole body must land within DEVICELINK_DL_TOTAL
   seconds (across retries) -> ArtifactDownloadTimeout

The body streams in 1MB chunks - never fully resident in memory - the sha256
is computed incrementally (Phase 7: no full-body hashing on the event loop,
and the old double-hash in the error path is gone), and cache files are
written to ``<name>.part`` then atomically renamed. Cache-hit verification
hashes on a worker thread (asyncio.to_thread) so big cached files can never
block the WebSocket heartbeat again.
"""

import asyncio
import hashlib
import os
from pathlib import Path

import httpx

from worker.capability.cache import work_root

DOWNLOAD_TIMEOUT = 300.0  # per-stage httpx timeout (§67 data plane)
MAX_RETRIES = 2  # retries AFTER the first attempt (§67)
_NO_RETRY_STATUS = {401, 403, 404}
CHUNK_SIZE = 1024 * 1024


def _env_seconds(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return value if value > 0 else default
    except ValueError:
        return default


STALL_TIMEOUT = _env_seconds("DEVICELINK_DL_STALL", 90)
TOTAL_TIMEOUT = _env_seconds("DEVICELINK_DL_TOTAL", 900)


class ArtifactDownloadFailed(Exception):
    """Download exhausted its retries (§53 ARTIFACT_DOWNLOAD_FAILED)."""


class ArtifactDownloadStalled(ArtifactDownloadFailed):
    """Connection alive but bytes stopped flowing (Phase 6 watchdog)."""


class ArtifactDownloadTimeout(ArtifactDownloadFailed):
    """Body did not finish within the total budget (Phase 6 watchdog)."""


class ArtifactDownloadCancelled(Exception):
    """The owning task was cancelled mid-download (Phase 2 cancel channel)."""


class ArtifactChecksumMismatch(Exception):
    """Downloaded bytes do not match the dispatch checksum (§54)."""


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class ArtifactDownloader:
    def __init__(self, server_url: str, token, cache_root: Path | None = None) -> None:
        """token: the device token string, or a zero-arg callable returning it."""
        self.server_url = server_url.rstrip("/")
        self._token = token
        self.cache_root = Path(cache_root) if cache_root else work_root() / "artifact_cache"

    def _headers(self) -> dict:
        token = self._token() if callable(self._token) else self._token
        return {"Authorization": f"Bearer {token}"}

    def url(self, artifact_id: str) -> str:
        return f"{self.server_url}/api/artifacts/{artifact_id}/download"

    def _cache_path(self, artifact_id: str, checksum: str | None) -> Path:
        # Content-addressed when the server told us the checksum (the normal
        # dispatch path); artifact-id keyed otherwise (best effort).
        return self.cache_root / (checksum or artifact_id)

    async def download(self, artifact_id: str, checksum: str | None = None, cancel=None) -> Path:
        """Return a local (cached) copy of the artifact.

        Raises ArtifactDownloadFailed (incl. Stalled/Timeout subclasses),
        ArtifactChecksumMismatch or ArtifactDownloadCancelled."""
        cached = self._cache_path(artifact_id, checksum)
        if cached.is_file():
            # Phase 7: verification hash runs on a thread - a multi-hundred-MB
            # cache hit must never block the heartbeat.
            digest = await asyncio.to_thread(sha256_file, cached)
            if checksum is None or digest == checksum:
                return cached
            try:  # corrupt cache entry: drop and re-download
                cached.unlink()
            except OSError:
                pass

        tmp, digest = await self._fetch_to_cache(artifact_id, cancel)
        if checksum is not None and digest != checksum:
            try:
                tmp.unlink()
            except OSError:
                pass
            raise ArtifactChecksumMismatch(
                f"artifact {artifact_id} checksum mismatch "
                f"(expected {checksum[:12]}..., got {digest[:12]}...)"
            )
        try:
            os.replace(tmp, cached)
        except OSError as exc:
            raise ArtifactDownloadFailed(f"artifact {artifact_id} cache write failed: {exc}") from exc
        return cached

    async def _fetch_to_cache(self, artifact_id: str, cancel) -> tuple[Path, str]:
        """Stream the body into a .part cache file; returns (path, sha256)."""
        last_error = "unknown error"
        overall_started = asyncio.get_running_loop().time()
        self.cache_root.mkdir(parents=True, exist_ok=True)
        for attempt in range(1 + MAX_RETRIES):
            if cancel is not None and cancel.is_set():
                raise ArtifactDownloadCancelled(f"artifact {artifact_id} download cancelled")
            tmp = self.cache_root / f".{artifact_id}.{attempt}.part"
            try:
                async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
                    async with client.stream(
                        "GET", self.url(artifact_id), headers=self._headers()
                    ) as response:
                        if response.status_code != 200:
                            last_error = f"HTTP {response.status_code}"
                            if response.status_code in _NO_RETRY_STATUS:
                                break
                            continue  # transient: backoff + retry below
                        digest = hashlib.sha256()
                        aiter = response.aiter_bytes(CHUNK_SIZE)
                        chunk_task = asyncio.ensure_future(aiter.__anext__())
                        cancel_task = (
                            asyncio.ensure_future(cancel.wait()) if cancel is not None else None
                        )
                        try:
                            with open(tmp, "wb") as fh:
                                while True:
                                    now = asyncio.get_running_loop().time()
                                    if now - overall_started > TOTAL_TIMEOUT:
                                        raise ArtifactDownloadTimeout(
                                            f"artifact {artifact_id} download exceeded "
                                            f"{TOTAL_TIMEOUT:.0f}s total budget"
                                        )
                                    # Race the next chunk against the cancel event:
                                    # a cancel arriving while we are parked on a
                                    # silent socket must abort IMMEDIATELY, not at
                                    # the next byte (Phase 2 cancel channel). The
                                    # total budget rides the same wait - it must
                                    # fire even while no chunk boundary is reached.
                                    wait_set = {chunk_task}
                                    if cancel_task is not None:
                                        wait_set.add(cancel_task)
                                    remaining = TOTAL_TIMEOUT - (now - overall_started)
                                    wait_timeout = max(0.0, min(STALL_TIMEOUT, remaining))
                                    done, _ = await asyncio.wait(
                                        wait_set,
                                        timeout=wait_timeout,
                                        return_when=asyncio.FIRST_COMPLETED,
                                    )
                                    if cancel_task is not None and cancel_task in done:
                                        raise ArtifactDownloadCancelled(
                                            f"artifact {artifact_id} download cancelled"
                                        )
                                    if chunk_task not in done:
                                        if remaining <= STALL_TIMEOUT:
                                            raise ArtifactDownloadTimeout(
                                                f"artifact {artifact_id} download exceeded "
                                                f"{TOTAL_TIMEOUT:.0f}s total budget"
                                            )
                                        raise ArtifactDownloadStalled(
                                            f"artifact {artifact_id} stalled: no bytes for "
                                            f"{STALL_TIMEOUT:.0f}s"
                                        )
                                    try:
                                        chunk = chunk_task.result()
                                    except StopAsyncIteration:
                                        break
                                    digest.update(chunk)
                                    fh.write(chunk)
                                    chunk_task = asyncio.ensure_future(aiter.__anext__())
                        finally:
                            for pending in (chunk_task, cancel_task):
                                if pending is not None and not pending.done():
                                    pending.cancel()
                        return tmp, digest.hexdigest()
            except (ArtifactDownloadStalled, ArtifactDownloadTimeout, ArtifactDownloadCancelled):
                raise  # watchdogs/cancel: retrying cannot fix a dead link
            except (httpx.HTTPError, OSError) as exc:
                last_error = str(exc)
                try:
                    tmp.unlink()
                except OSError:
                    pass
            await asyncio.sleep(min(2**attempt, 4))
        raise ArtifactDownloadFailed(f"artifact {artifact_id} download failed: {last_error}")
