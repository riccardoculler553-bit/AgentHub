"""PackagePuller: HTTP package download for Lazy Pull (V1.4 §18/§19/§51/§66; Phase 6/7 rework).

Same data-plane fences as ArtifactDownloader: stall + total watchdogs, 1MB
streaming with incremental sha256, .part atomic rename. Retries = initial + 2
backoff (§67); a checksum mismatch does NOT retry (re-downloading the same
bytes cannot fix corruption).

V1.6 P0 0.6: the ZIP is streamed straight to <work_root>/packages/<package_id>.zip
- no b"".join() of the whole archive in memory.
"""

import asyncio
import hashlib
import os
from pathlib import Path

import httpx

from worker.capability.cache import work_root

MAX_RETRIES = 2  # retries AFTER the first attempt (§67: 最多重试 2 次)
DOWNLOAD_TIMEOUT = 60.0
CHUNK_SIZE = 1024 * 1024

# Permanent failures: retrying cannot change the outcome.
_NO_RETRY_STATUS = {401, 403, 404}


def _env_seconds(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return value if value > 0 else default
    except ValueError:
        return default


STALL_TIMEOUT = _env_seconds("DEVICELINK_DL_STALL", 90)
TOTAL_TIMEOUT = _env_seconds("DEVICELINK_DL_TOTAL", 900)


def packages_root() -> Path:
    """Downloaded package ZIP cache (siblings of the extract tree)."""
    return work_root() / "packages"


class DownloadFailed(Exception):
    """Download exhausted its retries (§53 DOWNLOAD_FAILED)."""


class DownloadStalled(DownloadFailed):
    """Connection alive but bytes stopped flowing (Phase 6 watchdog)."""


class DownloadTimeout(DownloadFailed):
    """Body did not finish within the total budget (Phase 6 watchdog)."""


class DownloadCancelled(Exception):
    """Lazy Pull was cancelled mid-download."""


class ChecksumFailed(Exception):
    """Downloaded bytes do not match the dispatch checksum (§37)."""


class PackagePuller:
    def __init__(self, server_url: str, token) -> None:
        """token: the device token string, or a zero-arg callable returning it
        (the client re-creates its TokenManager per connection)."""
        self.server_url = server_url.rstrip("/")
        self._token = token

    def _headers(self) -> dict:
        token = self._token() if callable(self._token) else self._token
        return {"Authorization": f"Bearer {token}"}

    def url(self, package_id: str) -> str:
        return f"{self.server_url}/api/capability-packages/{package_id}/download"

    async def download(self, package_id: str, checksum: str | None = None, cancel=None) -> Path:
        """Stream the package ZIP to <work_root>/packages/<package_id>.zip and
        return its path (V1.6 P0 0.6: no whole-archive bytes in memory)."""
        last_error = "unknown error"
        overall_started = asyncio.get_running_loop().time()
        root = packages_root()
        root.mkdir(parents=True, exist_ok=True)
        final = root / f"{package_id}.zip"
        part = root / f"{package_id}.zip.part"
        for attempt in range(1 + MAX_RETRIES):
            if cancel is not None and cancel.is_set():
                raise DownloadCancelled(f"package {package_id} download cancelled")
            try:
                async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
                    async with client.stream(
                        "GET", self.url(package_id), headers=self._headers()
                    ) as response:
                        if response.status_code != 200:
                            last_error = f"HTTP {response.status_code}"
                            if response.status_code in _NO_RETRY_STATUS:
                                break
                            continue
                        digest = hashlib.sha256()
                        aiter = response.aiter_bytes(CHUNK_SIZE)
                        chunk_task = asyncio.ensure_future(aiter.__anext__())
                        cancel_task = (
                            asyncio.ensure_future(cancel.wait()) if cancel is not None else None
                        )
                        try:
                            with part.open("wb") as out:
                                while True:
                                    now = asyncio.get_running_loop().time()
                                    if now - overall_started > TOTAL_TIMEOUT:
                                        raise DownloadTimeout(
                                            f"package {package_id} download exceeded {TOTAL_TIMEOUT:.0f}s"
                                        )
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
                                        raise DownloadCancelled(f"package {package_id} download cancelled")
                                    if chunk_task not in done:
                                        if remaining <= STALL_TIMEOUT:
                                            raise DownloadTimeout(
                                                f"package {package_id} download exceeded {TOTAL_TIMEOUT:.0f}s"
                                            )
                                        raise DownloadStalled(
                                            f"package {package_id} stalled: no bytes for {STALL_TIMEOUT:.0f}s"
                                        )
                                    try:
                                        chunk = chunk_task.result()
                                    except StopAsyncIteration:
                                        break
                                    digest.update(chunk)
                                    out.write(chunk)
                                    chunk_task = asyncio.ensure_future(aiter.__anext__())
                        finally:
                            for pending in (chunk_task, cancel_task):
                                if pending is not None and not pending.done():
                                    pending.cancel()
                if checksum is not None and digest.hexdigest() != checksum:
                    part.unlink(missing_ok=True)  # corrupt bytes: do not reuse
                    raise ChecksumFailed(
                        f"package {package_id} checksum mismatch "
                        f"(expected {checksum[:12]}..., got {digest.hexdigest()[:12]}...)"
                    )
                os.replace(part, final)  # atomic publish
                return final
            except (DownloadStalled, DownloadTimeout, DownloadCancelled):
                part.unlink(missing_ok=True)
                raise  # watchdogs/cancel: retrying cannot fix a dead link
            except ChecksumFailed:
                part.unlink(missing_ok=True)
                raise  # re-downloading the same bytes cannot fix corruption
            except (httpx.HTTPError, OSError) as exc:
                last_error = str(exc)
                part.unlink(missing_ok=True)
            await asyncio.sleep(min(2**attempt, 4))
        raise DownloadFailed(f"package {package_id} download failed: {last_error}")