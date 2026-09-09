"""PackagePuller: HTTP package download for Lazy Pull (V1.4 §18/§19/§51/§66).

DeviceLink (WebSocket) carries control + results; HTTP carries bytes:

    GET /api/capability-packages/{package_id}/download   (Bearer device token)

Retries: initial attempt + 2 retries with short backoff (§67). A checksum
mismatch does NOT retry - re-downloading identical bytes cannot fix it.
"""

import asyncio
import hashlib

import httpx

MAX_RETRIES = 2  # retries AFTER the first attempt (§67: 最多重试 2 次)
DOWNLOAD_TIMEOUT = 60.0
# Permanent failures: retrying cannot change the outcome.
_NO_RETRY_STATUS = {401, 403, 404}


class DownloadFailed(Exception):
    """Download exhausted its retries (§53 DOWNLOAD_FAILED)."""


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

    async def download(self, package_id: str, checksum: str | None = None) -> bytes:
        last_error = "unknown error"
        for attempt in range(1 + MAX_RETRIES):
            try:
                async with httpx.AsyncClient(timeout=DOWNLOAD_TIMEOUT) as client:
                    response = await client.get(self.url(package_id), headers=self._headers())
                if response.status_code == 200:
                    data = response.content
                    if checksum is not None and hashlib.sha256(data).hexdigest() != checksum:
                        raise ChecksumFailed(
                            f"package {package_id} checksum mismatch "
                            f"(expected {checksum[:12]}..., got {hashlib.sha256(data).hexdigest()[:12]}...)"
                        )
                    return data
                last_error = f"HTTP {response.status_code}"
                if response.status_code in _NO_RETRY_STATUS:
                    break
            except ChecksumFailed:
                raise
            except (httpx.HTTPError, OSError) as exc:
                last_error = str(exc)
            await asyncio.sleep(min(2 ** attempt, 4))
        raise DownloadFailed(f"package {package_id} download failed: {last_error}")
