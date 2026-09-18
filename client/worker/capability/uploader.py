"""ArtifactUploader: Worker -> Server artifact upload over HTTP (V1.4 §30/§64/§66; Phase 6/7 rework).

DeviceLink (WebSocket) carries control + results; HTTP carries bytes:

    POST /api/artifacts   (multipart, Bearer device token)
      fields: file, name, type, task_id, workflow_run_id, step_run_id

Phase 6/7: the whole transfer runs on a worker thread (sync httpx client +
sync file reads) so multi-hundred-MB uploads can never starve the WebSocket
heartbeat, and it is fenced by a TOTAL timeout - the old per-stage timeout
alone let a trickling-dead upload live forever. One transport retry (§72: the
server dedupes, so this uploader needs no dedup layer of its own).
"""

import asyncio
import os
from pathlib import Path

import httpx

UPLOAD_TIMEOUT = 120.0
ATTEMPTS = 2


def _env_seconds(name: str, default: float) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return value if value > 0 else default
    except ValueError:
        return default


UPLOAD_TOTAL_TIMEOUT = _env_seconds("DEVICELINK_UL_TOTAL", 900)


class ArtifactUploadFailed(Exception):
    pass


class ArtifactUploader:
    def __init__(self, server_url: str, token) -> None:
        """token: the device token string, or a zero-arg callable returning it."""
        self.server_url = server_url.rstrip("/")
        self._token = token

    def _headers(self) -> dict:
        token = self._token() if callable(self._token) else self._token
        return {"Authorization": f"Bearer {token}"}

    def _upload_once(self, path: Path, name: str, data: dict) -> dict | None:
        """Blocking single attempt - runs entirely on a worker thread."""
        with httpx.Client(timeout=UPLOAD_TIMEOUT) as client:
            with open(path, "rb") as fh:
                response = client.post(
                    f"{self.server_url}/api/artifacts",
                    headers=self._headers(),
                    data=data,
                    files={"file": (name, fh)},
                )
        if response.status_code in (200, 201):
            payload = response.json()
            if isinstance(payload, dict) and payload.get("artifact_id"):
                return payload
            return None  # malformed upload response
        raise _HTTPStatus(response.status_code, response.text[:200])

    async def upload(
        self,
        path: str | Path,
        *,
        name: str,
        artifact_type: str = "file",
        task_id: str = "",
        workflow_run_id: str | None = None,
        step_run_id: str | None = None,
    ) -> dict:
        """Upload one artifact; returns the server's artifact dict
        (artifact_id/name/type/...). Raises ArtifactUploadFailed."""
        path = Path(path)
        data = {
            "name": name,
            "type": artifact_type,
            "task_id": task_id,
        }
        if workflow_run_id:
            data["workflow_run_id"] = workflow_run_id
        if step_run_id:
            data["step_run_id"] = step_run_id
        last_error = "unknown error"
        for attempt in range(ATTEMPTS):
            try:
                payload = await asyncio.wait_for(
                    asyncio.to_thread(self._upload_once, path, name, data),
                    timeout=UPLOAD_TOTAL_TIMEOUT,
                )
                if payload is not None:
                    return payload
                last_error = "malformed upload response"
            except asyncio.TimeoutError as exc:
                raise ArtifactUploadFailed(
                    f"artifact {name!r} upload exceeded {UPLOAD_TOTAL_TIMEOUT:.0f}s total budget"
                ) from exc
            except _HTTPStatus as exc:
                last_error = f"HTTP {exc.status_code}"
                if exc.status_code in (401, 403):
                    break  # permanent: retrying cannot help
            except (httpx.HTTPError, OSError) as exc:
                last_error = str(exc)
            await asyncio.sleep(min(2**attempt, 4))
        raise ArtifactUploadFailed(f"artifact {name!r} upload failed: {last_error}")


class _HTTPStatus(Exception):
    def __init__(self, status_code: int, text: str) -> None:
        self.status_code = status_code
        self.text = text
