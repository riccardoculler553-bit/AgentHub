"""ArtifactUploader: Worker -> Server artifact upload over HTTP (V1.4 §30/§64/§66).

DeviceLink (WebSocket) carries control + results; HTTP carries bytes:

    POST /api/artifacts   (multipart, Bearer device token)
      fields: file, name, type, task_id, workflow_run_id, step_run_id

The server dedupes per (task_id, step_run_id, checksum) - §72 idempotency -
so this uploader does not need its own dedup layer. One transport retry.
"""

import asyncio
from pathlib import Path

import httpx

UPLOAD_TIMEOUT = 120.0
ATTEMPTS = 2


class ArtifactUploadFailed(Exception):
    pass


class ArtifactUploader:
    def __init__(self, server_url: str, token) -> None:
        """token: device token string, or a zero-arg callable returning it."""
        self.server_url = server_url.rstrip("/")
        self._token = token

    def _headers(self) -> dict:
        token = self._token() if callable(self._token) else self._token
        return {"Authorization": f"Bearer {token}"}

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
                async with httpx.AsyncClient(timeout=UPLOAD_TIMEOUT) as client:
                    with open(path, "rb") as fh:
                        response = await client.post(
                            f"{self.server_url}/api/artifacts",
                            headers=self._headers(),
                            data=data,
                            files={"file": (name, fh)},
                        )
                if response.status_code in (200, 201):
                    payload = response.json()
                    if isinstance(payload, dict) and payload.get("artifact_id"):
                        return payload
                    last_error = "malformed upload response"
                else:
                    last_error = f"HTTP {response.status_code}"
                    if response.status_code in (401, 403):
                        break  # permanent: retrying cannot help
            except (httpx.HTTPError, OSError) as exc:
                last_error = str(exc)
            await asyncio.sleep(min(2 ** attempt, 4))
        raise ArtifactUploadFailed(f"artifact {name!r} upload failed: {last_error}")
