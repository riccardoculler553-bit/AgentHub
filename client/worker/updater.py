"""Worker self-update (V1.7 doc §64, §8 phase 8).

Flow: POST /api/worker-updates/check (device Bearer token or admin token)
-> when an update is available and apply=True, download GET
/api/bootstrap/latest, verify its SHA-256 against the server's X-Checksum
header, extract into <install_dir>/worker.update and atomically swap
worker -> worker.old -> worker.update.

The service restart is manual (doc: 第一版人工触发) - the updater never
restarts anything; it prints "restart the AgentHubWorker service to complete
the update".
"""

import hashlib
import hmac
import os
import shutil
import tempfile
import zipfile
from pathlib import Path

import httpx

try:
    from worker.environment import WORKER_VERSION as CURRENT_VERSION
except ImportError:  # standalone install without the full worker tree
    CURRENT_VERSION = "1.7.0"

UPDATE_CHECK_PATH = "/api/worker-updates/check"
BUNDLE_PATH = "/api/bootstrap/latest"


class UpdateError(RuntimeError):
    """Worker update failed; the currently installed worker is untouched."""


def _sha256_file(path: Path) -> str:
    """SHA-256 of a file, streamed (pure helper; unit-tested)."""
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _auth_headers(token: str) -> dict[str, str]:
    """Present the token in both accepted header styles: the check endpoint
    takes a device Bearer token or the admin token, and the bundle download
    is admin-guarded (open-mode / admin-token deployments)."""
    return {"Authorization": f"Bearer {token}", "X-Admin-Token": token}


def _load_device_id() -> str:
    """Best-effort device_id from the local identity (informational only)."""
    try:
        import storage
    except ImportError:
        return ""
    data = storage.load_identity() or {}
    return data.get("device_id") or ""


def _extract_bundle(zip_path: Path, staging: Path) -> None:
    """Extract the bundle into the staging dir (zip-slip guarded)."""
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    dest_root = staging.resolve()
    with zipfile.ZipFile(zip_path) as zf:
        for info in zf.infolist():
            if not (staging / info.filename).resolve().is_relative_to(dest_root):
                raise UpdateError(f"unsafe path in worker bundle: {info.filename}")
        zf.extractall(staging)


def _swap_worker_dirs(install_dir: Path, staging: Path) -> None:
    """Atomic swap: worker -> worker.old, staging -> worker, drop worker.old.

    Run with the service stopped (V1.7 update is manual, so no files are
    locked); on failure the previous worker directory is rolled back."""
    worker = install_dir / "worker"
    old = install_dir / "worker.old"
    if old.exists():
        shutil.rmtree(old)
    if worker.exists():
        worker.rename(old)
        try:
            staging.rename(worker)
        except OSError:
            old.rename(worker)  # roll back so the previous worker stays intact
            raise
    else:
        staging.rename(worker)
    if old.exists():
        shutil.rmtree(old)


async def _download_bundle(
    client: httpx.AsyncClient,
    server_url: str,
    download_path: str,
    token: str,
) -> Path:
    """Stream the bundle to a temp file and verify the X-Checksum header."""
    url = f"{server_url.rstrip('/')}{download_path}"
    fd, tmp_name = tempfile.mkstemp(prefix="agenthub-worker-", suffix=".zip")
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        async with client.stream("GET", url, headers=_auth_headers(token)) as response:
            response.raise_for_status()
            checksum = response.headers.get("X-Checksum")
            with open(tmp_path, "wb") as fh:
                async for chunk in response.aiter_bytes():
                    fh.write(chunk)
    except BaseException:
        tmp_path.unlink(missing_ok=True)
        raise
    if not checksum:
        tmp_path.unlink(missing_ok=True)
        raise UpdateError("server did not provide an X-Checksum header; refusing to apply the update")
    actual = _sha256_file(tmp_path)
    if not hmac.compare_digest(actual, checksum.lower()):
        tmp_path.unlink(missing_ok=True)
        raise UpdateError(f"worker bundle checksum mismatch: expected {checksum}, got {actual}")
    return tmp_path


async def check_and_apply(server_url: str, token: str, install_dir, apply: bool = False) -> dict:
    """Probe for a worker update and optionally apply it.

    Returns {"update_available": bool, "target_version": str, "applied": bool}.
    Never restarts the service (V1.7: manual trigger) - the operator is told
    to restart the AgentHubWorker service to complete the update."""
    install_dir = Path(install_dir)
    base = server_url.rstrip("/")
    async with httpx.AsyncClient(timeout=30.0) as client:
        check_response = await client.post(
            f"{base}{UPDATE_CHECK_PATH}",
            json={"device_id": _load_device_id(), "current_version": CURRENT_VERSION},
            headers=_auth_headers(token),
        )
        check_response.raise_for_status()
        check = check_response.json()
        result = {
            "update_available": bool(check.get("update_available")),
            "target_version": check.get("target_version", ""),
            "applied": False,
        }
        if not apply or not result["update_available"]:
            return result
        zip_path = await _download_bundle(
            client, base, check.get("download_url", BUNDLE_PATH), token
        )
    try:
        staging = install_dir / "worker.update"
        _extract_bundle(zip_path, staging)
        _swap_worker_dirs(install_dir, staging)
    finally:
        zip_path.unlink(missing_ok=True)
    result["applied"] = True
    print("worker updated: restart the AgentHubWorker service to complete the update")
    return result
