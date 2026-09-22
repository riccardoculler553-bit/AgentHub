"""AgentHub worker bootstrap (V1.7 doc §15-§20).

One-command enrollment for fresh machines (stdlib only - no worker deps):

    python bootstrap.py --server http://host:8000 --token <enrollment-token> \
        [--device-name NAME] [--install-dir C:\\ProgramData\\AgentHub] [--download-worker]

Steps (doc §17):
1. register:  POST /api/devices/register (enrollment token -> device_id + device_token)
2. identity:  saved via the client checkout's storage.py when importable
              (preferred), else mirrored exactly at <install-dir>/config/device.json
3. --download-worker: GET /api/bootstrap/latest and extract the bundle into
              <install-dir>/worker
"""

from __future__ import annotations

import argparse
import io
import json
import os
import platform
import shutil
import socket
import sys
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

try:
    from .discovery import discover  # imported as the bootstrap package ...
except ImportError:  # ... or run directly: python client/bootstrap/bootstrap.py
    from discovery import discover  # type: ignore[no-redef]

BOOTSTRAP_CLIENT_VERSION = "1.7.0"

# Default install root; mirrors the ops scripts (scripts/install_worker_service.ps1).
DEFAULT_INSTALL_DIR = Path(os.getenv("PROGRAMDATA", str(Path.home()))) / "AgentHub"

# Registration error codes that mean "the token is wrong/expired/used" (§17).
_ENROLLMENT_ERROR_CODES = {
    "registration_code_invalid",
    "registration_code_expired",
    "registration_code_used",
}


class BootstrapError(RuntimeError):
    """Fatal bootstrap failure with an operator-actionable message."""


def _join_url(server_url: str, path: str) -> str:
    """Join a base URL and an API path (pure helper; unit-tested)."""
    return f"{server_url.rstrip('/')}{path}"


def _registration_error(exc: urllib.error.HTTPError) -> BootstrapError:
    """Map registration HTTP failures to actionable messages."""
    try:
        detail = json.loads(exc.read().decode("utf-8")).get("detail", {})
    except (ValueError, OSError):
        detail = {}
    code = detail.get("code", "") if isinstance(detail, dict) else ""
    if code in _ENROLLMENT_ERROR_CODES or exc.code == 401:
        return BootstrapError(
            "enrollment token invalid or expired - ask the admin for a fresh "
            "token (POST /api/device-enrollment/tokens)"
        )
    message = detail.get("message", str(exc)) if isinstance(detail, dict) else str(exc)
    return BootstrapError(f"registration failed (HTTP {exc.code}): {message}")


def register_device(server_url: str, token: str, device_name: str | None = None) -> dict:
    """POST /api/devices/register. Returns {"device_id", "device_token"}."""
    body = json.dumps(
        {
            "registration_code": token.strip().upper(),
            "device_name": device_name or "",
            "hostname": socket.gethostname(),
            "platform": platform.system().lower() or "unknown",
            "client_version": BOOTSTRAP_CLIENT_VERSION,
        }
    ).encode("utf-8")
    request = urllib.request.Request(
        _join_url(server_url, "/api/devices/register"),
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    print(f"[bootstrap] registering with {server_url} ...")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raise _registration_error(exc) from exc
    except urllib.error.URLError as exc:
        raise BootstrapError(f"cannot reach server at {server_url}: {exc.reason}") from exc
    print(f"[bootstrap] registered: device_id={payload.get('device_id')}")
    return payload


def _import_storage():
    """Import the client checkout's storage module (preferred: keeps the
    identity file name/schema in one place)."""
    try:
        import storage
    except ImportError:
        pass
    else:
        return storage
    client_root = Path(__file__).resolve().parents[1]
    if (client_root / "storage.py").is_file():
        sys.path.insert(0, str(client_root))
        import storage
        return storage
    return None


def save_identity(identity: dict, install_dir: Path) -> Path:
    """Persist {"device_id", "device_token"}.

    Uses the real storage module when importable; otherwise mirrors its
    schema exactly (storage.IDENTITY_FILE = device.json) under
    <install-dir>/config/."""
    storage = _import_storage()
    if storage is not None:
        return storage.save_identity(identity)
    config_dir = install_dir / "config"
    config_dir.mkdir(parents=True, exist_ok=True)
    path = config_dir / "device.json"  # mirrors storage.IDENTITY_FILE
    path.write_text(json.dumps(identity, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _extract_bundle(data: bytes, target_dir: Path) -> None:
    """Extract a worker bundle, replacing any previous worker directory."""
    dest_root = target_dir.resolve()
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for info in zf.infolist():
            if not (target_dir / info.filename).resolve().is_relative_to(dest_root):
                raise BootstrapError(f"unsafe path in worker bundle: {info.filename}")
    if target_dir.exists():
        shutil.rmtree(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        zf.extractall(target_dir)


def download_worker(server_url: str, install_dir: Path, token: str | None = None) -> Path:
    """GET /api/bootstrap/latest and extract the bundle into <install-dir>/worker."""
    request = urllib.request.Request(_join_url(server_url, "/api/bootstrap/latest"))
    if token:
        # The bundle endpoint is admin-guarded; present the token in both
        # accepted header styles so device-token and admin deployments work.
        request.add_header("Authorization", f"Bearer {token}")
        request.add_header("X-Admin-Token", token)
    target_dir = install_dir / "worker"
    print(f"[bootstrap] downloading worker bundle from {request.full_url} ...")
    try:
        with urllib.request.urlopen(request, timeout=300) as response:
            data = response.read()
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            raise BootstrapError("server has no worker bundle (client directory missing)") from exc
        if exc.code == 401:
            raise BootstrapError(
                "worker bundle download unauthorized - /api/bootstrap/latest requires the admin token"
            ) from exc
        raise BootstrapError(f"worker bundle download failed (HTTP {exc.code})") from exc
    except urllib.error.URLError as exc:
        raise BootstrapError(f"cannot reach server at {server_url}: {exc.reason}") from exc
    if not data.startswith(b"PK"):
        raise BootstrapError("downloaded worker bundle is not a ZIP file")
    print(f"[bootstrap] downloaded {len(data)} bytes; extracting to {target_dir} ...")
    _extract_bundle(data, target_dir)
    print(f"[bootstrap] worker {BOOTSTRAP_CLIENT_VERSION} installed at {target_dir}")
    return target_dir


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="AgentHub worker bootstrap (V1.7)")
    parser.add_argument("--server", default=None, help="AgentHub server base URL, e.g. http://host:8000")
    parser.add_argument("--token", required=True, help="enrollment token from POST /api/device-enrollment/tokens")
    parser.add_argument("--device-name", default=None, help="friendly device name")
    parser.add_argument("--install-dir", default=str(DEFAULT_INSTALL_DIR), help="worker install root")
    parser.add_argument("--download-worker", action="store_true", help="also download and extract the worker bundle")
    args = parser.parse_args(argv)

    try:
        server = discover(args.server)
        install_dir = Path(args.install_dir)
        identity = register_device(server, args.token, args.device_name)
        saved = save_identity(
            {"device_id": identity["device_id"], "device_token": identity["device_token"]},
            install_dir,
        )
        print(f"[bootstrap] identity saved to {saved}")
        if args.download_worker:
            download_worker(server, install_dir, token=identity.get("device_token"))
        print("[bootstrap] done")
        return 0
    except BootstrapError as exc:
        print(f"[bootstrap] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
