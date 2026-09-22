"""Bootstrap APIs (V1.7 doc §15-§20, §64).

- GET  /api/bootstrap/latest          admin-guarded worker bundle ZIP of the client tree
- POST /api/device-enrollment/tokens  admin-guarded one-time enrollment token + command
- POST /api/worker-updates/check      device Bearer token OR admin token: update probe
"""

import hashlib
import hmac
import os
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Depends, Header, HTTPException, status
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth.admin import require_admin
from app.auth.token import TokenService
from app.core.config import settings
from app.core.exceptions import DeviceLinkError
from app.db.database import get_db
from app.db.models import Device
from app.registration.service import RegistrationService

router = APIRouter(prefix="/api", tags=["bootstrap"])

# Server-side constant: the worker version this server distributes. The server
# never imports client modules, so it is hardcoded here and must be kept in
# sync with client/bootstrap/bootstrap.py BOOTSTRAP_CLIENT_VERSION.
BOOTSTRAP_WORKER_VERSION = "1.7.0"

# Never ship these directories/files in the bundle (§15).
_EXCLUDED_DIRS = {"__pycache__", ".venv", "logs"}


class EnrollmentTokenIn(BaseModel):
    device_name: str = Field(default="", max_length=100)


class EnrollmentTokenOut(BaseModel):
    enrollment_token: str
    expires_at: datetime
    bootstrap_command: str


class WorkerUpdateCheckIn(BaseModel):
    device_id: str = Field(default="", max_length=64)
    current_version: str = Field(default="", max_length=50)


def _excluded_dir(name: str) -> bool:
    return name in _EXCLUDED_DIRS


def _excluded_file(name: str) -> bool:
    return name.endswith(".pyc") or name.startswith("identity")


def _client_root() -> Path:
    """Repo checkout layout: server/app/api/bootstrap.py -> repo/client."""
    return Path(__file__).resolve().parents[3] / "client"


def _iter_bundle_files(root: Path):
    """Deterministic walk of the client tree with exclusions applied."""
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = sorted(d for d in dirnames if not _excluded_dir(d))
        for name in sorted(filenames):
            if _excluded_file(name):
                continue
            yield Path(dirpath) / name


def _bundle_signature(root: Path) -> str:
    """Cheap change signature (rel path + mtime + size per shipped file).

    The signature walk is far cheaper than zipping, so it keys the bundle
    cache without serving stale content."""
    parts = []
    for path in _iter_bundle_files(root):
        try:
            st = path.stat()
        except OSError:
            continue
        parts.append(f"{path.relative_to(root).as_posix()}:{st.st_mtime_ns}:{st.st_size}")
    return hashlib.sha256("\n".join(sorted(parts)).encode("utf-8")).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _build_bundle(root: Path, dest: Path) -> None:
    """Zip the client tree to a temp file, then atomically replace dest."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=str(dest.parent), prefix=".bundle-", suffix=".zip.tmp")
    os.close(fd)
    try:
        with zipfile.ZipFile(tmp_name, "w", zipfile.ZIP_DEFLATED) as zf:
            for path in _iter_bundle_files(root):
                zf.write(path, path.relative_to(root))
        os.replace(tmp_name, dest)
    except Exception:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise


def ensure_worker_bundle() -> Path:
    """Build (or reuse the cached) worker bundle ZIP under storage/bootstrap."""
    root = _client_root()
    if not root.is_dir():
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"code": "client_bundle_missing", "message": "client directory not found on the server"},
        )
    bundle_dir = settings.storage_dir / "bootstrap"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    dest = bundle_dir / f"agenthub-worker-{BOOTSTRAP_WORKER_VERSION}.zip"
    sig_path = dest.with_suffix(".zip.sig")
    signature = _bundle_signature(root)
    if not (dest.is_file() and sig_path.is_file() and sig_path.read_text(encoding="utf-8").strip() == signature):
        _build_bundle(root, dest)
        sig_path.write_text(signature, encoding="utf-8")
    return dest


@router.get("/bootstrap/latest", dependencies=[Depends(require_admin)])
def download_worker_bundle():
    """V1.7 §15: one-file worker bundle for fresh machines (bootstrap.py).

    Streams the cached ZIP and pins its integrity with an X-Checksum header
    (the updater verifies it before applying, §64)."""
    dest = ensure_worker_bundle()
    return FileResponse(
        dest,
        media_type="application/zip",
        filename=dest.name,
        headers={"X-Checksum": _sha256_file(dest)},
    )


@router.get("/bootstrap/python-runtime", dependencies=[Depends(require_admin)])
def download_python_runtime():
    """Serve the Python installer the admin staged for fresh machines that
    have no Python at all (bootstrap.bat downloads it from here in LAN mode).

    Drop ONE installer file (e.g. python-3.11.9-amd64.exe or the embeddable
    zip) into  <storage>/bootstrap/python/  - the endpoint serves it with a
    checksum header so bootstrap.bat can verify the download."""
    py_dir = settings.storage_dir / "bootstrap" / "python"
    if not py_dir.is_dir():
        raise HTTPException(
            status_code=404,
            detail={
                "code": "python_runtime_missing",
                "message": (
                    "no python runtime staged; ask the admin to place an installer "
                    f"at {py_dir} (python-3.11.9-amd64.exe recommended)"
                ),
            },
        )
    candidates = sorted(
        p for p in py_dir.iterdir()
        if p.is_file() and p.suffix.lower() in (".exe", ".zip") and not p.name.startswith(".")
    )
    if not candidates:
        raise HTTPException(
            status_code=404,
            detail={"code": "python_runtime_missing", "message": f"no .exe/.zip installer found in {py_dir}"},
        )
    dest = candidates[0]
    media = "application/zip" if dest.suffix.lower() == ".zip" else "application/octet-stream"
    return FileResponse(
        dest,
        media_type=media,
        filename=dest.name,
        headers={"X-Checksum": _sha256_file(dest)},
    )


@router.post(
    "/device-enrollment/tokens",
    response_model=EnrollmentTokenOut,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin)],
)
def create_enrollment_token(payload: EnrollmentTokenIn, db: Session = Depends(get_db)):
    """V1.7 §16: enrollment token = one-time registration code plus the
    ready-to-run bootstrap command. Reuses RegistrationService; no duplicate
    code lifecycle logic here."""
    from app.api.registration import get_or_create_default_user

    user = get_or_create_default_user(db)
    row, plaintext = RegistrationService(db).create_code(user_id=user.id)
    db.commit()
    command = f"python bootstrap.py --server {settings.server_public_url} --token {plaintext}"
    if payload.device_name:
        command += f' --device-name "{payload.device_name}"'
    return EnrollmentTokenOut(
        enrollment_token=plaintext,
        expires_at=row.expires_at,
        bootstrap_command=command,
    )


def _device_from_bearer(authorization: str | None, db: Session) -> Device:
    """Bearer device-token auth, mirroring app.api.capability._device_from_bearer (§66)."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail={"code": "token_invalid", "message": "missing Bearer token"})
    token = authorization[len("Bearer "):].strip()
    try:
        token_row = TokenService(db).verify(token)
        device = db.scalars(select(Device).where(Device.device_id == token_row.device_id)).first()
        if device is None or device.revoked_at is not None:
            raise DeviceLinkError("device unavailable")
        return device
    except DeviceLinkError as exc:
        raise HTTPException(status_code=401, detail={"code": "token_invalid", "message": str(exc)}) from exc


def _require_device_or_admin(authorization: str | None, x_admin_token: str | None, db: Session) -> None:
    """Worker update probes accept a device token (worker self-update, §64)
    or the admin token (manual probe from the dashboard/ops shell)."""
    expected = settings.admin_token
    if expected and x_admin_token and hmac.compare_digest(x_admin_token, expected):
        return
    _device_from_bearer(authorization, db)


@router.post("/worker-updates/check")
def check_worker_update(
    payload: WorkerUpdateCheckIn,
    authorization: str | None = Header(default=None),
    x_admin_token: str | None = Header(default=None),
    db: Session = Depends(get_db),
):
    """V1.7 §64: worker update probe (simple version-string compare)."""
    _require_device_or_admin(authorization, x_admin_token, db)
    return {
        "update_available": payload.current_version != BOOTSTRAP_WORKER_VERSION,
        "target_version": BOOTSTRAP_WORKER_VERSION,
        "download_url": "/api/bootstrap/latest",
    }
