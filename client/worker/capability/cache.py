"""CapabilityCache: local package install tree (V1.4 §20/§54).

Layout (§54):

    <work_root>/capabilities/<name>/<version>/
        manifest.json
        ...payload...
        .installed.json    <- marker {"checksum": ..., "installed_at": ...}

Reuse rule (§20): a cached version is reused while the marker checksum
matches the dispatch checksum. Missing install, corrupt manifest or a
different checksum all count as cache miss -> re-pull.
"""

import io
import json
import os
import shutil
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from worker.capability.manifest import MANIFEST_FILE, Manifest, parse_manifest_bytes

MARKER_FILE = ".installed.json"


def work_root() -> Path:
    """Worker work tree root: DEVICELINK_WORK_DIR override or <home>/work."""
    override = os.getenv("DEVICELINK_WORK_DIR")
    if override:
        return Path(override)
    import storage

    return storage.identity_dir() / "work"


def capabilities_root() -> Path:
    return work_root() / "capabilities"


class InvalidPackage(Exception):
    """Package bytes failed structural validation (§53 INVALID_PACKAGE)."""


class InstallFailed(Exception):
    """Extract/persist failed on the local filesystem (§53 INSTALL_FAILED)."""


@dataclass
class Install:
    name: str
    version: str
    checksum: str
    path: Path
    manifest: Manifest


def sha256_bytes(data: bytes) -> str:
    import hashlib

    return hashlib.sha256(data).hexdigest()


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Incremental SHA-256 of a file on disk (V1.6 P0 0.6)."""
    import hashlib

    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


class CapabilityCache:
    def __init__(self, root: Path | None = None) -> None:
        self.root = root or capabilities_root()

    # ------------------------------------------------------------------ reads

    def install_dir(self, name: str, version: str) -> Path:
        return self.root / name / version

    def installed_checksum(self, name: str, version: str) -> str | None:
        """Checksum from the install marker, or None when absent/corrupt."""
        try:
            data = json.loads((self.install_dir(name, version) / MARKER_FILE).read_text(encoding="utf-8"))
            value = str(data["checksum"]).strip()
            return value or None
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def manifest(self, name: str, version: str) -> Manifest:
        path = self.install_dir(name, version) / MANIFEST_FILE
        try:
            return parse_manifest_bytes(path.read_bytes())
        except (OSError, ValueError) as exc:
            raise InvalidPackage(f"{name}@{version}: unreadable manifest: {exc}") from exc

    def load(self, name: str, version: str, checksum: str | None = None) -> Install | None:
        """Return the Install when a healthy matching version is cached."""
        marker_checksum = self.installed_checksum(name, version)
        if marker_checksum is None:
            return None
        if checksum is not None and marker_checksum != checksum:
            return None  # §20: checksum mismatch -> re-pull
        try:
            manifest = self.manifest(name, version)
        except InvalidPackage:
            return None
        return Install(name, version, marker_checksum, self.install_dir(name, version), manifest)

    # ----------------------------------------------------------------- writes

    def install(self, name: str, version: str, checksum: str | None, zip_bytes: bytes) -> Install:
        """Validate + extract + mark a package. Caller must hold the per-
        (name, version) package lock (manager guarantees this)."""
        manifest = _validate_zip(zip_bytes, name, version)
        final = self.install_dir(name, version)
        try:
            staging = self._extract_staged(zip_bytes, name, version)
            marker = {
                "name": name,
                "version": version,
                "checksum": checksum or sha256_bytes(zip_bytes),
                "installed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            (staging / MARKER_FILE).write_text(
                json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            if final.exists():
                shutil.rmtree(final)
            final.parent.mkdir(parents=True, exist_ok=True)  # os.replace won't create it
            os.replace(staging, final)
        except OSError as exc:
            raise InstallFailed(f"{name}@{version}: install failed: {exc}") from exc
        return Install(name, version, marker["checksum"], final, manifest)

    def install_from_file(
        self, name: str, version: str, checksum: str | None, zip_path: Path
    ) -> Install:
        """V1.6 P0 0.6: file-backed twin of install - the ZIP is validated and
        extracted straight from disk, never fully held in memory. Caller must
        hold the per-(name, version) package lock (manager guarantees this)."""
        manifest = _validate_zip_path(zip_path, name, version)
        final = self.install_dir(name, version)
        try:
            staging = self._extract_staged_from_path(zip_path, name, version)
            marker = {
                "name": name,
                "version": version,
                "checksum": checksum or sha256_file(zip_path),
                "installed_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
            (staging / MARKER_FILE).write_text(
                json.dumps(marker, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            if final.exists():
                shutil.rmtree(final)
            final.parent.mkdir(parents=True, exist_ok=True)  # os.replace won't create it
            os.replace(staging, final)
        except OSError as exc:
            raise InstallFailed(f"{name}@{version}: install failed: {exc}") from exc
        return Install(name, version, marker["checksum"], final, manifest)

    def _extract_staged(self, zip_bytes: bytes, name: str, version: str) -> Path:
        tmp_root = self.root / ".tmp"
        tmp_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f"{name}.{version}.", dir=str(tmp_root)))
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                zf.extractall(staging)
        except (zipfile.BadZipFile, OSError) as exc:
            shutil.rmtree(staging, ignore_errors=True)
            raise InstallFailed(f"{name}@{version}: extract failed: {exc}") from exc
        return staging

    def _extract_staged_from_path(self, zip_path: Path, name: str, version: str) -> Path:
        tmp_root = self.root / ".tmp"
        tmp_root.mkdir(parents=True, exist_ok=True)
        staging = Path(tempfile.mkdtemp(prefix=f"{name}.{version}.", dir=str(tmp_root)))
        try:
            with zipfile.ZipFile(zip_path) as zf:
                zf.extractall(staging)
        except (zipfile.BadZipFile, OSError) as exc:
            shutil.rmtree(staging, ignore_errors=True)
            raise InstallFailed(f"{name}@{version}: extract failed: {exc}") from exc
        return staging


def _validate_zip(zip_bytes: bytes, expected_name: str, expected_version: str) -> Manifest:
    """Mirror of the server-side upload validation (defense in depth §11)."""
    if not zip_bytes:
        raise InvalidPackage("empty package")
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            return _validate_zipfile(zf, expected_name, expected_version)
    except zipfile.BadZipFile as exc:
        raise InvalidPackage("not a valid ZIP archive") from exc


def _validate_zip_path(zip_path: Path, expected_name: str, expected_version: str) -> Manifest:
    """V1.6 P0 0.6: file-backed twin of _validate_zip."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            return _validate_zipfile(zf, expected_name, expected_version)
    except zipfile.BadZipFile as exc:
        raise InvalidPackage("not a valid ZIP archive") from exc


def _validate_zipfile(zf: zipfile.ZipFile, expected_name: str, expected_version: str) -> Manifest:
    names = zf.namelist()
    if MANIFEST_FILE not in names:
        raise InvalidPackage(f"missing {MANIFEST_FILE} at the archive root")
    raw = zf.read(MANIFEST_FILE)
    for entry in names:  # zip-slip defense
        if entry.startswith("/") or ".." in entry.replace("\\", "/").split("/"):
            raise InvalidPackage(f"unsafe archive entry: {entry}")
    try:
        manifest = parse_manifest_bytes(raw)
    except ValueError as exc:
        raise InvalidPackage(str(exc)) from exc
    if manifest.name != expected_name:
        raise InvalidPackage(f"manifest.name {manifest.name!r} != {expected_name!r}")
    if manifest.version != expected_version:
        raise InvalidPackage(f"manifest.version {manifest.version!r} != {expected_version!r}")
    return manifest
