"""PackageService: capability package upload/validation/storage (V1.4 §10/§37).

A package is a ZIP containing manifest.json (+ runtime payload). Server:
1. unzip-scan -> manifest.json must exist and parse
2. manifest.name/version must match the upload target
3. SHA256 checksum (§37) recorded for Worker-side verification
4. bytes stored under storage/capability_packages/<package_id>.zip
"""

import hashlib
import io
import zipfile
from pathlib import Path
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.artifact.service import sha256_file
from app.capability_runtime.errors import PackageInvalid, PackageNotFound
from app.capability_runtime.manifest import MANIFEST_FILE, Manifest, parse_manifest_bytes
from app.capability_runtime.db_models import CapabilityPackage
from app.core.config import settings
from app.db.models import utcnow


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


class PackageService:
    def __init__(self, db: Session) -> None:
        self.db = db

    # ---------------------------------------------------------------- storage

    @staticmethod
    def packages_root():
        return settings.storage_dir / "capability_packages"

    def save_package(
        self,
        name: str,
        version: str,
        runtime_type: str,
        zip_bytes: bytes,
    ) -> tuple[CapabilityPackage, Manifest]:
        """Validate + persist an uploaded package. Returns (row, manifest)."""
        manifest = _validate_zip(zip_bytes, name, version)
        package_id = f"pkg_{uuid4().hex[:16]}"
        root = self.packages_root()
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{package_id}.zip"
        path.write_bytes(zip_bytes)
        row = CapabilityPackage(
            package_id=package_id,
            name=name,
            version=version,
            storage_path=str(path.relative_to(settings.storage_dir)),
            size=len(zip_bytes),
            checksum=sha256_bytes(zip_bytes),
            runtime_type=runtime_type,
            created_at=utcnow(),
        )
        self.db.add(row)
        self.db.commit()
        return row, manifest

    def save_package_from_path(
        self,
        name: str,
        version: str,
        runtime_type: str,
        source: Path,
        checksum: str | None = None,
    ) -> tuple[CapabilityPackage, Manifest]:
        """V1.6 P0 0.5: file-backed twin of save_package - the ZIP is renamed
        into the store, never held in memory. `source` must live on the same
        volume as packages_root (the API streams the upload to a temp file
        there first)."""
        manifest = _validate_zip_path(source, name, version)
        package_id = f"pkg_{uuid4().hex[:16]}"
        root = self.packages_root()
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{package_id}.zip"
        source.replace(path)
        row = CapabilityPackage(
            package_id=package_id,
            name=name,
            version=version,
            storage_path=str(path.relative_to(settings.storage_dir)),
            size=path.stat().st_size,
            checksum=checksum or sha256_file(path),
            runtime_type=runtime_type,
            created_at=utcnow(),
        )
        self.db.add(row)
        self.db.commit()
        return row, manifest

    def get_package(self, package_id: str) -> CapabilityPackage:
        row = self.db.scalars(
            select(CapabilityPackage).where(CapabilityPackage.package_id == package_id)
        ).first()
        if row is None:
            raise PackageNotFound(package_id)
        return row

    def read_package_bytes(self, package_id: str) -> bytes:
        row = self.get_package(package_id)
        path = settings.storage_dir / row.storage_path
        try:
            return path.read_bytes()
        except OSError as exc:
            raise PackageNotFound(package_id) from exc


def peek_manifest(zip_bytes: bytes) -> Manifest:
    """Parse + structurally validate the manifest from an upload BEFORE the
    version is known (the manifest itself declares the version). Consistency
    with the upload target is re-checked in save_package."""
    if not zip_bytes:
        raise PackageInvalid("empty upload")
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            return _peek_manifest_from_zipfile(zf)
    except zipfile.BadZipFile as exc:
        raise PackageInvalid("not a valid ZIP archive") from exc


def peek_manifest_path(path: Path) -> Manifest:
    """V1.6 P0 0.5: file-backed peek - validates from disk without loading
    the whole archive into memory."""
    try:
        with zipfile.ZipFile(path) as zf:
            return _peek_manifest_from_zipfile(zf)
    except zipfile.BadZipFile as exc:
        raise PackageInvalid("not a valid ZIP archive") from exc


def _peek_manifest_from_zipfile(zf: zipfile.ZipFile) -> Manifest:
    if MANIFEST_FILE not in zf.namelist():
        raise PackageInvalid(f"missing {MANIFEST_FILE} at the archive root")
    raw = zf.read(MANIFEST_FILE)
    try:
        return parse_manifest_bytes(raw)
    except ValueError as exc:
        raise PackageInvalid(str(exc)) from exc


def _validate_zip(zip_bytes: bytes, expected_name: str, expected_version: str) -> Manifest:
    if not zip_bytes:
        raise PackageInvalid("empty upload")
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            return _validate_zipfile(zf, expected_name, expected_version)
    except zipfile.BadZipFile as exc:
        raise PackageInvalid("not a valid ZIP archive") from exc


def _validate_zip_path(path: Path, expected_name: str, expected_version: str) -> Manifest:
    try:
        with zipfile.ZipFile(path) as zf:
            return _validate_zipfile(zf, expected_name, expected_version)
    except zipfile.BadZipFile as exc:
        raise PackageInvalid("not a valid ZIP archive") from exc


def _validate_zipfile(zf: zipfile.ZipFile, expected_name: str, expected_version: str) -> Manifest:
    names = zf.namelist()
    if MANIFEST_FILE not in names:
        raise PackageInvalid(f"missing {MANIFEST_FILE} at the archive root")
    raw = zf.read(MANIFEST_FILE)
    # Reject path traversal entries (zip-slip defense at upload time).
    for entry in names:
        if entry.startswith("/") or ".." in entry.replace("\\", "/").split("/"):
            raise PackageInvalid(f"unsafe archive entry: {entry}")
    try:
        manifest = parse_manifest_bytes(raw)
    except ValueError as exc:
        raise PackageInvalid(str(exc)) from exc
    if manifest.name != expected_name:
        raise PackageInvalid(
            f"manifest.name {manifest.name!r} != capability name {expected_name!r}"
        )
    if manifest.version != expected_version:
        raise PackageInvalid(
            f"manifest.version {manifest.version!r} != version {expected_version!r}"
        )
    return manifest
