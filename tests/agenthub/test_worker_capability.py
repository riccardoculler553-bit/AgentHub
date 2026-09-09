"""Worker Capability Manager unit tests (V1.4 Phase 2+3, §11/§17/§18/§20/§51/§71).

Pure client-side tests: no server involved. Package bytes are synthesized
in-process and the puller is faked.
"""

import asyncio
import hashlib
import io
import json
import zipfile

import pytest

from worker.capability.cache import (
    MARKER_FILE,
    CapabilityCache,
    InstallFailed,
    InvalidPackage,
    sha256_bytes,
)
from worker.capability.local_registry import scan_installed
from worker.capability.manifest import parse_manifest, parse_manifest_bytes
from worker.capability.manager import CapabilityInstallError, CapabilityManager
from worker.capability.puller import ChecksumFailed, DownloadFailed


# --------------------------------------------------------------------- helpers


def build_package(name: str = "a.b.c", version: str = "1.0.0", runtime: str = "python",
                  files: dict | None = None, manifest_override: dict | None = None) -> bytes:
    manifest = manifest_override if manifest_override is not None else {
        "name": name, "version": version, "runtime": runtime, "entrypoint": "main",
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("manifest.json", json.dumps(manifest, ensure_ascii=False))
        for fname, content in (files if files is not None else {"main.py": "print('hi')"}).items():
            zf.writestr(fname, content)
    return buf.getvalue()


class FakePuller:
    """Stand-in for PackagePuller; records calls, optionally injects failure."""

    def __init__(self, payload: bytes | Exception = b"", delay: float = 0.0) -> None:
        self.payload = payload
        self.delay = delay
        self.calls: list[tuple[str, str | None]] = []

    async def download(self, package_id: str, checksum: str | None = None) -> bytes:
        self.calls.append((package_id, checksum))
        if self.delay:
            await asyncio.sleep(self.delay)
        if isinstance(self.payload, Exception):
            raise self.payload
        return self.payload


# -------------------------------------------------------------------- manifest


def test_manifest_parse_valid():
    manifest = parse_manifest({"name": "amazon.order.download", "version": "1.2.0", "runtime": "yingdao"})
    assert manifest.name == "amazon.order.download"
    assert manifest.version == "1.2.0"
    assert manifest.runtime == "yingdao"
    assert manifest.entrypoint == "main"


@pytest.mark.parametrize("bad", [
    {"name": "Bad.Name", "version": "1.0.0", "runtime": "python"},      # uppercase
    {"name": "a.b", "version": "1.0.0", "runtime": "python"},           # too few segments
    {"name": "a.b.c", "version": "1.0", "runtime": "python"},           # not semver
    {"name": "a.b.c", "version": "1.0.0", "runtime": "shell"},          # unknown runtime
])
def test_manifest_parse_invalid(bad):
    with pytest.raises(ValueError):
        parse_manifest(bad)


def test_manifest_parse_bytes_bad_json():
    with pytest.raises(ValueError):
        parse_manifest_bytes(b"not json")


# ----------------------------------------------------------------------- cache


def test_cache_install_and_load(tmp_path):
    cache = CapabilityCache(tmp_path / "caps")
    data = build_package()
    checksum = sha256_bytes(data)
    installed = cache.install("a.b.c", "1.0.0", checksum, data)

    assert installed.manifest.entrypoint == "main"
    assert (installed.path / "main.py").read_text(encoding="utf-8") == "print('hi')"
    marker = json.loads((installed.path / MARKER_FILE).read_text(encoding="utf-8"))
    assert marker["checksum"] == checksum

    loaded = cache.load("a.b.c", "1.0.0", checksum)
    assert loaded is not None and loaded.path == installed.path
    # Same version + different expected checksum -> cache miss (§20)
    assert cache.load("a.b.c", "1.0.0", "deadbeef") is None
    # Unknown version -> miss
    assert cache.load("a.b.c", "9.9.9", None) is None


def test_cache_rejects_bad_packages(tmp_path):
    cache = CapabilityCache(tmp_path / "caps")
    with pytest.raises(InvalidPackage):  # not a zip
        cache.install("a.b.c", "1.0.0", None, b"garbage")
    with pytest.raises(InvalidPackage):  # manifest name mismatch
        cache.install("a.b.c", "1.0.0", None, build_package(name="x.y.z"))
    with pytest.raises(InvalidPackage):  # manifest version mismatch
        cache.install("a.b.c", "1.0.0", None, build_package(version="2.0.0"))
    with pytest.raises(InvalidPackage):  # zip-slip entry
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("manifest.json", json.dumps(
                {"name": "a.b.c", "version": "1.0.0", "runtime": "python"}))
            zf.writestr("../evil.txt", "boom")
        cache.install("a.b.c", "1.0.0", None, buf.getvalue())


def test_cache_checksum_defaults_to_computed(tmp_path):
    cache = CapabilityCache(tmp_path / "caps")
    data = build_package()
    installed = cache.install("a.b.c", "1.0.0", None, data)
    assert installed.checksum == sha256_bytes(data)


def test_cache_load_corrupt_manifest_is_miss(tmp_path):
    cache = CapabilityCache(tmp_path / "caps")
    installed = cache.install("a.b.c", "1.0.0", "c0ffee", build_package())
    (installed.path / "manifest.json").write_text("broken{", encoding="utf-8")
    assert cache.load("a.b.c", "1.0.0", "c0ffee") is None


# ---------------------------------------------------------------------- puller


@pytest.mark.anyio
async def test_puller_matches_package_puller_contract():
    """The manager duck-types the puller; PackagePuller must expose download()."""
    from worker.capability.puller import PackagePuller

    puller = PackagePuller("http://server", "tok")
    assert puller.url("pkg_1").endswith("/api/capability-packages/pkg_1/download")
    assert puller._headers() == {"Authorization": "Bearer tok"}
    assert DownloadFailed is not None  # import sanity


# --------------------------------------------------------------------- manager


@pytest.mark.anyio
async def test_ensure_pulls_once_then_reuses_cache(tmp_path):
    data = build_package()
    checksum = sha256_bytes(data)
    puller = FakePuller(data)
    manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), puller)

    first = await manager.ensure("a.b.c", "1.0.0", "pkg_1", checksum)
    assert puller.calls == [("pkg_1", checksum)]
    assert first.manifest.runtime == "python"

    again = await manager.ensure("a.b.c", "1.0.0", "pkg_1", checksum)
    assert len(puller.calls) == 1  # §20: same version reused
    assert again.path == first.path
    assert manager.states[("a.b.c", "1.0.0")] == "READY"


@pytest.mark.anyio
async def test_concurrent_ensure_pulls_once(tmp_path):
    """§71 package lock: same capability+version pulls exactly once."""
    data = build_package()
    checksum = sha256_bytes(data)
    puller = FakePuller(data, delay=0.05)
    manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), puller)

    results = await asyncio.gather(*(
        manager.ensure("a.b.c", "1.0.0", "pkg_1", checksum) for _ in range(4)
    ))
    assert len(puller.calls) == 1
    assert all(r.path == results[0].path for r in results)


@pytest.mark.anyio
async def test_checksum_mismatch_repulls(tmp_path):
    old = build_package(files={"main.py": "old"})
    new = build_package(files={"main.py": "new"})
    old_sum, new_sum = sha256_bytes(old), sha256_bytes(new)
    cache = CapabilityCache(tmp_path / "caps")
    # A stale install exists from a previous dispatch (different checksum, §20).
    cache.install("a.b.c", "1.0.0", old_sum, old)
    puller = FakePuller(new)
    manager = CapabilityManager(cache, puller)

    installed = await manager.ensure("a.b.c", "1.0.0", "pkg_1", new_sum)
    assert installed.path.joinpath("main.py").read_text(encoding="utf-8") == "new"
    assert installed.checksum == new_sum
    assert len(puller.calls) == 1
    assert manager.states[("a.b.c", "1.0.0")] == "READY"


@pytest.mark.anyio
async def test_ensure_download_failure_code(tmp_path):
    puller = FakePuller(DownloadFailed("boom"))
    manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), puller)
    with pytest.raises(CapabilityInstallError) as exc_info:
        await manager.ensure("a.b.c", "1.0.0", "pkg_1", "0" * 64)
    assert exc_info.value.code == "DOWNLOAD_FAILED"
    assert manager.states[("a.b.c", "1.0.0")] == "NONE"


@pytest.mark.anyio
async def test_ensure_checksum_failure_code(tmp_path):
    puller = FakePuller(ChecksumFailed("mismatch"))
    manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), puller)
    with pytest.raises(CapabilityInstallError) as exc_info:
        await manager.ensure("a.b.c", "1.0.0", "pkg_1", "0" * 64)
    assert exc_info.value.code == "CHECKSUM_FAILED"


@pytest.mark.anyio
async def test_ensure_invalid_package_code(tmp_path):
    puller = FakePuller(b"not a zip")
    manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), puller)
    with pytest.raises(CapabilityInstallError) as exc_info:
        await manager.ensure("a.b.c", "1.0.0", "pkg_1", None)
    assert exc_info.value.code == "INVALID_PACKAGE"


@pytest.mark.anyio
async def test_ensure_install_failure_code(tmp_path, monkeypatch):
    data = build_package()
    puller = FakePuller(data)

    def broken_install(name, version, checksum, zip_bytes):
        raise InstallFailed("disk on fire")

    cache = CapabilityCache(tmp_path / "caps")
    monkeypatch.setattr(cache, "install", broken_install)
    manager = CapabilityManager(cache, puller)
    with pytest.raises(CapabilityInstallError) as exc_info:
        await manager.ensure("a.b.c", "1.0.0", "pkg_1", sha256_bytes(data))
    assert exc_info.value.code == "INSTALL_FAILED"


@pytest.mark.anyio
async def test_ensure_without_package_id_fails(tmp_path):
    manager = CapabilityManager(CapabilityCache(tmp_path / "caps"), FakePuller())
    with pytest.raises(CapabilityInstallError) as exc_info:
        await manager.ensure("a.b.c", "1.0.0", "", None)
    assert exc_info.value.code == "INVALID_PACKAGE"


# -------------------------------------------------------------- local registry


def test_scan_installed_reports_and_skips_corrupt(tmp_path):
    root = tmp_path / "caps"
    cache = CapabilityCache(root)
    cache.install("a.b.c", "1.0.0", "aa", build_package("a.b.c", "1.0.0"))
    cache.install("a.b.c", "1.2.0", "bb", build_package("a.b.c", "1.2.0"))
    cache.install("d.e.f", "0.1.0", "cc", build_package("d.e.f", "0.1.0", runtime="http"))
    # corrupt: manifest deleted
    cache.install("g.h.i", "1.0.0", "dd", build_package("g.h.i", "1.0.0"))
    (root / "g.h.i" / "1.0.0" / "manifest.json").unlink()

    scanned = scan_installed(root)
    assert {"name": "a.b.c", "version": "1.0.0"} in scanned
    assert {"name": "a.b.c", "version": "1.2.0"} in scanned
    assert {"name": "d.e.f", "version": "0.1.0"} in scanned
    assert all(item["name"] != "g.h.i" for item in scanned)


def test_scan_installed_empty_or_missing_root(tmp_path):
    assert scan_installed(tmp_path / "missing") == []
    (tmp_path / "caps").mkdir()
    assert scan_installed(tmp_path / "caps") == []


def test_scan_installed_manifest_name_wins_over_dir(tmp_path):
    """A hand-moved directory (dir name != manifest identity) is skipped."""
    import shutil

    root = tmp_path / "caps"
    cache = CapabilityCache(root)
    cache.install("a.b.c", "1.0.0", "aa", build_package("a.b.c", "1.0.0"))
    moved = root / "renamed.here" / "1.0.0"
    moved.parent.mkdir(parents=True)
    shutil.move(str(root / "a.b.c" / "1.0.0"), str(moved))
    assert scan_installed(root) == []
