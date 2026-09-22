"""CapabilityManager: Lazy Pull orchestration (V1.4 §18/§51/§53/§71).

ensure(capability, version, package_id, checksum) is the single entry the
TaskManager capability path calls before executing a capability task:

    cache hit (same checksum)  -> READY, no network
    cache miss                 -> DOWNLOADING -> pull -> verify -> install -> READY

Failure codes (§53): DOWNLOAD_FAILED / CHECKSUM_FAILED / INVALID_PACKAGE /
INSTALL_FAILED, raised as CapabilityInstallError. Per-(capability, version)
asyncio locks make concurrent dispatches of the same capability pull exactly
once (§71 package lock); business retry/cancel stays with the Task Engine
(§67/§68) - this manager never retries a pull beyond the puller's own budget.
"""

import asyncio
import logging
import threading
from pathlib import Path

from worker.capability.cache import CapabilityCache, Install, InstallFailed, InvalidPackage
from worker.capability.manifest import Manifest
from worker.capability.puller import ChecksumFailed, DownloadFailed, PackagePuller

logger = logging.getLogger(__name__)


class CapabilityInstallError(Exception):
    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(f"{code}: {message}")


class CapabilityManager:
    def __init__(self, cache: CapabilityCache, puller: PackagePuller) -> None:
        self.cache = cache
        self.puller = puller
        # (name, version) -> NONE | DOWNLOADING | INSTALLED | READY (§53)
        self.states: dict[tuple[str, str], str] = {}
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._lock_guard = threading.Lock()

    def _lock_for(self, name: str, version: str) -> asyncio.Lock:
        key = (name, version)
        with self._lock_guard:
            lock = self._locks.get(key)
            if lock is None:
                lock = asyncio.Lock()
                self._locks[key] = lock
            return lock

    async def ensure(
        self,
        name: str,
        version: str,
        package_id: str,
        checksum: str | None = None,
    ) -> Install:
        """Guarantee the package is installed; returns its install info."""
        key = (name, version)
        async with self._lock_for(name, version):
            cached = self.cache.load(name, version, checksum)
            if cached is not None:
                self.states[key] = "READY"
                return cached

            if not package_id:
                self.states[key] = "NONE"
                raise CapabilityInstallError(
                    "INVALID_PACKAGE", f"{name}@{version} not installed and no package_id to pull"
                )
            self.states[key] = "DOWNLOADING"
            try:
                # V1.6 P0 0.6: the puller streams to a file; no whole-archive
                # bytes ever sit in memory.
                zip_path = await self.puller.download(package_id, checksum)
            except ChecksumFailed as exc:
                self.states[key] = "NONE"
                raise CapabilityInstallError("CHECKSUM_FAILED", str(exc)) from exc
            except DownloadFailed as exc:
                self.states[key] = "NONE"
                raise CapabilityInstallError("DOWNLOAD_FAILED", str(exc)) from exc

            try:
                # Extract is blocking FS work; keep the loop responsive.
                installed = await asyncio.to_thread(
                    self.cache.install_from_file, name, version, checksum, zip_path
                )
            except InvalidPackage as exc:
                self.states[key] = "NONE"
                raise CapabilityInstallError("INVALID_PACKAGE", str(exc)) from exc
            except InstallFailed as exc:
                self.states[key] = "NONE"
                raise CapabilityInstallError("INSTALL_FAILED", str(exc)) from exc
            self.states[key] = "READY"
            logger.info("capability %s@%s installed from %s", name, version, package_id)
            return installed
