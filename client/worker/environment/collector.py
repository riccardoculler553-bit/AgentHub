"""Worker Environment Inventory (V1.7 §22-§28).

The Worker reports WHAT it can run and HOW it is built:
- machine:  hostname / OS / arch / cpu / ram / disk
- runtime:  python versions available
- automation: yingdao (RPA) detection
- agenthub: worker version

Everything is best-effort: a failed probe yields None, never a crash - the
environment report must not break the worker loop. The fingerprint is a
stable sha256 over the normalized snapshot so the server can detect drift
(Python upgraded overnight, RAM changed, ...) between two reports.
"""

import ctypes
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import hashlib
from pathlib import Path

WORKER_VERSION = "1.7.0"

# cache probed subprocess facts for the process lifetime (they rarely change)
_CACHE: dict[str, object] = {}


def _run(command: list[str], timeout: float = 6.0) -> str | None:
    try:
        out = subprocess.run(
            command, capture_output=True, text=True, timeout=timeout,
            creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0),
        )
        if out.returncode == 0 and out.stdout.strip():
            return out.stdout.strip()
    except (OSError, subprocess.SubprocessError):
        pass
    return None


def _memory_total_gb() -> float | None:
    if os.name == "nt":
        try:
            class MEMORYSTATUSEX(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong),
                    ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong),
                    ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong),
                    ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong),
                    ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            stat = MEMORYSTATUSEX()
            stat.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(stat)):
                return round(stat.ullTotalPhys / (1024 ** 3), 1)
        except Exception:  # noqa: BLE001 - best effort probe
            return None
    try:
        with open("/proc/meminfo", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return round(kb / (1024 * 1024), 1)
    except (OSError, ValueError):
        pass
    return None


def _disk_free_gb() -> float | None:
    try:
        from worker.capability.cache import work_root

        root = work_root()
        root.mkdir(parents=True, exist_ok=True)
        total, _used, free = shutil.disk_usage(str(root))
        return round(free / (1024 ** 3), 1)
    except Exception:  # noqa: BLE001 - best effort probe
        return None


def _python_versions() -> list[str]:
    """Detect installed CPython versions via the Windows launcher and PATH."""
    if "pythons" in _CACHE:
        return _CACHE["pythons"]  # type: ignore[return-value]
    found: set[str] = set()
    listing = _run(["py", "-0p"])
    if listing:
        # lines look like: " -V:3.11 *        C:\Python311\python.exe"
        found.update(re.findall(r"-V:?(\d+\.\d+)", listing))
    probe = _run(["python", "--version"])
    if probe:
        m = re.search(r"(\d+\.\d+)", probe)
        if m:
            found.add(m.group(1))
    _CACHE["pythons"] = sorted(found)
    return _CACHE["pythons"]  # type: ignore[return-value]


def _yingdao() -> dict:
    """RPA runtime detection (V1.7 §25): install dir + process probe."""
    candidates = [
        Path(os.environ.get("PROGRAMFILES", r"C:\Program Files")) / "Yingdao" / "RPA",
        Path(os.environ.get("LOCALAPPDATA", "")) / "Yingdao" / "RPA",
        Path(r"C:\Program Files (x86)\Yingdao"),
    ]
    installed = any(p.is_dir() for p in candidates)
    version = None
    if installed:
        # best-effort: any version-y folder under the install root
        for p in candidates:
            if p.is_dir():
                for child in p.iterdir():
                    m = re.match(r"^(\d+\.\d+)", child.name)
                    if child.is_dir() and m:
                        version = m.group(1)
                        break
            if version:
                break
    return {"installed": installed, "version": version}


def collect() -> dict:
    """Build the environment snapshot (V1.7 §27 shape)."""
    machine = platform.machine() or platform.processor() or "unknown"
    snapshot = {
        "hostname": socket.gethostname(),
        "os": {"name": platform.system() or "unknown", "version": platform.release() or ""},
        "arch": machine,
        "cpu": {"cores": os.cpu_count()},
        "memory": {"total_gb": _memory_total_gb()},
        "disk": {"free_gb": _disk_free_gb()},
        "python": _python_versions(),
        "yingdao": _yingdao(),
        "worker": {"version": WORKER_VERSION},
    }
    snapshot["fingerprint"] = fingerprint(snapshot)
    return snapshot


def fingerprint(snapshot: dict) -> str:
    """Stable sha256 over the drift-relevant subset (V1.7 §28)."""
    relevant = {
        "os": snapshot.get("os"),
        "arch": snapshot.get("arch"),
        "cpu": snapshot.get("cpu"),
        "memory": snapshot.get("memory"),
        "python": snapshot.get("python"),
        "yingdao": snapshot.get("yingdao"),
        "worker": snapshot.get("worker"),
    }
    normalized = json.dumps(relevant, sort_keys=True, ensure_ascii=False)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()
