"""Local identity storage: ~/.devicelink/device.json"""

import json
import os
import tempfile
from pathlib import Path

IDENTITY_FILE = "device.json"


def identity_dir() -> Path:
    root = os.getenv("DEVICELINK_HOME")
    base = Path(root) if root else Path.home() / ".devicelink"
    return base


def identity_path() -> Path:
    return identity_dir() / IDENTITY_FILE


def save_identity(data: dict) -> Path:
    directory = identity_dir()
    directory.mkdir(parents=True, exist_ok=True)
    path = identity_path()
    # Atomic write: temp file in same directory, then replace
    fd, tmp_name = tempfile.mkstemp(dir=str(directory), prefix=".device-", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp_name, path)
    except Exception:
        if os.path.exists(tmp_name):
            os.remove(tmp_name)
        raise
    return path


def load_identity() -> dict | None:
    path = identity_path()
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, OSError):
        return None


def clear_identity() -> None:
    path = identity_path()
    if path.exists():
        path.unlink()
