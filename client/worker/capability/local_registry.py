"""Installed capability scan -> worker.capabilities report (V1.4 §17).

Worker 上报本地 Capability (name + version)。Corruption-tolerant: a broken
install dir is skipped (the next ensure() will re-pull it).
"""

import json
from pathlib import Path

from worker.capability.cache import MARKER_FILE, capabilities_root
from worker.capability.manifest import MANIFEST_FILE, parse_manifest_bytes


def scan_installed(root: Path | None = None) -> list[dict]:
    root = root or capabilities_root()
    if not root.is_dir():
        return []
    found: list[dict] = []
    for name_dir in sorted(root.iterdir()):
        if not name_dir.is_dir() or name_dir.name.startswith("."):
            continue
        for version_dir in sorted(name_dir.iterdir()):
            if not version_dir.is_dir() or version_dir.name.startswith("."):
                continue
            try:
                manifest = parse_manifest_bytes((version_dir / MANIFEST_FILE).read_bytes())
                marker = json.loads((version_dir / MARKER_FILE).read_text(encoding="utf-8"))
            except (OSError, ValueError):
                continue
            # Directory names are not authoritative: the manifest inside is.
            if manifest.name != name_dir.name or manifest.version != version_dir.name:
                continue
            if not str(marker.get("checksum", "")).strip():
                continue
            if not found or not any(
                c["name"] == manifest.name and c["version"] == manifest.version for c in found
            ):
                found.append({"name": manifest.name, "version": manifest.version})
    return found
