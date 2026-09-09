"""Capability Package Manifest (V1.4 §11) - worker-side copy.

The Worker re-validates the manifest after extract (defense in depth).
Keep in sync with server/app/capability_runtime/manifest.py - server and
client are separate deployables.
"""

import json
import re
from dataclasses import dataclass, field

MANIFEST_FILE = "manifest.json"

NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2}$")
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
RUNTIMES = {"yingdao", "python", "http", "local"}


@dataclass
class Manifest:
    name: str
    version: str
    runtime: str
    entrypoint: str = "main"
    inputs: dict = field(default_factory=dict)
    outputs: dict = field(default_factory=dict)
    config: dict = field(default_factory=dict)


def parse_manifest(data: dict) -> Manifest:
    """Validate raw manifest dict -> Manifest. Raises ValueError on any problem."""
    if not isinstance(data, dict):
        raise ValueError("manifest must be a JSON object")
    name = str(data.get("name", "")).strip()
    version = str(data.get("version", "")).strip()
    runtime = str(data.get("runtime", "")).strip().lower()
    if not NAME_RE.match(name):
        raise ValueError(f"manifest.name must be <domain>.<resource>.<action>: {name!r}")
    if not VERSION_RE.match(version):
        raise ValueError(f"manifest.version must be semver x.y.z: {version!r}")
    if runtime not in RUNTIMES:
        raise ValueError(f"manifest.runtime must be one of {sorted(RUNTIMES)}: {runtime!r}")
    entrypoint = str(data.get("entrypoint", "main")).strip() or "main"
    if len(entrypoint) > 200:
        raise ValueError("manifest.entrypoint too long")
    inputs = data.get("inputs") or {}
    outputs = data.get("outputs") or {}
    config = data.get("config") or {}
    for label, block in (("inputs", inputs), ("outputs", outputs)):
        if not isinstance(block, dict):
            raise ValueError(f"manifest.{label} must be an object")
    if not isinstance(config, dict):
        raise ValueError("manifest.config must be an object")
    return Manifest(
        name=name, version=version, runtime=runtime,
        entrypoint=entrypoint, inputs=inputs, outputs=outputs, config=config,
    )


def parse_manifest_bytes(raw: bytes) -> Manifest:
    try:
        data = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"manifest.json is not valid JSON: {exc}") from exc
    return parse_manifest(data)
