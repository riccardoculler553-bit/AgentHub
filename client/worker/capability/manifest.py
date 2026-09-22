"""Capability Package Manifest (V1.4 §11) - worker-side copy.

The Worker re-validates the manifest after extract (defense in depth).
Keep in sync with server/app/capability_runtime/manifest.py - server and
client are separate deployables.
"""

import json
import re
from dataclasses import dataclass, field
from pathlib import Path

MANIFEST_FILE = "manifest.json"
AGENTHUB_FILE = "agenthub.yaml"  # V1.7 §3: the additive runtime declaration

NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2}$")
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
RUNTIMES = {"yingdao", "python", "http", "local"}
EXECUTION_MODES = {"once", "service"}  # V1.7 §6
AGENTHUB_BLOCKS = ("execution", "entrypoint", "workspace", "environment", "resources", "restart")


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


# --------------------------------------------------------------------------
# V1.7 agenthub.yaml (§3/§4): the additive declaration merged into config.
# --------------------------------------------------------------------------


def _mini_yaml(text: str) -> dict:
    """Zero-dependency fallback parser for the agenthub.yaml subset:
    nested mappings (one level of indentation), scalar values, string lists
    and inline {} maps. Full YAML is used when PyYAML is importable."""
    data: dict = {}
    stack: list[tuple[int, dict]] = [(-1, data)]
    for raw_line in text.splitlines():
        line = raw_line.split("#", 1)[0].rstrip() if not raw_line.lstrip().startswith("#") else ""
        if not line.strip():
            continue
        indent = len(line) - len(line.lstrip(" "))
        key, _, value = line.strip().partition(":")
        key, value = key.strip(), value.strip()
        while stack and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]
        if value == "":
            child: dict = {}
            parent[key] = child
            stack.append((indent, child))
        elif value.startswith("[") and value.endswith("]"):
            items = [v.strip().strip("'\"") for v in value[1:-1].split(",") if v.strip()]
            parent[key] = items
        elif value == "{}":
            parent[key] = {}
        else:
            parent[key] = value.strip("'\"")
    return data


def _load_yaml(text: str) -> dict:
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(text)
    except ImportError:
        data = _mini_yaml(text)
    return data if isinstance(data, dict) else {}


def parse_agenthub(raw: bytes) -> dict:
    """Parse + validate agenthub.yaml (mirror of the server-side validator).
    Raises ValueError on any problem (INVALID_PACKAGE upstream)."""
    try:
        data = _load_yaml(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError("agenthub.yaml is not valid UTF-8") from exc
    if not isinstance(data, dict):
        raise ValueError("agenthub.yaml must be a mapping")
    name = data.get("name")
    if name is not None and not NAME_RE.match(str(name).strip()):
        raise ValueError(f"agenthub.name must be <domain>.<resource>.<action>: {name!r}")
    version = data.get("version")
    if version is not None and not VERSION_RE.match(str(version).strip()):
        raise ValueError(f"agenthub.version must be semver x.y.z: {version!r}")
    execution = data.get("execution") or {}
    if not isinstance(execution, dict):
        raise ValueError("agenthub.execution must be a mapping")
    mode = execution.get("mode")
    if mode is not None and mode not in EXECUTION_MODES:
        raise ValueError(f"agenthub.execution.mode must be one of {sorted(EXECUTION_MODES)}: {mode!r}")
    entrypoint = data.get("entrypoint")
    if entrypoint is not None:
        if not isinstance(entrypoint, dict) or not str(entrypoint.get("command", "")).strip():
            raise ValueError("agenthub.entrypoint must be a mapping with command: <script>")
        command = str(entrypoint["command"]).strip()
        if len(command) > 200 or command.startswith(("/", "\\")) or ".." in command.replace("\\", "/").split("/"):
            raise ValueError(f"agenthub.entrypoint.command must be a relative package path: {command!r}")
        args = entrypoint.get("args") or []
        if not isinstance(args, list) or not all(isinstance(a, str) for a in args):
            raise ValueError("agenthub.entrypoint.args must be a list of strings")
    for block in ("workspace", "environment", "resources", "restart"):
        value = data.get(block)
        if value is not None and not isinstance(value, dict):
            raise ValueError(f"agenthub.{block} must be a mapping")
    return data


def merge_agenthub(manifest: Manifest, agenthub: dict) -> Manifest:
    """Merge validated agenthub.yaml blocks into manifest.config (yaml wins
    per block; identity fields must agree with manifest.json)."""
    if str(agenthub.get("name", manifest.name)).strip() != manifest.name:
        raise ValueError(f"agenthub.name != manifest.name: {agenthub.get('name')!r} != {manifest.name!r}")
    if str(agenthub.get("version", manifest.version)).strip() != manifest.version:
        raise ValueError(f"agenthub.version != manifest.version: {agenthub.get('version')!r} != {manifest.version!r}")
    merged = dict(manifest.config)
    for block in AGENTHUB_BLOCKS:
        if agenthub.get(block) is not None:
            merged[block] = agenthub[block]
    command = (agenthub.get("entrypoint") or {}).get("command")
    if command:
        merged["entrypoint_command"] = str(command).strip()
    inputs = agenthub.get("inputs")
    if isinstance(inputs, dict) and inputs:
        merged_inputs = dict(manifest.inputs)
        merged_inputs.update(inputs)
        manifest.inputs = merged_inputs
    manifest.config = merged
    return manifest


def config_entrypoint_command(config: dict) -> str | None:
    """The V1.7 entrypoint.command (relative package path) if declared."""
    value = config.get("entrypoint_command")
    if isinstance(value, str) and value.strip():
        return value.strip()
    entrypoint = config.get("entrypoint")
    if isinstance(entrypoint, dict):
        command = str(entrypoint.get("command", "")).strip()
        return command or None
    return None


def load_package_config(package_dir: Path, manifest: Manifest) -> Manifest:
    """Merge agenthub.yaml from the installed package (when present) into the
    manifest config (V1.7 §3: 原业务代码 + agenthub.yaml = AgentHub Capability).
    A missing yaml is the normal legacy path; a malformed one raises
    ValueError -> INVALID_PACKAGE upstream."""
    yaml_path = package_dir / AGENTHUB_FILE
    if not yaml_path.is_file():
        return manifest
    return merge_agenthub(manifest, parse_agenthub(yaml_path.read_bytes()))
