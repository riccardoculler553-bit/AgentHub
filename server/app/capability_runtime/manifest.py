"""Capability Package Manifest parsing/validation (V1.4 §11).

The manifest is the FIRST validation layer: server validates it on version
creation, Worker re-validates it after extract (defense in depth). The Worker
keeps its own copy (client/worker/capability/manifest.py) - server and client
are separate deployables.
"""

import json
import re
from dataclasses import dataclass, field

MANIFEST_FILE = "manifest.json"
AGENTHUB_FILE = "agenthub.yaml"  # V1.7 §3: the additive runtime declaration

NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*){2}$")
VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")
RUNTIMES = {"yingdao", "python", "http", "local"}
EXECUTION_MODES = {"once", "service"}  # V1.7 §6

# agenthub.yaml blocks merged into manifest.config (V1.7 §4)
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

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "version": self.version,
            "runtime": self.runtime,
            "entrypoint": self.entrypoint,
            "inputs": self.inputs,
            "outputs": self.outputs,
            "config": self.config,
        }


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


def parse_agenthub(raw: bytes) -> dict:
    """Parse + validate agenthub.yaml (V1.7 §4). Returns the validated dict.
    The file is ADDITIVE: business Python stays untouched (§2.1); it only
    declares how AgentHub runs the program."""
    try:
        import yaml

        data = yaml.safe_load(raw.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ValueError("agenthub.yaml is not valid UTF-8") from exc
    except Exception as exc:  # yaml.YAMLError and friends
        raise ValueError(f"agenthub.yaml is not valid YAML: {exc}") from exc
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
        raise ValueError(
            f"agenthub.name {agenthub.get('name')!r} != manifest.name {manifest.name!r}"
        )
    if str(agenthub.get("version", manifest.version)).strip() != manifest.version:
        raise ValueError(
            f"agenthub.version {agenthub.get('version')!r} != manifest.version {manifest.version!r}"
        )
    merged = dict(manifest.config)
    for block in AGENTHUB_BLOCKS:
        if agenthub.get(block) is not None:
            merged[block] = agenthub[block]
    # entrypoint.command upgrades the legacy bare-module entrypoint
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
