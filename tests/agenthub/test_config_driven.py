"""V1.7 config-driven execution tests (§2/§3/§4/§60).

A plain business program (argv-based CLI, no CAPABILITY_* env usage, no
AgentHub imports) + one agenthub.yaml = a working Capability. The executor
maps the workspace, substitutes ${INPUT_DIR}/${OUTPUT_DIR}, injects the
declared environment variables and uploads the output scan.
"""

import asyncio
import uuid

import pytest

from worker.capability.executors import PythonCapabilityExecutor
from worker.capability.manifest import load_package_config, parse_manifest
from worker.capability.context import ExecutionContext

from .test_worker_capability_runtime import _progress

MAIN = """\
import sys, os
args = sys.argv[1:]
def opt(name):
    return args[args.index(name) + 1]
out = opt("--output")
marker = os.environ.get("BIZ_MARKER", "none")
open(f"{out}/result_{marker}.txt", "w", encoding="utf-8").write("done")
print("business program finished")
"""

AGENTHUB = """\
name: biz.data.clean
version: 1.0.0

runtime:
  type: python

execution:
  mode: once

entrypoint:
  command: main.py
  args:
    - "--input"
    - "${INPUT_DIR}"
    - "--output"
    - "${OUTPUT_DIR}"

workspace:
  input_dir: input
  output_dir: results

environment:
  variables:
    BIZ_MARKER: v17
"""


def _context(tmp_path: dict) -> ExecutionContext:
    package_dir = tmp_path / "pkg" / "biz.data.clean" / "1.0.0"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "main.py").write_text(MAIN, encoding="utf-8")
    (package_dir / "agenthub.yaml").write_text(AGENTHUB, encoding="utf-8")
    manifest = parse_manifest({
        "name": "biz.data.clean", "version": "1.0.0", "runtime": "python",
        "entrypoint": "main",
    })
    manifest = load_package_config(package_dir, manifest)
    return ExecutionContext(
        execution_id=f"exec_{uuid.uuid4().hex[:8]}", task_id="task_1", step_id="step_1",
        attempt_id="attempt_1", capability="biz.data.clean", version="1.0.0",
        worker_id="worker-1", params={}, manifest=manifest, package_dir=package_dir, timeout=60,
    )


@pytest.mark.anyio
async def test_config_driven_once_execution(tmp_path):
    context = _context(tmp_path)
    assert context.manifest.config["execution"]["mode"] == "once"
    assert context.manifest.config["entrypoint_command"] == "main.py"

    result = await PythonCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert result.success, result.__dict__
    # output scan picked the workspace.output_dir (results/, not output/)
    assert [(name, "file") for name, _path in result.artifact_files] == [("result_v17.txt", "file")]
    # the artifact lives under the declared workspace output dir
    assert str(result.artifact_files[0][1]).replace("\\", "/").endswith("/results/result_v17.txt")


@pytest.mark.anyio
async def test_config_driven_bad_yaml_fails_invalid_package(tmp_path):
    package_dir = tmp_path / "pkg" / "biz.data.clean" / "1.0.0"
    package_dir.mkdir(parents=True, exist_ok=True)
    (package_dir / "main.py").write_text("print('hi')", encoding="utf-8")
    (package_dir / "agenthub.yaml").write_text(
        "name: other.name.here\nversion: 1.0.0\n", encoding="utf-8"
    )
    manifest = parse_manifest({
        "name": "biz.data.clean", "version": "1.0.0", "runtime": "python", "entrypoint": "main",
    })
    context = ExecutionContext(
        execution_id="exec_bad", task_id="t", step_id="s", attempt_id="a",
        capability="biz.data.clean", version="1.0.0", worker_id="w",
        params={}, manifest=manifest, package_dir=package_dir, timeout=30,
    )
    result = await PythonCapabilityExecutor().execute(context, _progress, asyncio.Event())
    assert not result.success
    assert result.error_code == "INVALID_PACKAGE"
