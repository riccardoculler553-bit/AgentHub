"""V1.5 端到端验收（§46/§62）：data.preprocess_excel 在指定 Worker 上真实执行。

前置：server 已启动（E2E 专用 SQLite），真实 worker 已注册在线。

流程：
  1. 发布能力：create capability -> upload package zip -> publish 1.0.0
  2. 上传输入 Artifact：1 个数据文件 + 4 张映射表（admin token）
  3. 创建 CAPABILITY 任务（input_artifacts 引用 + 指定 device）
  4. 轮询任务到终态（首次执行含 venv + pip install，可能数分钟）
  5. 下载输出 Artifact 并校验 xlsx 可读

用法：python scripts/e2e_v15.py [--server http://127.0.0.1:8000]
"""

import argparse
import io
import sys
import time
from pathlib import Path

import httpx
from openpyxl import load_workbook

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_ZIP = Path(r"D:\Slaes - 副本\preprocess_tool\dist\data.excel.preprocess-1.0.0.zip")
CAP_NAME = "data.excel.preprocess"
CAP_VERSION = "1.0.0"
DEVICE_NAME = "V15测试机"

DATA_FILE = Path(r"D:\Slaes - 副本\preprocess_tool\sandbox\fixtures\tiny\订单样例.xlsx")
MAPPING_FILES = [
    "店铺对应平台.xlsx",
    "商品资料.xlsx",
    "匹配码.xlsx",
    "组合装商品.xlsx",
]
MAPPING_DIR = Path(r"D:\Slaes - 副本\preprocess_tool\sandbox\mappings")


def admin_token() -> str:
    for line in (ROOT / ".env").read_text(encoding="utf-8").splitlines():
        if line.startswith("AGENTHUB_ADMIN_TOKEN="):
            return line.split("=", 1)[1].strip()
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--server", default="http://127.0.0.1:8000")
    args = parser.parse_args()
    base = args.server.rstrip("/")
    headers = {"X-Admin-Token": admin_token()}
    client = httpx.Client(timeout=60, headers=headers)

    # ---- 0. server + device -------------------------------------------------
    assert client.get(f"{base}/api/health").json()["status"] == "ok"
    devices = client.get(f"{base}/api/devices").json()
    candidates = [d for d in devices if d["name"] == DEVICE_NAME]
    device = next((d for d in candidates if d.get("status") == "online"), None) or (
        candidates[0] if candidates else None
    )
    if device is None:
        print(f"FATAL device {DEVICE_NAME!r} not online; devices={[d['name'] for d in devices]}")
        return 2
    if device.get("status") != "online":
        print(f"FATAL device {device['device_id']} is {device.get('status')}")
        return 2
    print(f"[0] device  {device['name']} ({device['device_id']}) {device['status']}")

    # ---- 1. capability + package + version ---------------------------------
    caps = client.get(f"{base}/api/capabilities").json()
    if not any(c["name"] == CAP_NAME for c in caps):
        res = client.post(f"{base}/api/capabilities", json={
            "name": CAP_NAME, "runtime_type": "PYTHON",
            "display_name": "Excel 大数据预处理",
            "description": "Polars 流水线预处理能力（V1.5 首个真实能力）",
        })
        assert res.status_code == 201, res.text
    package_bytes = PACKAGE_ZIP.read_bytes()
    res = client.post(
        f"{base}/api/capabilities/{CAP_NAME}/versions",
        files={"file": (PACKAGE_ZIP.name, package_bytes, "application/zip")},
    )
    if res.status_code == 201:
        version_id = res.json()["id"]
        res = client.post(f"{base}/api/capability-versions/{version_id}/publish")
        assert res.status_code == 200, res.text
        print(f"[1] published {CAP_NAME}@{CAP_VERSION} (version_id={version_id})")
    else:
        assert res.status_code == 409, res.text  # version already exists
        print(f"[1] {CAP_NAME}@{CAP_VERSION} already published")

    # ---- 2. input artifacts -------------------------------------------------
    def upload(path: Path) -> str:
        res = client.post(
            f"{base}/api/artifacts",
            files={"file": (path.name, path.read_bytes(), "application/octet-stream")},
            data={"name": path.name, "type": "file"},
        )
        assert res.status_code == 201, res.text
        body = res.json()
        print(f"[2] artifact {body['artifact_id']} <- {path.name} ({body['size']}B)")
        return body["artifact_id"]

    data_artifact = upload(DATA_FILE)
    mapping_artifacts = [upload(MAPPING_DIR / name) for name in MAPPING_FILES]

    # ---- 3. create capability task ------------------------------------------
    res = client.post(f"{base}/api/tasks", json={
        "name": f"[E2E] {CAP_NAME}",
        "execution_type": "CAPABILITY",
        "capability_version": CAP_VERSION,
        "target_device_id": device["device_id"],
        "steps": [{"command": CAP_NAME, "params": {}}],
        "input_artifacts": [
            {"artifact_id": data_artifact, "role": "data_dir"},
            *[{"artifact_id": a, "role": "mapping_dir"} for a in mapping_artifacts],
        ],
    })
    assert res.status_code == 201, res.text
    task_id = res.json()["task_id"]
    print(f"[3] task {task_id} created -> {device['name']}")

    # ---- 4. wait terminal ----------------------------------------------------
    start = time.monotonic()
    terminal = {"SUCCESS", "FAILED", "CANCELLED", "TIMEOUT"}
    status = ""
    while time.monotonic() - start < 900:
        task = client.get(f"{base}/api/tasks/{task_id}").json()
        status = task["status"]
        if status in terminal:
            break
        time.sleep(3)
    print(f"[4] task {task_id} -> {status} ({time.monotonic() - start:.0f}s)")
    for event in task.get("events", [])[-6:]:
        print(f"    {event['event_type']}: {event.get('payload', {}).get('error_code') or ''}")

    # ---- 5. collect outputs --------------------------------------------------
    artifacts = client.get(f"{base}/api/artifacts", params={"task_id": task_id}).json()
    outputs = [a for a in artifacts if a["artifact_id"] != data_artifact]
    print(f"[5] artifacts: {[(a['name'], a['size']) for a in outputs]}")
    if status != "SUCCESS":
        return 1

    out_dir = Path(r"D:\Slaes - 副本\preprocess_tool\sandbox\output\e2e")
    out_dir.mkdir(parents=True, exist_ok=True)
    for a in outputs:
        blob = client.get(f"{base}/api/artifacts/{a['artifact_id']}/download").content
        target = out_dir / a["name"]
        target.write_bytes(blob)
        wb = load_workbook(target, read_only=True)
        rows = wb.active.max_row
        wb.close()
        print(f"    downloaded {target} ({len(blob)}B, {rows} rows incl. header)")
    print("E2E OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
