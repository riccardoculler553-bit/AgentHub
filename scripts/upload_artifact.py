"""上传数据源文件为 Artifact（V1.5 §26 数据平面入口）。

用法：
  python scripts/upload_artifact.py 数据.xlsx                      # 单文件
  python scripts/upload_artifact.py 数据目录/                      # 目录内全部文件逐个上传
  python scripts/upload_artifact.py a.xlsx b.xlsx --role-prefix m  # 多文件

输出每个文件的 artifact_id（创建任务时填入 run_capability 的 inputs）。
认证：优先 X-Admin-Token（.env 的 AGENTHUB_ADMIN_TOKEN）。
"""

import argparse
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]


def admin_token() -> str:
    env = ROOT / ".env"
    if env.exists():
        for line in env.read_text(encoding="utf-8").splitlines():
            if line.startswith("AGENTHUB_ADMIN_TOKEN="):
                return line.split("=", 1)[1].strip()
    return ""


def collect_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for p in paths:
        if p.is_dir():
            files.extend(sorted(f for f in p.iterdir() if f.is_file() and not f.name.startswith(("~$", "."))))
        elif p.is_file():
            files.append(p)
        else:
            print(f"WARN 跳过不存在的路径：{p}")
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description="上传 Artifact")
    parser.add_argument("paths", nargs="+", help="文件或目录（目录取第一层文件）")
    parser.add_argument("--server", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    files = collect_files([Path(p) for p in args.paths])
    if not files:
        print("没有可上传的文件")
        return 2

    headers = {"X-Admin-Token": admin_token()}
    client = httpx.Client(timeout=300, headers=headers)
    print(f"{'artifact_id':<24} name                 size")
    for f in files:
        res = client.post(
            f"{args.server.rstrip('/')}/api/artifacts",
            files={"file": (f.name, f.read_bytes(), "application/octet-stream")},
            data={"name": f.name, "type": "file"},
        )
        if res.status_code != 201:
            print(f"FAILED {f.name}: HTTP {res.status_code} {res.text[:200]}")
            return 1
        body = res.json()
        print(f"{body['artifact_id']:<24} {body['name']:<20} {body['size']}B")
    return 0


if __name__ == "__main__":
    sys.exit(main())
