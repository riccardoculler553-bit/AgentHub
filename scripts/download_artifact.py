"""列出 / 下载 Artifact（V1.5 §26 数据平面）。

用法：
  python scripts/download_artifact.py --list                       # 最近 100 个
  python scripts/download_artifact.py --list --task-id task_xxx    # 某任务的产物
  python scripts/download_artifact.py art_xxx art_yyy              # 下载到 ./downloads
  python scripts/download_artifact.py art_xxx --out D:/结果目录
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


def main() -> int:
    parser = argparse.ArgumentParser(description="列出/下载 Artifact")
    parser.add_argument("artifact_ids", nargs="*", help="要下载的 artifact_id")
    parser.add_argument("--list", action="store_true", help="列出最近的 Artifact")
    parser.add_argument("--task-id", default=None, help="按任务过滤（配合 --list）")
    parser.add_argument("--out", default="downloads", help="下载目录")
    parser.add_argument("--server", default="http://127.0.0.1:8000")
    args = parser.parse_args()

    headers = {"X-Admin-Token": admin_token()}
    base = args.server.rstrip("/")
    client = httpx.Client(timeout=300, headers=headers)

    if args.list:
        params = {"limit": 100}
        if args.task_id:
            params["task_id"] = args.task_id
        rows = client.get(f"{base}/api/artifacts", params=params).json()
        print(f"{'artifact_id':<24} {'name':<24} {'size':>10}  task_id")
        for r in rows:
            print(f"{r['artifact_id']:<24} {r['name']:<24} {r['size']:>10}  {r.get('task_id') or '-'}")
        return 0

    if not args.artifact_ids:
        parser.print_help()
        return 2

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    for artifact_id in args.artifact_ids:
        res = client.get(f"{base}/api/artifacts/{artifact_id}/download")
        if res.status_code != 200:
            print(f"FAILED {artifact_id}: HTTP {res.status_code} {res.text[:200]}")
            return 1
        name = res.headers.get("content-disposition", "")
        filename = artifact_id
        if "filename=" in name:
            filename = name.split("filename=")[-1].strip('"').strip("'")
        target = out_dir / filename
        target.write_bytes(res.content)
        print(f"{artifact_id} -> {target} ({len(res.content)}B)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
