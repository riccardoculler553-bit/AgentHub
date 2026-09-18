"""V1.5 save_artifact agent tool tests: 结果产物落到服务器本地目录。"""

import pytest

from app.agent.tools.registry import build_default_registry
from app.core.config import settings

from ._worker import register_device


@pytest.fixture()
def registry():
    return build_default_registry(None)


async def _call(registry, name: str, args: dict):
    tool = registry.get(name)
    assert tool is not None, f"tool {name} missing"
    from app.db.database import SessionLocal

    with SessionLocal() as db:
        return await tool.run(db, args)


def _upload(client, token: str, name: str, content: bytes) -> str:
    res = client.post(
        "/api/artifacts",
        files={"file": (name, content, "application/octet-stream")},
        data={"name": name, "type": "file"},
        headers={"Authorization": f"Bearer {token}"},
    )
    assert res.status_code == 201, res.text
    return res.json()["artifact_id"]


@pytest.mark.anyio
async def test_save_artifact_writes_local_file(client, registry, tmp_path):
    device = register_device(client, "产物落盘测试机")
    artifact_id = _upload(client, device["device_token"], "结果.xlsx", b"RESULT-BYTES")

    r = await _call(registry, "save_artifact", {
        "artifact_id": artifact_id, "dir": str(tmp_path),
    })
    assert r.success, r
    assert r.data["size"] == len(b"RESULT-BYTES")
    saved = tmp_path / "结果.xlsx"
    assert saved.read_bytes() == b"RESULT-BYTES"

    # 同名不覆盖：第二次保存加时间戳后缀
    r2 = await _call(registry, "save_artifact", {
        "artifact_id": artifact_id, "dir": str(tmp_path),
    })
    assert r2.success
    assert r2.data["saved_to"] != str(saved)


@pytest.mark.anyio
async def test_save_artifact_rejects_relative_dir(registry):
    r = await _call(registry, "save_artifact", {
        "artifact_id": "art_x", "dir": "relative/dir",
    })
    assert not r.success and r.error_code == "INVALID_ARGS"


@pytest.mark.anyio
async def test_save_artifact_unknown_id(client, registry, tmp_path):
    r = await _call(registry, "save_artifact", {
        "artifact_id": "art_missing", "dir": str(tmp_path),
    })
    assert not r.success and r.error_code == "ARTIFACT_NOT_FOUND"
