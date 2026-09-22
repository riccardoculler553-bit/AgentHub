"""V1.7 bootstrap / service-wrapper / updater tests (doc §15-§20, §21, §64)."""

import hashlib
import io
import zipfile

import pytest


# ------------------------------------------------------------- enrollment (§16)


def test_enrollment_token_flow(client):
    """Admin creates an enrollment token; the token actually registers a device."""
    res = client.post("/api/device-enrollment/tokens", json={"device_name": "Bootstrap Machine"})
    assert res.status_code in (200, 201), res.text
    body = res.json()
    assert body["enrollment_token"]
    assert body["expires_at"]
    command = body["bootstrap_command"]
    assert command.startswith("python bootstrap.py --server ")
    assert f"--token {body['enrollment_token']}" in command

    reg = client.post(
        "/api/devices/register",
        json={
            "registration_code": body["enrollment_token"],
            "device_name": "Bootstrap Machine",
            "hostname": "DESKTOP-BOOTSTRAP",
            "platform": "windows",
            "client_version": "1.7.0",
        },
    )
    assert reg.status_code == 201, reg.text
    assert reg.json()["device_id"]
    assert reg.json()["device_token"]


# ------------------------------------------------------------- bundle (§15)


def test_bootstrap_latest_returns_zip_with_checksum(client):
    res = client.get("/api/bootstrap/latest")
    assert res.status_code == 200, res.text
    assert res.headers["content-type"].startswith("application/zip")
    assert res.content[:2] == b"PK"
    # §64: the server pins bundle integrity with X-Checksum (sha256 of the body).
    assert res.headers["x-checksum"] == hashlib.sha256(res.content).hexdigest()

    # The bundle is a real zip of the client tree (bootstrap ships worker/main).
    with zipfile.ZipFile(io.BytesIO(res.content)) as zf:
        names = zf.namelist()
    assert "main.py" in names
    assert any(name.startswith("worker/") for name in names)
    # Exclusions (§15): no caches/venv/logs/identity files ship.
    assert not any("__pycache__" in name or "/logs/" in name for name in names)
    assert not any(name.rsplit("/", 1)[-1].startswith("identity") for name in names)


# ------------------------------------------------------------- update check (§64)


def test_worker_update_check_with_device_token(client, registered_device):
    from app.api.bootstrap import BOOTSTRAP_WORKER_VERSION

    res = client.post(
        "/api/worker-updates/check",
        headers={"Authorization": f"Bearer {registered_device['device_token']}"},
        json={"device_id": registered_device["device_id"], "current_version": "0.0.0-fake"},
    )
    assert res.status_code == 200, res.text
    body = res.json()
    assert body["update_available"] is True
    assert body["target_version"] == BOOTSTRAP_WORKER_VERSION
    assert body["download_url"] == "/api/bootstrap/latest"


def test_worker_update_check_with_admin_token(client, monkeypatch):
    """The admin-token path (manual probe) also works."""
    from app.api.bootstrap import BOOTSTRAP_WORKER_VERSION
    from app.core.config import settings

    monkeypatch.setattr(settings, "admin_token", "secret-admin", raising=False)
    res = client.post(
        "/api/worker-updates/check",
        headers={"X-Admin-Token": "secret-admin"},
        json={"current_version": BOOTSTRAP_WORKER_VERSION},
    )
    assert res.status_code == 200, res.text
    assert res.json()["update_available"] is False


def test_worker_update_check_rejects_bad_bearer(client):
    res = client.post(
        "/api/worker-updates/check",
        headers={"Authorization": "Bearer not-a-real-token"},
        json={"current_version": "1.0.0"},
    )
    assert res.status_code == 401, res.text


# ------------------------------------------------------------- client helpers


def test_bootstrap_join_url():
    from bootstrap.bootstrap import _join_url

    assert _join_url("http://host:8000/", "/api/devices/register") == "http://host:8000/api/devices/register"
    assert _join_url("http://host:8000", "/api/devices/register") == "http://host:8000/api/devices/register"


def test_discovery_prefers_arg_then_env(monkeypatch):
    from bootstrap.discovery import DiscoveryError, discover

    assert discover("http://a:1/") == "http://a:1"
    monkeypatch.setenv("AGENTHUB_SERVER_URL", "http://b:2")
    assert discover(None) == "http://b:2"
    # Explicit --server still wins over the env var.
    assert discover("http://a:1") == "http://a:1"
    monkeypatch.delenv("AGENTHUB_SERVER_URL", raising=False)
    with pytest.raises(DiscoveryError) as excinfo:
        discover(None)
    assert "--server" in str(excinfo.value)


def test_updater_sha256_file(tmp_path):
    from worker.updater import _sha256_file

    path = tmp_path / "bundle.zip"
    path.write_bytes(b"PK\x03\x04 agenthub test payload")
    assert _sha256_file(path) == hashlib.sha256(path.read_bytes()).hexdigest()


def test_updater_swap_dirs_rolls_back(tmp_path, monkeypatch):
    """If the staging swap fails mid-way, the previous worker dir is restored."""
    import worker.updater as updater

    install_dir = tmp_path
    worker = install_dir / "worker"
    worker.mkdir()
    (worker / "main.py").write_text("old", encoding="utf-8")

    real_rename = updater.Path.rename
    calls = {"n": 0}

    def fake_rename(self, target):
        calls["n"] += 1
        if calls["n"] == 2:  # first rename (worker -> worker.old) succeeds, second fails
            raise OSError("locked")
        return real_rename(self, target)

    monkeypatch.setattr(updater.Path, "rename", fake_rename, raising=True)
    with pytest.raises(OSError):
        updater._swap_worker_dirs(install_dir, install_dir / "worker.update")
    # roll back: previous worker content intact under worker/
    assert (worker / "main.py").read_text(encoding="utf-8") == "old"


def test_windows_service_module_imports_without_pywin32():
    import worker.service.windows_service as ws_mod

    assert hasattr(ws_mod, "service_main")
