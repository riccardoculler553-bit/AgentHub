"""Integration: registration code -> device registration -> listing."""

import pytest


def test_full_registration_flow(client):
    # 1. create registration code
    res = client.post("/api/device-registration", json={"device_name": "办公室电脑01"})
    assert res.status_code == 201
    code_payload = res.json()
    assert code_payload["code"].startswith("DL-")
    assert code_payload["registration_id"]
    assert code_payload["expires_at"]

    # 2. device registers
    reg = client.post(
        "/api/devices/register",
        json={
            "registration_code": code_payload["code"],
            "device_name": "办公室电脑01",
            "hostname": "DESKTOP-001",
            "platform": "windows",
            "client_version": "1.0.0",
        },
    )
    assert reg.status_code == 201
    body = reg.json()
    assert body["device_token"].startswith("dl_dev_")
    device_id = body["device_id"]
    assert len(device_id) == 36

    # 3. device appears in listing
    listing = client.get("/api/devices").json()
    match = [d for d in listing if d["device_id"] == device_id]
    assert len(match) == 1
    device = match[0]
    assert device["name"] == "办公室电脑01"
    assert device["status"] == "offline"
    assert device["online"] is False
    assert device["connection_count"] == 0

    # 4. detail endpoint works
    detail = client.get(f"/api/devices/{device_id}")
    assert detail.status_code == 200


def test_code_single_use(client):
    res = client.post("/api/device-registration", json={})
    code = res.json()["code"]

    first = client.post("/api/devices/register", json={"registration_code": code, "device_name": "D1"})
    assert first.status_code == 201

    second = client.post("/api/devices/register", json={"registration_code": code, "device_name": "D2"})
    assert second.status_code == 400
    assert second.json()["detail"]["code"] == "registration_code_used"


def test_invalid_code_rejected(client):
    res = client.post("/api/devices/register", json={"registration_code": "DL-NOPE-NOPE"})
    assert res.status_code == 400
    assert res.json()["detail"]["code"] == "registration_code_invalid"


def test_revoke_device(client, registered_device):
    device_id = registered_device["device_id"]
    token = registered_device["device_token"]

    res = client.post(f"/api/devices/{device_id}/revoke")
    assert res.status_code == 200
    assert res.json()["status"] == "revoked"

    # Token must no longer authenticate on HTTP (guarded dependency)
    from app.db.database import SessionLocal
    from app.auth.token import TokenService
    from app.core.exceptions import TokenRevoked

    with SessionLocal() as db:
        with pytest.raises(TokenRevoked):
            TokenService(db).verify(token)


def test_message_to_offline_device_fails(client, registered_device):
    device_id = registered_device["device_id"]
    res = client.post(
        f"/api/devices/{device_id}/messages",
        json={"type": "message", "data": {"content": "hello"}},
    )
    assert res.status_code == 409
    assert res.json()["detail"]["code"] == "device_offline"
