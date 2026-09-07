"""Integration: WebSocket authentication, heartbeat, messaging, revocation."""

import pytest
from starlette.testclient import WebSocketDisconnect


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def test_ws_requires_token(client):
    try:
        with client.websocket_connect("/api/ws/device") as ws:
            data = ws.receive_json()
            assert data["type"] == "error"
    except WebSocketDisconnect as exc:
        assert exc.code == 4401


def test_ws_rejects_bad_token(client):
    try:
        with client.websocket_connect("/api/ws/device", headers=_auth_headers("dl_dev_bogus")) as ws:
            data = ws.receive_json()
            assert data["type"] == "error"
    except WebSocketDisconnect as exc:
        assert exc.code == 4401


def test_ws_connect_heartbeat_and_online(client, registered_device):
    device_id = registered_device["device_id"]
    token = registered_device["device_token"]

    with client.websocket_connect("/api/ws/device", headers=_auth_headers(token)) as ws:
        welcome = ws.receive_json()
        assert welcome["type"] == "device.connected"
        assert welcome["data"]["device_id"] == device_id

        # business heartbeat round trip
        ws.send_json({"id": "evt_hb_1", "type": "heartbeat", "version": 1, "timestamp": 1, "data": {"seq": 1}})
        ack = ws.receive_json()
        assert ack["type"] == "heartbeat_ack"
        assert ack["id"] == "evt_hb_1"
        assert "server_time" in ack["data"]

        # device must be reported online while connected
        listing = client.get("/api/devices").json()
        device = next(d for d in listing if d["device_id"] == device_id)
        assert device["status"] == "online"
        assert device["online"] is True
        assert device["connection_count"] == 1


def test_ws_multiple_connections_same_device(client, registered_device):
    device_id = registered_device["device_id"]
    token = registered_device["device_token"]

    with client.websocket_connect("/api/ws/device", headers=_auth_headers(token)) as ws1:
        ws1.receive_json()  # device.connected
        with client.websocket_connect("/api/ws/device", headers=_auth_headers(token)) as ws2:
            ws2.receive_json()  # device.connected
            listing = client.get("/api/devices").json()
            device = next(d for d in listing if d["device_id"] == device_id)
            assert device["connection_count"] == 2
            assert device["online"] is True


def test_server_to_device_message_and_audit(client, registered_device):
    device_id = registered_device["device_id"]
    token = registered_device["device_token"]

    with client.websocket_connect("/api/ws/device", headers=_auth_headers(token)) as ws:
        ws.receive_json()  # device.connected

        res = client.post(
            f"/api/devices/{device_id}/messages",
            json={"type": "message", "data": {"content": "hello device"}},
        )
        assert res.status_code == 200
        payload = res.json()
        assert payload["sent"] == 1

        msg = ws.receive_json()
        assert msg["type"] == "message"
        assert msg["id"] == payload["message_id"]
        assert msg["data"]["content"] == "hello device"

        # device acknowledges
        ws.send_json(
            {"id": payload["message_id"], "type": "message_ack", "version": 1, "timestamp": 2, "data": {"success": True}}
        )


def test_revoked_token_closes_with_4403(client, registered_device):
    device_id = registered_device["device_id"]
    token = registered_device["device_token"]

    with client.websocket_connect("/api/ws/device", headers=_auth_headers(token)) as ws:
        ws.receive_json()  # device.connected

        res = client.post(f"/api/devices/{device_id}/revoke")
        assert res.status_code == 200

        # hub closes the live connection with 4403
        try:
            while True:
                ws.receive_json()
        except WebSocketDisconnect as exc:
            assert exc.code == 4403

    # reconnect attempts with the revoked token must fail with 4403
    try:
        with client.websocket_connect("/api/ws/device", headers=_auth_headers(token)) as ws:
            ws.receive_json()
    except WebSocketDisconnect as exc:
        assert exc.code == 4403
