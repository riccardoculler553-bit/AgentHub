"""Device registration via one-time code."""

import platform
import socket

import httpx

import protocol

CLIENT_VERSION = "1.0.0"


class RegistrationError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"[{code}] {message}")
        self.code = code


def collect_device_info() -> dict:
    return {
        "hostname": socket.gethostname(),
        "platform": platform.system().lower() or "unknown",
        "client_version": CLIENT_VERSION,
    }


def register_device(server_url: str, registration_code: str, device_name: str | None = None) -> dict:
    """POST /api/devices/register. Returns {"device_id", "device_token"}."""
    body = {"registration_code": registration_code.strip().upper(), **collect_device_info()}
    if device_name:
        body["device_name"] = device_name
    response = httpx.post(f"{server_url.rstrip('/')}/api/devices/register", json=body, timeout=10)
    if response.status_code == 201:
        return response.json()
    detail = response.json().get("detail", {}) if response.headers.get("content-type", "").startswith("application/json") else {}
    code = detail.get("code", "registration_failed") if isinstance(detail, dict) else "registration_failed"
    message = detail.get("message", response.text) if isinstance(detail, dict) else response.text
    raise RegistrationError(code, message)
