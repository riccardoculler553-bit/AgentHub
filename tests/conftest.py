"""Test fixtures.

DATABASE_URL is forced to a file-based SQLite BEFORE app modules are imported,
so config picks it up at import time.
"""

import os
import sys
import tempfile
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]

# File-based SQLite: concurrent TestClient WS sessions each run in their own
# thread and get their own pooled connection; a shared in-memory connection
# (StaticPool) corrupts transaction state under this concurrency.
_TMP_DB = Path(tempfile.mkdtemp(prefix="devicelink-test-")) / "test.db"
os.environ["DATABASE_URL"] = f"sqlite:///{_TMP_DB.as_posix()}"
# Fresh client identity/ledger home per session (worker.db accumulates state)
os.environ["DEVICELINK_HOME"] = str(Path(tempfile.mkdtemp(prefix="devicelink-client-")))
# Keep MVP agent runs short if a result never arrives.
os.environ["AGENT_RUN_MAX_WAIT"] = "20"
# Tests exercise the open mode; admin auth is covered separately.
os.environ.pop("AGENTHUB_ADMIN_TOKEN", None)
# Do not let a developer .env steer the agent into real LLM calls during tests.
os.environ.pop("OPENAI_API_KEY", None)

sys.path.insert(0, str(PROJECT_ROOT / "server"))
sys.path.insert(0, str(PROJECT_ROOT / "client"))

import app.core.config as _config  # noqa: E402

# .env may define AGENTHUB_ADMIN_TOKEN; tests exercise the open mode
# (empty token disables admin auth by contract).
_config.settings.admin_token = ""

import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from app.db.database import Base, engine  # noqa: E402
from app.main import app as fastapi_app  # noqa: E402


@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)
    engine.dispose()


@pytest.fixture()
def client():
    with TestClient(fastapi_app) as test_client:
        yield test_client


@pytest.fixture()
def registered_device(client: TestClient):
    """Create a registration code and register a device. Returns dict with token."""
    code_res = client.post("/api/device-registration", json={"device_name": "测试设备A"})
    assert code_res.status_code == 201, code_res.text
    code = code_res.json()["code"]

    reg_res = client.post(
        "/api/devices/register",
        json={
            "registration_code": code,
            "device_name": "测试设备A",
            "hostname": "DESKTOP-TEST",
            "platform": "windows",
            "client_version": "1.0.0",
        },
    )
    assert reg_res.status_code == 201, reg_res.text
    return reg_res.json()
