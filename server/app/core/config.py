"""Application configuration.

Reads .env from the project root (two levels above server/), then falls back
to process environment variables and defaults.
"""

import os
from pathlib import Path
from urllib.parse import quote


def _load_env_file() -> None:
    env_path = Path(__file__).resolve().parents[3] / ".env"
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, value)


_load_env_file()


def _build_database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    host = os.getenv("DB_HOST", "127.0.0.1")
    port = os.getenv("DB_PORT", "3306")
    user = os.getenv("DB_USER", "datapilot")
    password = quote(os.getenv("DB_PASSWORD", ""), safe="")
    name = os.getenv("DB_NAME", "datapilot")
    return f"mysql+pymysql://{user}:{password}@{host}:{port}/{name}?charset=utf8mb4"


class Settings:
    def __init__(self) -> None:
        self.app_name = "DeviceLink"
        self.host = os.getenv("APP_HOST", "127.0.0.1")
        self.port = int(os.getenv("APP_PORT", "8000"))
        # Public base URL handed to device clients (--server). The exposure
        # layer (Funnel/Tunnel/ngrok/Nginx) decides how it reaches host:port.
        self.server_public_url = os.getenv("SERVER_PUBLIC_URL", f"http://127.0.0.1:{self.port}")
        self.database_url = _build_database_url()
        self.registration_code_ttl = int(os.getenv("REGISTRATION_CODE_TTL", "300"))
        self.heartbeat_interval = int(os.getenv("HEARTBEAT_INTERVAL", "15"))
        self.offline_threshold = int(os.getenv("OFFLINE_THRESHOLD", "45"))
        self.default_username = os.getenv("DEFAULT_USERNAME", "admin")
        # --- AgentHub V1.0 ---
        # Admin token for dashboard/management APIs (X-Admin-Token), separate
        # from device tokens. Empty disables admin auth (local dev only).
        self.admin_token = os.getenv("AGENTHUB_ADMIN_TOKEN", "")
        self.task_offline_max_wait = int(os.getenv("TASK_OFFLINE_MAX_WAIT", "600"))
        self.task_max_attempts = int(os.getenv("TASK_MAX_ATTEMPTS", "3"))
        # Main Agent LLM (OpenAI-compatible)
        self.openai_api_base = os.getenv("OPENAI_API_BASE", "") or None
        self.openai_api_key = os.getenv("OPENAI_API_KEY", "") or None
        self.agenthub_model = os.getenv("AGENTHUB_MODEL", "gpt-4o-mini")
        # Main Agent run loop
        self.agent_poll_interval = int(os.getenv("AGENT_POLL_INTERVAL", "2"))
        self.agent_max_wait = int(os.getenv("AGENT_MAX_WAIT", "300"))
        # --- MVP (DingTalk -> Agent -> yingdao.audit) ---
        # Background agent run waits at most this long for the task result
        # (yingdao.audit timeout is 1800s, so leave headroom).
        self.agent_run_max_wait = int(os.getenv("AGENT_RUN_MAX_WAIT", "1900"))
        # Device assumed when the message names none (MVP: fixed target).
        self.agent_default_device_name = os.getenv("AGENT_DEFAULT_DEVICE_NAME", "办公室电脑02")
        # DingTalk robot (Stream Mode). Empty disables the integration.
        self.dingtalk_client_id = os.getenv("DINGTALK_CLIENT_ID", "") or None
        self.dingtalk_client_secret = os.getenv("DINGTALK_CLIENT_SECRET", "") or None
        self.dingtalk_robot_code = os.getenv("DINGTALK_ROBOT_CODE", "") or None


settings = Settings()
