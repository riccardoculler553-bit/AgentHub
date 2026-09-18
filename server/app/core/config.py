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
        # 业务->命令->设备路由表 JSON; empty = built-in defaults
        self.agent_tools_config = os.getenv("AGENT_TOOLS_CONFIG", "") or None
        # --- V1.2 Tool-Using Agent ---
        # mvp = fixed-pipeline MVP agent handles DingTalk messages;
        # tool_agent = V1.2 LangGraph tool-using agent (PDF §97).
        self.agent_mode = os.getenv("AGENT_MODE", "mvp")
        # Per-run tool call budget (PDF §41: max_tool_calls = 8)
        self.agent_max_tool_calls = int(os.getenv("AGENT_MAX_TOOL_CALLS", "8"))
        # Transient LLM failures get limited retries (PDF §127)
        self.agent_max_llm_retries = int(os.getenv("AGENT_MAX_LLM_RETRIES", "2"))
        # execute_command/retry_task wait at most this long for a terminal
        # task state inside the tool call; past that the tool reports
        # status=RUNNING and the LLM answers accordingly (PDF §45).
        self.agent_tool_wait_max = int(os.getenv("AGENT_TOOL_WAIT_MAX", "1900"))
        # Require user confirmation before ACTION tools (execute_command).
        # WRITE tools (retry/cancel) always confirm (PDF §56-§57).
        self.agent_confirm_actions = os.getenv("AGENT_CONFIRM_ACTIONS", "false").lower() in ("1", "true", "yes")
        # Reasoning-loop wall-clock guard (PDF §42): when exceeded the agent
        # finishes with whatever it has instead of looping further.
        self.agent_max_runtime = int(os.getenv("AGENT_MAX_RUNTIME", "120"))
        # DingTalk robot (Stream Mode). Empty disables the integration.
        self.dingtalk_client_id = os.getenv("DINGTALK_CLIENT_ID", "") or None
        self.dingtalk_client_secret = os.getenv("DINGTALK_CLIENT_SECRET", "") or None
        self.dingtalk_robot_code = os.getenv("DINGTALK_ROBOT_CODE", "") or None
        # Per-group throttle; <=0 disables (dingtalk channel only)
        self.dingtalk_throttle_seconds = int(os.getenv("DINGTALK_THROTTLE_SECONDS", "30"))
        # --- V1.3 Workflow Engine ---
        # Safety-net sweep interval (task-terminal observer does real-time work)
        self.workflow_monitor_interval = float(os.getenv("WORKFLOW_MONITOR_INTERVAL", "5"))
        # --- V1.4 Capability Runtime ---
        # Local object storage for capability packages + artifacts (§31: files
        # live on disk, MySQL keeps metadata; swap for OSS/MinIO later).
        self.storage_dir = Path(
            os.getenv("STORAGE_DIR", str(Path(__file__).resolve().parents[2] / "storage"))
        )
        # Task timeout for CAPABILITY tasks (no Command row to inherit from).
        self.capability_default_timeout = int(os.getenv("CAPABILITY_DEFAULT_TIMEOUT", "1800"))
        # Phase 2: how long (seconds) the monitor keeps re-delivering stop
        # instructions for a terminal task whose attempt is still live before
        # closing that attempt STALE.
        self.task_cancel_resend_window = int(os.getenv("TASK_CANCEL_RESEND_WINDOW", "600"))
        # Worker-side package pull retry budget (§67: 最多重试 2 次).
        self.capability_pull_max_retries = int(os.getenv("CAPABILITY_PULL_MAX_RETRIES", "2"))


settings = Settings()
