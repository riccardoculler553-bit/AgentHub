"""AgentTool registry: 业务(用户语言) -> command -> 设备路由 (PDF §19/§23).

AgentHub 里 Agent 的"工具"不是 shell 命令, 而是注册在 Command Registry 的
业务命令。这里维护一张小表:

    {name, label, keywords, command, devices}

- label/keywords: 用户侧叫法和触发词 (多命令意图识别)
- devices: 允许承接该业务的设备名白名单, 按优先级排序 (空列表 = 自动发现
  所有上报了该能力的设备)

配置来源: AGENT_TOOLS_CONFIG 指向的 JSON 文件 (缺省 server/config/
agent_tools.json); 文件不存在或解析失败时用内置默认表, 保证零配置可跑。
这样"哪台电脑能运行什么程序"完全是服务端配置, 不用改代码。
"""

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

from app.core.config import settings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class AgentTool:
    name: str
    label: str  # 业务名, 用于用户回复
    command: str  # Command Registry 里的 command_name
    keywords: tuple[str, ...] = ()
    devices: tuple[str, ...] = ()  # 设备名白名单, 优先级从高到低


# 内置默认: 只有审单一个业务, 设备自动发现
_DEFAULT_TOOLS = (
    AgentTool(
        name="audit",
        label="审单",
        command="yingdao.audit",
        keywords=("审单",),
        devices=(),
    ),
)


def _config_path() -> Path | None:
    raw = getattr(settings, "agent_tools_config", "") or ""
    if raw:
        return Path(raw)
    # repo layout: server/app/core/config.py -> server/config/agent_tools.json
    default = Path(__file__).resolve().parents[3] / "config" / "agent_tools.json"
    return default if default.is_file() else None


def _parse_tool(raw: dict) -> AgentTool | None:
    name = str(raw.get("name", "")).strip()
    command = str(raw.get("command", "")).strip()
    if not name or not command:
        return None
    keywords = tuple(str(k).strip() for k in (raw.get("keywords") or []) if str(k).strip())
    devices = tuple(str(d).strip() for d in (raw.get("devices") or []) if str(d).strip())
    label = str(raw.get("label", "")).strip() or name
    return AgentTool(name=name, label=label, command=command, keywords=keywords, devices=devices)


def load_tools() -> tuple[AgentTool, ...]:
    """Load the tool table; falls back to the built-in default on any problem."""
    path = _config_path()
    if path is None:
        return _DEFAULT_TOOLS
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        tools = tuple(
            t for t in (_parse_tool(item) for item in (raw.get("tools") or [])) if t
        )
        if not tools:
            logger.warning("agent tools config %s is empty, using defaults", path)
            return _DEFAULT_TOOLS
        return tools
    except (OSError, ValueError) as exc:
        logger.warning("failed to load agent tools config %s: %s", path, exc)
        return _DEFAULT_TOOLS


class AgentToolRegistry:
    """Process-wide cache of the tool table (reload() re-reads the file)."""

    def __init__(self) -> None:
        self._tools: tuple[AgentTool, ...] | None = None

    def reload(self) -> None:
        self._tools = load_tools()

    @property
    def tools(self) -> tuple[AgentTool, ...]:
        if self._tools is None:
            self.reload()
        return self._tools or _DEFAULT_TOOLS

    def all_keywords(self) -> dict[str, tuple[str, ...]]:
        return {t.command: t.keywords for t in self.tools}

    def by_command(self, command: str) -> AgentTool | None:
        return next((t for t in self.tools if t.command == command), None)

    def default_device_name(self) -> str:
        return settings.agent_default_device_name


tool_registry = AgentToolRegistry()


def command_label(command: str) -> str:
    tool = tool_registry.by_command(command)
    return tool.label if tool else command
