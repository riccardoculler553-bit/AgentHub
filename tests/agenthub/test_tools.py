"""AgentTool registry + intent analyzer unit tests (PDF §19-§24)."""

import json

from app.agent.mvp.analyzer import analyze
from app.agent.mvp.tools import tool_registry


def test_default_registry_loads_audit_tool():
    tool_registry.reload()
    tool = tool_registry.by_command("yingdao.audit")
    assert tool is not None
    assert tool.label == "审单"
    assert "审单" in tool.keywords


def test_analyze_matches_keyword_and_device():
    intent = analyze("运行办公室电脑02的审单", ["办公室电脑02", "测试机A"])
    assert intent is not None
    assert intent.intent == "run_command"
    assert intent.command == "yingdao.audit"
    assert intent.device_name == "办公室电脑02"


def test_analyze_without_device_mention():
    intent = analyze("帮我审单", [])
    assert intent is not None
    assert intent.command == "yingdao.audit"
    assert intent.device_name == ""


def test_analyze_rejects_unknown_intent():
    assert analyze("今天天气怎么样", []) is None
    assert analyze("帮我订个会议室", []) is None


def test_analyze_longest_device_name_wins():
    intent = analyze("在测试机A上审单", ["测试机", "测试机A"])
    assert intent.device_name == "测试机A"


def test_registry_reloads_custom_config(tmp_path, monkeypatch):
    from app.core.config import settings

    config_file = tmp_path / "agent_tools.json"
    config_file.write_text(
        json.dumps(
            {
                "tools": [
                    {
                        "name": "pack",
                        "label": "打包",
                        "command": "yingdao.pack",
                        "keywords": ["打包"],
                        "devices": ["仓库电脑01"],
                    }
                ]
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(settings, "agent_tools_config", str(config_file))
    tool_registry.reload()
    tool = tool_registry.by_command("yingdao.pack")
    assert tool is not None
    assert tool.devices == ("仓库电脑01",)
    assert analyze("打包", []) is not None
    assert analyze("审单", []) is None  # custom config replaces defaults


def test_registry_falls_back_to_defaults_on_bad_config(tmp_path, monkeypatch):
    from app.core.config import settings

    config_file = tmp_path / "broken.json"
    config_file.write_text("{not json", encoding="utf-8")
    monkeypatch.setattr(settings, "agent_tools_config", str(config_file))
    tool_registry.reload()
    assert tool_registry.by_command("yingdao.audit") is not None
