"""Agent loop tests (PDF Phase 5 / §171).

A scripted FakeLLM drives the real graph: Tool Call -> Observe -> Evaluate ->
Continue -> END. Tools are in-memory probes (no business DB writes); the
policy chain, guards and routing are the production code paths.
"""

import pytest

from _agentloop import ScriptedLLM, dec, probe_tool

from app.agent.core.runner import AgentRunner
from app.agent.graph.nodes import AGENT_LLM_ERROR, AGENT_MAX_TOOL_CALLS, AGENT_TIMEOUT
from app.agent.llm.errors import LLMTransientError
from app.agent.tools.base import ToolErrorCodes
from app.agent.tools.registry import ToolRegistry


def _dec(**kwargs):
    return dec(**kwargs)


def _probe_tool(name: str = "probe", *, fail: tuple[str, str] | None = None):
    return probe_tool(name, fail=fail)


def _runner(llm: ScriptedLLM, registry: ToolRegistry, **kwargs) -> AgentRunner:
    return AgentRunner(registry=registry, llm=llm, **kwargs)


@pytest.mark.anyio
async def test_two_round_tool_loop_then_finish():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(
        _dec(action="tool_call", tool_name="probe", tool_args={"text": "第一轮"}),
        _dec(action="tool_call", tool_name="probe", tool_args={"text": "第二轮"}),
        _dec(action="finish", answer="两轮探测完成"),
    )
    final = await _runner(llm, reg).run("帮我探测两次")

    assert final["reply"] == "两轮探测完成"
    assert final["error"] is None
    assert final["tool_call_count"] == 2
    assert len(final["observations"]) == 2
    assert final["observations"][0]["fact"].startswith("工具 probe 成功")
    # PDF §40: the next decision round must see the prior observations
    third_human = llm.prompts[2][1].content
    assert "第一轮" in third_human and "第二轮" in third_human


@pytest.mark.anyio
async def test_prompt_carries_whitelist_rules_and_request():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(_dec(action="finish", answer="ok"))
    await _runner(llm, reg).run("你好，帮我看看状态")

    system, human = llm.prompts[0]
    assert "probe" in system.content
    assert "不允许伪造工具结果" in system.content
    assert "你好，帮我看看状态" in human.content
    assert "当前计划" in human.content


@pytest.mark.anyio
async def test_tool_failure_is_observation_not_crash():
    reg = ToolRegistry()
    reg.register(_probe_tool(fail=(ToolErrorCodes.TASK_NOT_FOUND, "task gone")))
    llm = ScriptedLLM(
        _dec(action="tool_call", tool_name="probe"),
        _dec(action="finish", answer="查询的任务不存在"),
    )
    final = await _runner(llm, reg).run("查一下 task-1")

    assert final["reply"] == "查询的任务不存在"
    assert "失败 TASK_NOT_FOUND" in final["observations"][0]["fact"]


@pytest.mark.anyio
async def test_unknown_tool_rejected_by_policy_then_finish():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(
        _dec(
            action="tool_call",
            tool_name="drop_table",
            tool_args={"sql": "DROP TABLE tasks"},
        ),
        _dec(action="finish", answer="没有这个能力"),
    )
    final = await _runner(llm, reg).run("把任务表清了")

    assert final["reply"] == "没有这个能力"
    assert "TOOL_NOT_FOUND" in final["observations"][0]["fact"]
    # a rejected call must never reach any tool handler
    assert final["tool_call_count"] == 1  # attempt counted, ledger empty


@pytest.mark.anyio
async def test_argument_smuggling_rejected_in_loop():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(
        _dec(action="tool_call", tool_name="probe", tool_args={"script_path": "D:\\evil.py"}),
        _dec(action="finish", answer="参数不合法，已拒绝"),
    )
    final = await _runner(llm, reg).run("帮我执行这个脚本")

    assert "INVALID_ARGS" in final["observations"][0]["fact"]
    assert final["reply"] == "参数不合法，已拒绝"


@pytest.mark.anyio
async def test_max_tool_calls_guard_forces_finish():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    # §41: the 3rd call runs; the 4th is rejected by the policy as an
    # observation; the backstop guard then force-finishes the run.
    llm = ScriptedLLM(
        _dec(action="tool_call", tool_name="probe"),
        _dec(action="tool_call", tool_name="probe"),
        _dec(action="tool_call", tool_name="probe"),
        _dec(action="tool_call", tool_name="probe"),
        _dec(action="finish", answer="收尾"),
    )
    final = await _runner(llm, reg, max_tool_calls=3).run("连续探测")

    assert (final["error"] or {}).get("error_code") == AGENT_MAX_TOOL_CALLS
    assert final["tool_call_count"] == 4
    assert "MAX_CALLS_EXCEEDED" in final["observations"][3]["fact"]
    assert "上限" in final["reply"]


@pytest.mark.anyio
async def test_runtime_guard_forces_finish():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(
        _dec(action="tool_call", tool_name="probe"),
        _dec(action="tool_call", tool_name="probe"),
    )
    final = await _runner(llm, reg, max_runtime=0).run("长时间任务")

    assert (final["error"] or {}).get("error_code") == AGENT_TIMEOUT
    assert final["tool_call_count"] == 1


@pytest.mark.anyio
async def test_ask_user_ends_run_with_question():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(_dec(action="ask_user", answer="请问要在哪台电脑上运行？"))
    final = await _runner(llm, reg).run("运行任务")

    assert final["reply"] == "请问要在哪台电脑上运行？"
    assert final["error"] is None


@pytest.mark.anyio
async def test_llm_error_short_circuits_to_friendly_reply():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(LLMTransientError("simulated 500"))
    final = await _runner(llm, reg).run("查询状态")

    assert (final["error"] or {}).get("error_code") == AGENT_LLM_ERROR
    assert "稍后再试" in final["reply"]


@pytest.mark.anyio
async def test_finish_without_answer_falls_back_to_facts():
    reg = ToolRegistry()
    reg.register(_probe_tool())
    llm = ScriptedLLM(
        _dec(action="tool_call", tool_name="probe", tool_args={"text": "x"}),
        _dec(action="finish"),  # no answer
    )
    final = await _runner(llm, reg).run("探测")

    assert "已完成查询" in final["reply"]
    assert "probe" in final["reply"]
