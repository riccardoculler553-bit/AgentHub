"""WAITING_USER + resume tests (PDF Phase 6 / §46-§48, §133-§134, §172).

Round 1 parks the run on ask_user; the serialized AgentState lands in
agent_runs.state_json; the user's follow-up resumes the SAME run through the
real graph - no LangGraph checkpointer involved.
"""

import pytest

from _agentloop import ScriptedLLM, dec, probe_tool

from app.agent.core.runner import AgentRunner
from app.agent.graph.nodes import AGENT_MAX_TOOL_CALLS
from app.agent.runs import STATUS_WAITING_USER, AgentRunService
from app.agent.tools.registry import ToolRegistry
from app.db.database import SessionLocal


def _create_run(conversation_id: str = "conv-1", sender_id: str = "user-1", text: str = "帮我重新运行。") -> str:
    svc = AgentRunService(SessionLocal())
    run = svc.create(
        channel="dingtalk",
        message_id="",
        conversation_id=conversation_id,
        sender_id=sender_id,
        sender_name=None,
        input_text=text,
    )
    return run.run_id


@pytest.mark.anyio
async def test_ask_user_parks_run_and_persists_state():
    reg = ToolRegistry()
    reg.register(probe_tool())
    llm = ScriptedLLM(dec(action="ask_user", answer="请问要运行哪台电脑？"))
    run_id = _create_run()

    final = await AgentRunner(registry=reg, llm=llm).run(
        "帮我重新运行。", channel="dingtalk", conversation_id="conv-1", sender_id="user-1"
    )
    assert final["paused"] is True
    assert final["reply"] == "请问要运行哪台电脑？"
    assert final["error"] is None  # §47: asking is NOT an error

    svc = AgentRunService(SessionLocal())
    svc.mark_waiting_user(run_id, final, final["reply"])
    row = svc.get(run_id)
    assert row.status == STATUS_WAITING_USER
    assert row.final_reply == "请问要运行哪台电脑？"
    # §134: plain JSON state, no checkpointer - it must round-trip
    restored = svc.load_state(run_id)
    assert restored["run_id"] == final["run_id"]
    assert restored["context"]["conversation_id"] == "conv-1"
    assert restored["tool_call_count"] == 0


@pytest.mark.anyio
async def test_resume_completes_run_with_supplement():
    reg = ToolRegistry()
    reg.register(probe_tool())
    ask_llm = ScriptedLLM(dec(action="ask_user", answer="请问要运行哪台电脑？"))
    run_id = _create_run()
    parked = await AgentRunner(registry=reg, llm=ask_llm).run(
        "帮我重新运行。", channel="dingtalk", conversation_id="conv-1", sender_id="user-1"
    )
    svc = AgentRunService(SessionLocal())
    svc.mark_waiting_user(run_id, parked, parked["reply"])

    resume_llm = ScriptedLLM(
        dec(action="tool_call", tool_name="probe", tool_args={"text": "办公室02"}),
        dec(action="finish", answer="已在办公室电脑02重新执行，任务成功。"),
    )
    state = svc.load_state(run_id)
    final = await AgentRunner(registry=reg, llm=resume_llm).resume(state, "办公室02。")

    assert final["resumed"] is True
    assert final["user_reply"] == "办公室02。"
    assert final["paused"] is False
    assert final["reply"] == "已在办公室电脑02重新执行，任务成功。"
    # the resumed prompt must carry the original request AND the supplement
    human = resume_llm.prompts[0][1].content
    assert "帮我重新运行" in human and "用户补充：办公室02" in human
    # close the lifecycle: WAITING_USER -> SUCCESS is allowed (§48)
    svc.finish(run_id, status="SUCCESS", final_reply=final["reply"])
    assert svc.get(run_id).status == "SUCCESS"
    assert svc.find_resumable("conv-1", "user-1") is None


@pytest.mark.anyio
async def test_resume_keeps_tool_call_budget():
    """§41 is per AgentRun: a resumed run inherits the spent budget."""
    reg = ToolRegistry()
    reg.register(probe_tool())
    llm = ScriptedLLM(
        dec(action="tool_call", tool_name="probe"),
        dec(action="tool_call", tool_name="probe"),
        dec(action="ask_user", answer="还要什么？"),
    )
    run_id = _create_run()
    parked = await AgentRunner(registry=reg, llm=llm, max_tool_calls=3).run("探测")
    assert parked["tool_call_count"] == 2 and parked["paused"] is True

    svc = AgentRunService(SessionLocal())
    svc.mark_waiting_user(run_id, parked, parked["reply"])
    assert svc.get(run_id).tool_call_count == 2

    # §41 ">" semantics + carried budget: the 3rd call runs, the 4th is
    # policy-rejected (the resumed policy inherits the 2 spent calls), then
    # the backstop guard force-finishes the run.
    resume_llm = ScriptedLLM(
        dec(action="tool_call", tool_name="probe"),
        dec(action="tool_call", tool_name="probe"),
    )
    final = await AgentRunner(registry=reg, llm=resume_llm, max_tool_calls=3).resume(
        svc.load_state(run_id), "继续"
    )
    assert final["tool_call_count"] == 4
    assert (final["error"] or {}).get("error_code") == AGENT_MAX_TOOL_CALLS
    # §133: resume restores the parked observations and keeps appending -
    # [成功, 成功, 3rd 成功, 4th 被拒], so the rejection is the last fact.
    assert len(final["observations"]) == 4
    assert "MAX_CALLS_EXCEEDED" in final["observations"][-1]["fact"]


@pytest.mark.anyio
async def test_find_resumable_returns_latest_waiting_run():
    reg = ToolRegistry()
    reg.register(probe_tool())
    svc = AgentRunService(SessionLocal())

    run_1 = _create_run(text="第一次")
    run_2 = _create_run(text="第二次")
    assert svc.find_resumable("conv-1", "user-1") is None

    runner = AgentRunner(
        registry=reg,
        llm=ScriptedLLM(dec(action="ask_user", answer="哪个？"), dec(action="ask_user", answer="哪个？")),
    )
    parked = await runner.run("第一次")
    svc.mark_waiting_user(run_1, parked, parked["reply"])
    assert svc.find_resumable("conv-1", "user-1").run_id == run_1

    parked2 = await runner.run("第二次")
    svc.mark_waiting_user(run_2, parked2, parked2["reply"])
    assert svc.find_resumable("conv-1", "user-1").run_id == run_2

    # a finished run no longer matches; the older waiting one resurfaces
    svc.finish(run_2, status="FAILED", final_reply="失败", error="boom")
    assert svc.find_resumable("conv-1", "user-1").run_id == run_1


@pytest.mark.anyio
async def test_terminal_run_never_parks_again():
    reg = ToolRegistry()
    reg.register(probe_tool())
    svc = AgentRunService(SessionLocal())
    run_id = _create_run()
    svc.finish(run_id, status="FAILED", final_reply="失败", error="boom")

    svc.mark_waiting_user(run_id, {"tool_call_count": 1}, "请问？")
    row = svc.get(run_id)
    assert row.status == "FAILED"
    assert row.state_json is None  # never polluted by the late park

    svc.save_state(run_id, {"tool_call_count": 2})
    assert svc.get(run_id).state_json is None
