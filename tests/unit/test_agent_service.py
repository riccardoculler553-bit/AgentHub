"""AgentService lifecycle tests (PDF §91-§94, §133).

Intake idempotency, WAITING_USER auto-resume on the next conversation
message, cancel semantics and resume guards - all against the real
AgentRunService persistence and a scripted LLM.
"""

import asyncio

import pytest

from _agentloop import ScriptedLLM, dec, probe_tool

from app.agent.runs import STATUS_WAITING_USER, AgentRunService
from app.agent.service import AgentService, NotResumableError
from app.agent.tools.registry import ToolRegistry
from app.db.database import SessionLocal


def _service(llm, **kwargs) -> AgentService:
    reg = ToolRegistry()
    reg.register(probe_tool())
    return AgentService(hub=None, registry=reg, llm=llm, **kwargs)


def _row(run_id: str) -> dict:
    with SessionLocal() as db:
        row = AgentRunService(db).get(run_id)
        return {
            "status": row.status,
            "final_reply": row.final_reply,
            "error": row.error,
            "tool_call_count": row.tool_call_count,
        }


async def _wait(predicate, timeout: float = 5.0) -> bool:
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.02)
    return False


@pytest.mark.anyio
async def test_handle_message_runs_to_success():
    svc = _service(ScriptedLLM(dec(action="finish", answer="已完成查询。")))
    run_id = svc.handle_message(text="查一下", channel="api")

    assert await _wait(lambda: _row(run_id)["status"] == "SUCCESS")
    row = _row(run_id)
    assert row["final_reply"] == "已完成查询。"
    assert row["error"] is None


@pytest.mark.anyio
async def test_follow_up_message_resumes_waiting_run():
    llm = ScriptedLLM(
        dec(action="ask_user", answer="请问要运行哪台电脑？"),
        dec(action="tool_call", tool_name="probe", tool_args={"text": "办公室02"}),
        dec(action="finish", answer="已在办公室02执行完成。"),
    )
    svc = _service(llm)
    run_id = svc.handle_message(
        text="帮我重新运行", channel="dingtalk", conversation_id="conv-9", sender_id="u9"
    )
    assert await _wait(lambda: _row(run_id)["status"] == STATUS_WAITING_USER)

    # the next message in the SAME conversation resumes that run (PDF §48)
    run_id2 = svc.handle_message(
        text="办公室02", channel="dingtalk", conversation_id="conv-9", sender_id="u9"
    )
    assert run_id2 == run_id
    assert await _wait(lambda: _row(run_id)["status"] == "SUCCESS")
    row = _row(run_id)
    assert row["final_reply"] == "已在办公室02执行完成。"
    assert row["tool_call_count"] == 1


@pytest.mark.anyio
async def test_message_id_is_idempotent():
    svc = _service(ScriptedLLM(dec(action="finish", answer="ok")))
    first = svc.handle_message(
        text="hi", channel="dingtalk", message_id="msg-1", conversation_id="c", sender_id="u"
    )
    second = svc.handle_message(
        text="hi", channel="dingtalk", message_id="msg-1", conversation_id="c", sender_id="u"
    )
    assert first == second


@pytest.mark.anyio
async def test_cancel_parked_run_and_late_writes_never_overwrite():
    svc = _service(ScriptedLLM(dec(action="ask_user", answer="要什么？")))
    run_id = svc.handle_message(text="做事", channel="api")
    assert await _wait(lambda: _row(run_id)["status"] == STATUS_WAITING_USER)

    assert svc.cancel_run(run_id) == "CANCELLED"
    assert _row(run_id)["status"] == "CANCELLED"

    # a late graph finalize must not resurrect a cancelled run
    with SessionLocal() as db:
        AgentRunService(db).finish(run_id, status="SUCCESS", final_reply="late")
    assert _row(run_id)["status"] == "CANCELLED"


@pytest.mark.anyio
async def test_resume_guard_rejects_terminal_run():
    svc = _service(ScriptedLLM(dec(action="finish", answer="ok")))
    run_id = svc.start_run(text="直接完成")
    assert await _wait(lambda: _row(run_id)["status"] == "SUCCESS")
    with pytest.raises(NotResumableError):
        await svc.resume_run(run_id, "继续")


@pytest.mark.anyio
async def test_llm_crash_fails_the_run():
    from app.agent.llm.errors import LLMTransientError

    async def boom(messages):  # noqa: ARG001
        raise LLMTransientError("provider down")

    llm = ScriptedLLM()
    llm.decide = boom
    svc = _service(llm)
    run_id = svc.start_run(text="hello")
    assert await _wait(lambda: _row(run_id)["status"] == "FAILED")
    row = _row(run_id)
    assert row["error"] == "AGENT_LLM_ERROR"
    assert "暂时无法处理" in row["final_reply"]
