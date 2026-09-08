"""Unit tests for V1.2 Agent contracts (PDF Phase 1 / §167).

AgentState / AgentContext / AgentDecision / ToolResult must all validate and
round-trip; these types are the boundary every other layer depends on.
"""

import pytest
from pydantic import ValidationError

from app.agent.core.context import AgentContext
from app.agent.core.state import AgentState
from app.agent.llm.schemas import AgentDecision
from app.agent.tools.base import RiskLevel, ToolErrorCodes, ToolResult


# ----------------------------------------------------------------- ToolResult

def test_tool_result_ok_shape():
    r = ToolResult.ok({"task_id": "t1", "status": "FAILED"}, tool_call_id="call_001", tool_name="get_task_detail")
    assert r.success is True
    assert r.data == {"task_id": "t1", "status": "FAILED"}
    assert r.error_code is None
    assert r.tool_call_id == "call_001"
    assert r.tool_name == "get_task_detail"
    assert r.model_dump()["success"] is True


def test_tool_result_fail_shape():
    r = ToolResult.fail(ToolErrorCodes.TASK_NOT_FOUND, "任务不存在", tool_call_id="call_002", tool_name="get_task_detail")
    assert r.success is False
    assert r.error_code == "TASK_NOT_FOUND"
    assert r.message == "任务不存在"
    assert r.data is None


def test_tool_result_with_call_stamps_identity():
    r = ToolResult.ok({"x": 1})
    r.with_call("call_003", "list_devices")
    assert r.tool_call_id == "call_003" and r.tool_name == "list_devices"


def test_tool_result_requires_success_field():
    with pytest.raises(ValidationError):
        ToolResult(data={})  # type: ignore[call-arg]


def test_risk_levels_are_read_write_action():
    assert [lv.value for lv in RiskLevel] == ["READ", "WRITE", "ACTION"]


# --------------------------------------------------------------- AgentDecision

def test_agent_decision_accepts_valid_actions():
    d = AgentDecision(action="tool_call", tool_name="get_recent_tasks", tool_args={"limit": 5})
    assert d.is_tool_call() and not d.is_finish()
    assert AgentDecision(action="ask_user", answer="请问要运行哪台电脑？").is_ask_user()
    assert AgentDecision(action="finish", answer="任务失败原因是影刀启动失败").is_finish()


def test_agent_decision_rejects_unknown_action():
    with pytest.raises(ValidationError):
        AgentDecision(action="run_sql")  # type: ignore[arg-type]


def test_agent_decision_defaults():
    d = AgentDecision(action="finish")
    assert d.tool_args == {} and d.tool_name is None


# ---------------------------------------------------------------- AgentContext

def test_agent_context_round_trip():
    ctx = AgentContext(
        user_id="u1",
        conversation_id="cid-100",
        channel="dingtalk",
        current_device_name="办公室电脑02",
        current_task_id="task-001",
        recent_tasks=[{"task_id": "task-001", "status": "FAILED"}],
        pending_confirmation={"tool_name": "retry_task", "tool_args": {"task_id": "task-001"}},
    )
    restored = AgentContext.from_dict(ctx.to_dict())
    assert restored == ctx


def test_agent_context_from_none_and_unknown_keys():
    assert AgentContext.from_dict(None) == AgentContext()
    # unknown keys (e.g. from older persisted states) are dropped, not fatal
    ctx = AgentContext.from_dict({"user_id": "u1", "obsolete_field": 1})
    assert ctx.user_id == "u1"
    assert not hasattr(ctx, "obsolete_field")


# ------------------------------------------------------------------ AgentState

def test_agent_state_declared_contract_keys():
    expected = {
        "run_id", "user_request", "channel", "conversation_id", "sender_id",
        "resumed", "user_reply", "context", "plan", "current_goal",
        "tool_name", "tool_args", "tool_result", "observations", "decision",
        "tool_call_count", "llm_retry_count", "started_at",
        "error", "final_answer", "reply",
        "paused",  # V1.2 Phase 6: ask_user parks the run in WAITING_USER
        "confirmed",  # V1.2 Phase 7: resume-after-confirmation skips llm_decide
    }
    assert expected == set(AgentState.__annotations__.keys())


def test_agent_state_total_false_allows_partial():
    # total=False: any subset is a valid initial state
    partial: AgentState = {"user_request": "运行审单", "tool_call_count": 0}
    assert partial["user_request"] == "运行审单"
