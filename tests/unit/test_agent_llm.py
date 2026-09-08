"""LLM layer contract tests (PDF Phase 4 / §127-§128).

A scripted FakeLLMClient stands in for the provider: the service must deliver
an AgentDecision exactly as scripted, retry transient failures up to
max_llm_retries, re-request exactly once on unparseable output, and never
guess when the output stays invalid.
"""

import pytest
from langchain_core.exceptions import OutputParserException
from pydantic import ValidationError

from app.agent.llm.client import LLMClient, map_provider_exception
from app.agent.llm.errors import LLMOutputError, LLMTransientError
from app.agent.llm.schemas import AgentDecision
from app.agent.llm.service import LLMService


class FakeLLMClient:
    """Scripted ainvoke_structured: pops one outcome per call."""

    def __init__(self, *outcomes) -> None:
        self.outcomes = list(outcomes)
        self.calls: list[tuple[type, list]] = []

    async def ainvoke_structured(self, schema, messages):
        self.calls.append((schema, messages))
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def _decision(**kwargs) -> AgentDecision:
    return AgentDecision(**kwargs)


@pytest.mark.anyio
async def test_decide_returns_tool_call_verbatim():
    d = _decision(
        action="tool_call",
        tool_name="get_recent_tasks",
        tool_args={"device_name": "办公室电脑02", "limit": 5},
        reason="查最近任务",
    )
    svc = LLMService(FakeLLMClient(d), max_retries=2)
    out = await svc.decide([("system", "sys"), ("human", "hi")])
    assert out is d  # scripted object passes through untouched
    fake = svc.client
    assert len(fake.calls) == 1
    schema, messages = fake.calls[0]
    assert schema is AgentDecision and len(messages) == 2


@pytest.mark.anyio
async def test_decide_ask_user_and_finish_pass_through():
    ask = _decision(action="ask_user", answer="请问要在哪台电脑上运行？")
    svc = LLMService(FakeLLMClient(ask), max_retries=2)
    assert (await svc.decide([])).is_ask_user()

    fin = _decision(action="finish", answer="任务失败：影刀启动失败")
    svc = LLMService(FakeLLMClient(fin), max_retries=2)
    assert (await svc.decide([])).is_finish()


def test_decision_rejects_smuggled_fields():
    """PDF §21/§121: SQL/shell/paths have no field to live in - extra keys
    are a schema error, which downstream surfaces as an output error."""
    with pytest.raises(ValidationError):
        AgentDecision.model_validate(
            {
                "action": "tool_call",
                "tool_name": "execute_command",
                "sql": "UPDATE tasks SET status='SUCCESS'",
            }
        )


def test_tool_call_without_tool_name_rejected():
    with pytest.raises(ValidationError):
        AgentDecision(action="tool_call", tool_args={"command": "echo"})


@pytest.mark.anyio
async def test_unparseable_output_rerequested_once_then_succeeds():
    """PDF §128: parse failure -> re-request once."""
    good = _decision(action="finish", answer="done")
    fake = FakeLLMClient(LLMOutputError("no tool call in answer"), good)
    out = await LLMService(fake, max_retries=2).decide([])
    assert out.answer == "done"
    assert len(fake.calls) == 2


@pytest.mark.anyio
async def test_unparseable_output_twice_raises_not_guessed():
    fake = FakeLLMClient(
        LLMOutputError("bad"), LLMOutputError("still bad"), _decision(action="finish")
    )
    with pytest.raises(LLMOutputError):
        await LLMService(fake, max_retries=2).decide([])
    # exactly one re-request; the scripted good outcome is never consumed
    assert len(fake.calls) == 2


@pytest.mark.anyio
async def test_transient_failures_retried_within_budget():
    """PDF §127: 429/500/timeout get up to max_llm_retries extra attempts."""
    good = _decision(action="finish", answer="ok")
    fake = FakeLLMClient(LLMTransientError("429"), LLMTransientError("500"), good)
    out = await LLMService(fake, max_retries=2).decide([])
    assert out.answer == "ok"
    assert len(fake.calls) == 3  # 1 initial + 2 retries


@pytest.mark.anyio
async def test_transient_failures_exhausted_raises():
    fake = FakeLLMClient(*[LLMTransientError("timeout")] * 3, _decision(action="finish"))
    with pytest.raises(LLMTransientError):
        await LLMService(fake, max_retries=2).decide([])
    assert len(fake.calls) == 3


@pytest.mark.anyio
async def test_parse_retry_and_transient_share_the_call_budget():
    """One parse re-request plus transient retries all live inside the same
    bounded loop - the run can never call the LLM more than max_retries+1."""
    fake = FakeLLMClient(
        LLMTransientError("429"), LLMOutputError("bad"), _decision(action="finish", answer="x")
    )
    out = await LLMService(fake, max_retries=2).decide([])
    assert out.answer == "x"
    assert len(fake.calls) == 3


def test_provider_exception_mapping():
    """OutputParserException -> output error; everything else -> transient."""
    assert isinstance(map_provider_exception(OutputParserException("x")), LLMOutputError)
    assert isinstance(map_provider_exception(RuntimeError("boom")), LLMTransientError)


def test_client_from_settings_reads_config(monkeypatch):
    from app.core.config import settings

    monkeypatch.setattr(settings, "openai_api_key", "k-test", raising=False)
    monkeypatch.setattr(settings, "openai_api_base", "https://open.bigmodel.cn/api/paas/v4", raising=False)
    monkeypatch.setattr(settings, "agenthub_model", "glm-4-flash", raising=False)
    c = LLMClient.from_settings()
    assert (c.api_key, c.base_url, c.model) == (
        "k-test",
        "https://open.bigmodel.cn/api/paas/v4",
        "glm-4-flash",
    )
