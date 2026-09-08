"""LLMService: decision requests with bounded retries (PDF §71-§72, §127-§128).

Planner / graph nodes depend on this service, never on ChatOpenAI directly,
so the model is swappable and tests inject a fake client.

Retry policy:
- LLMTransientError (429/5xx/timeout): up to max_llm_retries extra attempts
  (settings default 2, PDF §127).
- LLMOutputError (unparseable answer): exactly one re-request; still invalid
  -> raise, the run FAILS - never guess model output (PDF §128).
"""

from typing import Any, TypeVar

from pydantic import BaseModel

from app.agent.llm.client import LLMClient
from app.agent.llm.errors import LLMError, LLMOutputError, LLMTransientError
from app.agent.llm.schemas import AgentDecision
from app.core.config import settings

T = TypeVar("T", bound=BaseModel)


class LLMService:
    def __init__(
        self,
        client: LLMClient | None = None,
        *,
        max_retries: int | None = None,
    ) -> None:
        self.client = client or LLMClient.from_settings()
        if max_retries is None:
            max_retries = settings.agent_max_llm_retries
        self.max_retries = max(0, max_retries)

    async def decide(self, messages: list[Any]) -> AgentDecision:
        """The llm_decide node contract: messages -> AgentDecision."""
        return await self.request(AgentDecision, messages)

    async def request(self, schema: type[T], messages: list[Any]) -> T:
        attempts = 0
        parse_retried = False
        last: LLMError | None = None
        while attempts <= self.max_retries:
            attempts += 1
            try:
                return await self.client.ainvoke_structured(schema, messages)
            except LLMOutputError as exc:
                if parse_retried:
                    raise  # PDF §128: still invalid -> run FAILED
                parse_retried = True
                last = exc
            except LLMTransientError as exc:
                last = exc  # budget bounded by attempts <= max_retries + 1
        assert last is not None
        raise last
