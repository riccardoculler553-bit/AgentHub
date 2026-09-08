"""LLM client: the single place a chat model is instantiated (PDF §71).

Agent code never imports ChatOpenAI directly - graph nodes ask LLMService,
which asks this client, so swapping provider / mocking in tests touches one
file. Works with any OpenAI-compatible provider (OpenAI / GLM / DeepSeek /
Qwen) via base_url (PDF §70).

function_calling structured output is used deliberately: GLM-class models
append prose after raw JSON bodies, which breaks strict json_schema parsing,
while tool-call arguments arrive clean (prod lesson 2026-09-07).
"""

import logging
from typing import Any, TypeVar

from pydantic import BaseModel

from app.agent.llm.errors import LLMError, LLMOutputError, LLMTransientError
from app.core.config import settings

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


def map_provider_exception(exc: Exception) -> LLMError:
    """Classify a provider-side failure (PDF §127/§128).

    OutputParserException (incl. pydantic validation of the parsed tool args)
    means the model produced unusable output -> LLMOutputError. Everything
    else (429 / 5xx / timeout / connection / unknown SDK error) is treated as
    transient -> LLMTransientError; retries are bounded by LLMService anyway.
    """
    from langchain_core.exceptions import OutputParserException

    if isinstance(exc, OutputParserException):
        return LLMOutputError(str(exc))
    return LLMTransientError(f"{type(exc).__name__}: {exc}")


class LLMClient:
    def __init__(
        self,
        *,
        api_key: str | None,
        base_url: str | None,
        model: str,
        temperature: float = 0.0,
    ) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.model = model
        self.temperature = temperature

    @classmethod
    def from_settings(cls) -> "LLMClient":
        return cls(
            api_key=settings.openai_api_key,
            base_url=settings.openai_api_base,
            model=settings.agenthub_model,
        )

    async def ainvoke_structured(self, schema: type[T], messages: list[Any]) -> T:
        """messages -> schema instance through the function-calling channel."""
        from langchain_openai import ChatOpenAI

        llm = ChatOpenAI(
            model=self.model,
            api_key=self.api_key,
            base_url=self.base_url,
            temperature=self.temperature,
        )
        structured = llm.with_structured_output(schema, method="function_calling")
        try:
            result = await structured.ainvoke(messages)
        except Exception as exc:
            raise map_provider_exception(exc) from exc
        if not isinstance(result, schema):
            raise LLMOutputError(
                f"structured output returned {type(result).__name__}, expected {schema.__name__}"
            )
        return result
