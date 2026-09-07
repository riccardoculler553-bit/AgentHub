"""AnalyzeRequest: user text -> ExecutionIntent (PDF §21-§24).

Rules first (deterministic, offline-safe); LLM structured output only when an
OpenAI-compatible key is configured. MVP commands are whitelisted in
COMMAND_KEYWORDS - anything else becomes intent="unsupported".
"""

import json
import logging

from app.agent.mvp.schemas import ExecutionIntent
from app.core.config import settings

logger = logging.getLogger(__name__)

# command whitelist: keyword that triggers it inside the message
COMMAND_KEYWORDS: dict[str, tuple[str, ...]] = {
    "yingdao.audit": ("审单",),
}


def _rule_intent(text: str) -> tuple[str | None, str]:
    """(command, remaining device mention) matched by keywords."""
    lowered = text.lower()
    for command, keywords in COMMAND_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in lowered:
                return command, text
    return None, text


def _extract_device_name(text: str, device_names: list[str]) -> str | None:
    """Longest device name mentioned in the text (exact substring match)."""
    lowered = text.lower()
    best: str | None = None
    for name in device_names:
        if name and name.lower() in lowered:
            if best is None or len(name) > len(best):
                best = name
    return best


def analyze(text: str, device_names: list[str]) -> ExecutionIntent | None:
    """Rule-based intent. Returns None when the command is not recognized."""
    command, _ = _rule_intent(text)
    if command is None:
        return None
    return ExecutionIntent(
        intent="run_command",
        device_name=_extract_device_name(text, device_names) or "",
        command="yingdao.audit",  # narrow type for the schema
    )


async def analyze_with_llm(text: str, device_names: list[str]) -> ExecutionIntent | None:
    """LLM structured output first (when configured), rules as fallback.

    The prompt pins the command whitelist so the model cannot invent one.
    """
    if settings.openai_api_key:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            from langchain_openai import ChatOpenAI

            llm = ChatOpenAI(
                model=settings.agenthub_model,
                api_key=settings.openai_api_key,
                base_url=settings.openai_api_base,
                temperature=0,
            )
            structured = llm.with_structured_output(ExecutionIntent)
            system = (
                "You are the AgentHub MVP intent parser. Map the user request to "
                'ExecutionIntent. command MUST be "yingdao.audit" when the user asks '
                "to run an audit/审单 job; otherwise intent=\"unsupported\". "
                "device_name must be copied verbatim from the user text when they "
                "name a device, otherwise empty string."
            )
            result = await structured.ainvoke(
                [SystemMessage(content=system), HumanMessage(content=text)]
            )
            if isinstance(result, ExecutionIntent):
                if result.intent == "run_command" and not result.device_name:
                    result.device_name = _extract_device_name(text, device_names) or ""
                return result
        except Exception:
            logger.exception("LLM intent analysis failed, falling back to rules")
    return analyze(text, device_names)


def intent_to_json(intent: ExecutionIntent) -> str:
    return json.dumps(intent.model_dump(), ensure_ascii=False)
