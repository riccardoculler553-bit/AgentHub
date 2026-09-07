"""AnalyzeRequest: user text -> ExecutionIntent (PDF §21-§24).

Rules first (deterministic, offline-safe): keyword match against the
AgentTool registry decides which business command the user wants, and a
device-name mention is extracted from the registered device list. LLM
structured output is only used when an OpenAI-compatible key is configured,
and its command is validated against the same registry - the model cannot
invent a command.
"""

import logging

from app.agent.mvp.schemas import ExecutionIntent
from app.agent.mvp.tools import tool_registry
from app.core.config import settings

logger = logging.getLogger(__name__)


def _match_command(text: str) -> str | None:
    """Return the command whose keyword appears in the text (exact for
    short keywords like 审单 - substring match would over-trigger)."""
    lowered = text.lower()
    for command, keywords in tool_registry.all_keywords().items():
        for kw in keywords:
            if kw.lower() in lowered:
                return command
    return None


def _extract_device_name(text: str, device_names: list[str]) -> str | None:
    """Longest registered device name mentioned in the text."""
    lowered = text.lower()
    best: str | None = None
    for name in device_names:
        if name and name.lower() in lowered:
            if best is None or len(name) > len(best):
                best = name
    return best


def analyze(text: str, device_names: list[str]) -> ExecutionIntent | None:
    """Rule-based intent. Returns None when no registered keyword matches."""
    command = _match_command(text)
    if command is None:
        return None
    return ExecutionIntent(
        intent="run_command",
        device_name=_extract_device_name(text, device_names) or "",
        command=command,
    )


async def analyze_with_llm(text: str, device_names: list[str]) -> ExecutionIntent | None:
    """LLM structured output first (when configured), rules as fallback."""
    if settings.openai_api_key:
        try:
            from langchain_core.messages import HumanMessage, SystemMessage
            from langchain_openai import ChatOpenAI

            catalog = "\n".join(
                f'- command "{t.command}" ({t.label}): trigger words {list(t.keywords)}'
                for t in tool_registry.tools
            )
            llm = ChatOpenAI(
                model=settings.agenthub_model,
                api_key=settings.openai_api_key,
                base_url=settings.openai_api_base,
                temperature=0,
            )
            # function_calling keeps args in the tool-call channel; GLM appends
            # prose after JSON bodies which breaks strict json_schema parsing.
            structured = llm.with_structured_output(
                ExecutionIntent, method="function_calling"
            )
            system = (
                "You are the AgentHub MVP intent parser. Map the user request to "
                "ExecutionIntent. Only these commands exist:\n"
                f"{catalog}\n"
                "If the request does not match any of them, set "
                'intent="unsupported". device_name must be copied verbatim from '
                "the user text when they name a device, otherwise empty string."
            )
            result = await structured.ainvoke(
                [SystemMessage(content=system), HumanMessage(content=text)]
            )
            if isinstance(result, ExecutionIntent):
                if result.intent == "run_command":
                    # validate against the registry; unknown command -> refuse
                    if tool_registry.by_command(result.command) is None:
                        return None
                    if not result.device_name:
                        result.device_name = _extract_device_name(text, device_names) or ""
                return result
        except Exception:
            logger.exception("LLM intent analysis failed, falling back to rules")
    return analyze(text, device_names)
