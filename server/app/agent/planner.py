"""Planner: user request -> structured Execution Plan.

Two implementations (PDF Phase 13):
- LLM planner: OpenAI-compatible chat model with a JSON-only contract.
- Rule-based planner: keyword fallback used when no API key is configured or
  the LLM call fails. The Agent loop must never block on LLM availability.
"""

import json
import logging
import re
from pathlib import Path

from sqlalchemy.orm import Session

from app.core.config import settings

logger = logging.getLogger(__name__)

_PROMPT_PATH = Path(__file__).resolve().parent / "prompts" / "planner_system.txt"

_PLANNER_SYSTEM_PROMPT = _PROMPT_PATH.read_text(encoding="utf-8") if _PROMPT_PATH.exists() else (
    "You are the AgentHub planner. Output STRICT JSON only: "
    '{"device_hint": string|null, "steps": [{"command": string, "params": object}], "rationale": string}'
)


class PlanError(Exception):
    """Planner could not produce a usable plan (validation-quality failure)."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


def build_plan(db: Session, user_request: str, context: dict, error: dict | None = None) -> dict:
    """Entry point: LLM first (if configured), rules as fallback.

    Raises PlanError when no usable plan can be produced (the graph turns
    this into a replan/human decision).
    """
    if settings.openai_api_key:
        try:
            plan = _llm_plan(user_request, context, error)
            if plan is not None:
                return _validate_plan(db, plan)
        except PlanError:
            raise
        except Exception:
            logger.exception("LLM planner failed, falling back to rule-based planner")
    return _rule_plan(db, user_request, context)


# --------------------------------------------------------------------- LLM


def _llm_plan(user_request: str, context: dict, error: dict | None) -> dict | None:
    from langchain_core.messages import HumanMessage, SystemMessage
    from langchain_openai import ChatOpenAI

    llm = ChatOpenAI(
        model=settings.agenthub_model,
        api_key=settings.openai_api_key,
        base_url=settings.openai_api_base,
        temperature=0,
    )
    snapshot = json.dumps(
        {
            "devices": context.get("devices", []),
            "commands": context.get("commands", []),
            "capabilities": context.get("capabilities", []),
            "previous_error": error,
        },
        ensure_ascii=False,
        default=str,
    )
    human = (
        f"User request: {user_request}\n\n"
        f"Runtime snapshot (JSON):\n{snapshot}\n\n"
        "Return ONLY the JSON plan."
    )
    response = llm.invoke([SystemMessage(content=_PLANNER_SYSTEM_PROMPT), HumanMessage(content=human)])
    return _parse_plan_text(getattr(response, "content", ""))


def _parse_plan_text(text: str) -> dict | None:
    if isinstance(text, list):  # some providers return content blocks
        text = "".join(block.get("text", "") for block in text if isinstance(block, dict))
    text = (text or "").strip()
    # strip markdown fences
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text, flags=re.IGNORECASE)
    try:
        data = json.loads(text)
    except ValueError:
        match = re.search(r"\{.*\}", text, flags=re.DOTALL)
        if not match:
            return None
        try:
            data = json.loads(match.group(0))
        except ValueError:
            return None
    return data if isinstance(data, dict) else None


def _validate_plan(db, plan: dict) -> dict:
    """Structural validation against the snapshot; raises PlanError on misuse."""
    steps = plan.get("steps")
    if not isinstance(steps, list) or not steps:
        raise PlanError(str(plan.get("rationale") or "planner produced no steps"))
    known = {c["command_name"]: c for c in _commands(db) if c["enabled"]}
    clean_steps = []
    for raw in steps:
        if not isinstance(raw, dict):
            raise PlanError("plan step must be an object")
        command = str(raw.get("command", "")).strip()
        if command not in known:
            raise PlanError(f"unknown or disabled command: {command!r}")
        params = raw.get("params") or {}
        if not isinstance(params, dict):
            raise PlanError(f"params for {command} must be an object")
        clean_steps.append({"command": command, "params": params})
    hint = plan.get("device_hint")
    if hint is not None and not isinstance(hint, str):
        hint = None
    return {
        "device_hint": (hint or None),
        "steps": clean_steps,
        "rationale": str(plan.get("rationale") or ""),
    }


def _commands(db: Session) -> list[dict]:
    from app.agent.tools import list_commands

    return list_commands(db)


# ------------------------------------------------------------------- rules


def _rule_plan(db: Session, user_request: str, context: dict) -> dict:
    text = (user_request or "").lower().strip()
    commands = [c for c in context.get("commands", []) if c.get("enabled")]

    matched = _match_command(text, commands)
    if matched is None:
        names = ", ".join(c["command_name"] for c in commands) or "(none)"
        raise PlanError(f"no registered command matches the request; available: {names}")

    step = {"command": matched["command_name"], "params": _extract_params(matched, user_request)}
    hint = _extract_device_hint(text, context)
    return {
        "device_hint": hint,
        "steps": [step],
        "rationale": f"rule-based match on command '{matched['command_name']}'",
    }


def _match_command(text: str, commands: list[dict]) -> dict | None:
    # 1. exact command name in the text
    for c in commands:
        if c["command_name"].lower() in text:
            return c
    # 2. name tokens (echo -> "echo", python.demo -> "python demo")
    for c in commands:
        tokens = [t for t in re.split(r"[._\-]+", c["command_name"].lower()) if t]
        if len(tokens) > 1 and all(t in text for t in tokens):
            return c
    # 3. description keywords
    for c in commands:
        words = [w for w in re.split(r"[^a-z\u4e00-\u9fff]+", str(c.get("description", "")).lower()) if len(w) > 1]
        if words:
            hits = sum(1 for w in set(words) if w in text)
            if hits >= max(1, len(set(words)) // 2):
                return c
    return None


def _extract_params(command: dict, user_request: str) -> dict:
    name = command["command_name"]
    schema = command.get("params_schema") or {}
    properties = schema.get("properties", {}) if isinstance(schema, dict) else {}

    if name == "echo":
        quoted = re.search(r"['\"“”](.+?)['\"“”]", user_request, flags=re.DOTALL)
        if quoted:
            return {"message": quoted.group(1).strip()}
        # text after the command mention, else the whole request
        lowered = user_request.lower()
        idx = lowered.find("echo")
        remainder = user_request[idx + len("echo"):].strip(" :：,，。.") if idx >= 0 else user_request
        return {"message": (remainder or user_request).strip()[:500]}

    if name == "python.demo":
        fence = re.search(r"```(?:python)?\s*\n(.*?)```", user_request, flags=re.DOTALL)
        if fence:
            return {"code": fence.group(1).strip()}
        return {"code": "print('hello from AgentHub')"}

    # generic: fill required string fields from the request when obvious
    params: dict = {}
    required = schema.get("required", []) if isinstance(schema, dict) else []
    for field in required:
        prop = properties.get(field, {})
        if prop.get("type") == "string":
            params[field] = user_request.strip()[:500]
    return params


def _extract_device_hint(text: str, context: dict) -> str | None:
    devices = context.get("devices", [])
    for d in devices:
        if d["device_id"].lower() in text:
            return d["device_id"]
    for d in devices:
        name = str(d.get("name") or "").lower()
        if len(name) >= 2 and name in text:
            return d["name"]
    return None
