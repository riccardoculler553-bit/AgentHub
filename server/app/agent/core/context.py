"""AgentContext: business context the Agent currently reasons about (PDF §9).

NOT a memory store (PDF §49): it is rebuilt/refreshed per run from the
database via load_context, and persisted to agent_runs.state_json only so a
WAITING_USER run can resume (PDF §48). Task/Attempt state referenced here is
a *pointer* - real state must always be re-read through Tools (PDF §10).
"""

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass
class AgentContext:
    user_id: str = ""
    conversation_id: str = ""
    channel: str = "api"

    # pointers to the entities the agent is currently focused on
    current_device_id: str | None = None
    current_device_name: str | None = None
    current_task_id: str | None = None
    current_attempt_id: str | None = None

    # snapshots loaded by load_context (stale data is fine: pointers decide
    # what to re-query, facts always come from fresh Tool results)
    recent_tasks: list[dict] = field(default_factory=list)
    current_task: dict | None = None
    recent_events: list[dict] = field(default_factory=list)

    # confirmation flow (PDF §57): the WRITE/ACTION tool call awaiting the
    # user's yes/no, kept across resume as {"tool_name", "tool_args"}
    pending_confirmation: dict[str, Any] | None = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict | None) -> "AgentContext":
        if not data:
            return cls()
        known = {f for f in cls.__dataclass_fields__}  # type: ignore[attr-defined]
        return cls(**{k: v for k, v in data.items() if k in known})
