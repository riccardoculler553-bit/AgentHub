"""MVP agent schemas (PDF §19/§23/§59).

Deliberately tiny: the LLM (when configured) can only ever produce these
shapes, and the command is validated against the AgentTool registry - it can
never invent a shell command or pick an arbitrary device id.
"""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ExecutionIntent(BaseModel):
    """Structured output of AnalyzeRequest (PDF §21/§23)."""

    intent: Literal["run_command", "unsupported"] = "run_command"
    device_name: str = Field(default="", max_length=100)
    command: str = Field(default="yingdao.audit", max_length=128)


# ---------------------------------------------------------------- MVP API


class AgentMessageIn(BaseModel):
    """Simulated / relayed user request (PDF §94)."""

    text: str = Field(min_length=1, max_length=2000)
    channel: str = Field(default="api", max_length=32)
    message_id: str = Field(default="", max_length=128)
    conversation_id: str = Field(default="", max_length=128)
    sender_id: str = Field(default="", max_length=128)
    sender_name: str | None = Field(default=None, max_length=128)


class AgentMessageOut(BaseModel):
    run_id: str
    status: str = "RUNNING"


class ResumeMessageIn(BaseModel):
    """User follow-up that resumes a WAITING_USER run (PDF §91/§133)."""

    text: str = Field(min_length=1, max_length=2000)


class AgentRunRecordOut(BaseModel):
    run_id: str
    channel: str
    conversation_id: str
    sender_id: str
    sender_name: str | None = None
    # NULL when absent (API channel) - exempt from the idempotency unique key
    message_id: str | None = None
    input_text: str
    status: str
    task_id: str | None = None
    ack_reply: str
    final_reply: str | None = None
    error: str | None = None
    tool_call_count: int = 0  # V1.2 §125: kept on the run for audit
    created_at: datetime
    finished_at: datetime | None = None
