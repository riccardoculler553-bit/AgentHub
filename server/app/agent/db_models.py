"""AgentRun persistence: one row per user request (DingTalk message).

AgentRun = "one user request"; Task = "one business execution". They are
deliberately separate (MVP-Real §30-31): a run may fan out to multiple tasks
later, and a task survives server restarts while the run is the audit trail
of the conversation turn.

reply_webhook stores the DingTalk sessionWebhook so the final reply can be
sent back to the originating conversation after the background graph finishes.
"""

from datetime import datetime

from sqlalchemy import String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import utcnow


class AgentRun(Base):
    __tablename__ = "agent_runs"
    __table_args__ = (
        # V1.1 §21: DB-level idempotency for DingTalk retries / stream replays.
        # message_id is stored NULL when absent (API channel) - unique indexes
        # ignore NULLs, so API-created runs never collide.
        UniqueConstraint("channel", "message_id", name="uq_agent_run_message"),
    )

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    channel: Mapped[str] = mapped_column(String(32), default="dingtalk")
    conversation_id: Mapped[str] = mapped_column(String(128), default="")
    sender_id: Mapped[str] = mapped_column(String(128), default="")
    sender_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    message_id: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    input_text: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(16), default="RUNNING")  # RUNNING/WAITING_USER/SUCCESS/FAILED/CANCELLED
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    ack_reply: Mapped[str] = mapped_column(Text, default="")
    final_reply: Mapped[str | None] = mapped_column(Text, nullable=True)
    reply_webhook: Mapped[str | None] = mapped_column(Text, nullable=True)
    error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    # V1.2 (PDF §48/§134): serialized AgentState so a WAITING_USER run can
    # resume without a LangGraph checkpointer. Column ships with migration 0005.
    state_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    tool_call_count: Mapped[int] = mapped_column(default=0)
    created_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)


class AgentToolCall(Base):
    """One admitted (or rejected) tool call, for audit (PDF §122-§124).

    Written by ToolPolicy - the single admission gate - never by handlers.
    Large payloads are truncated at write time (§124: summary over blob);
    the run row keeps the authoritative tool_call_count.
    """

    __tablename__ = "agent_tool_calls"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    tool_call_id: Mapped[str] = mapped_column(String(32))
    tool_name: Mapped[str] = mapped_column(String(64))
    arguments: Mapped[str] = mapped_column(Text, default="{}")
    # PENDING reserved for a future async execution model; the inline policy
    # chain writes RUNNING at admission and a terminal state on return.
    status: Mapped[str] = mapped_column(String(16), default="RUNNING")
    result: Mapped[str | None] = mapped_column(Text, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    started_at: Mapped[datetime] = mapped_column(default=utcnow)
    finished_at: Mapped[datetime | None] = mapped_column(nullable=True)
