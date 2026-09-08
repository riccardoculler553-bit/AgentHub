"""AgentRunService: lifecycle of one user request (DingTalk message).

RUNNING -> SUCCESS (task succeeded / final reply delivered)
        -> FAILED  (no task created, task failed, or agent gave up waiting)
        -> WAITING_USER (ask_user, PDF §46: a normal state, not an error)

A WAITING_USER run parks its serialized AgentState in state_json; when the
user answers, resume flows reload it and continue the same run (PDF §48/§133).
"""

import json
import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agent.db_models import AgentRun
from app.db.models import utcnow

STATUS_RUNNING = "RUNNING"
STATUS_WAITING_USER = "WAITING_USER"
STATUS_SUCCESS = "SUCCESS"
STATUS_FAILED = "FAILED"
STATUS_CANCELLED = "CANCELLED"  # V1.2 §90: the AgentRun is stopped; business tasks are NOT auto-cancelled
# terminal statuses that a late update must never overwrite
_TERMINAL = (STATUS_SUCCESS, STATUS_FAILED, STATUS_CANCELLED)


def _new_run_id() -> str:
    return f"run_{uuid.uuid4().hex[:12]}"


def _dump_state(state: dict) -> str:
    return json.dumps(state, ensure_ascii=False, default=str)


class AgentRunService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create(
        self,
        *,
        channel: str,
        message_id: str,
        conversation_id: str,
        sender_id: str,
        sender_name: str | None,
        input_text: str,
        reply_webhook: str | None = None,
    ) -> AgentRun:
        run = AgentRun(
            run_id=_new_run_id(),
            channel=channel,
            message_id=message_id or None,  # NULL: exempt from the unique key
            conversation_id=conversation_id,
            sender_id=sender_id,
            sender_name=sender_name,
            input_text=input_text,
            status="RUNNING",
            reply_webhook=reply_webhook,
        )
        self.db.add(run)
        self.db.commit()
        return run

    def get(self, run_id: str) -> AgentRun:
        row = self.db.scalars(select(AgentRun).where(AgentRun.run_id == run_id)).first()
        if row is None:
            raise KeyError(run_id)
        return row

    def get_by_message_id(self, message_id: str) -> AgentRun | None:
        """Idempotency lookup (PDF 参考 dingtalk-xbot-audit MessageDeduplicator):
        the same DingTalk msgId must create exactly one run."""
        if not message_id:
            return None
        return self.db.scalars(
            select(AgentRun).where(AgentRun.message_id == message_id).order_by(AgentRun.id.desc())
        ).first()

    def list_runs(self, limit: int = 50) -> list[AgentRun]:
        return list(
            self.db.scalars(select(AgentRun).order_by(AgentRun.id.desc()).limit(max(1, min(limit, 200))))
        )

    def set_task(self, run_id: str, task_id: str, ack_reply: str) -> None:
        run = self.get(run_id)
        run.task_id = task_id
        run.ack_reply = ack_reply
        self.db.commit()

    def finish(
        self,
        run_id: str,
        *,
        status: str,
        final_reply: str,
        task_id: str | None = None,
        error: str | None = None,
        tool_call_count: int | None = None,
    ) -> None:
        run = self.get(run_id)
        if run.status in _TERMINAL:
            return  # idempotent: late updates never overwrite a finished run
        run.status = status
        run.final_reply = final_reply
        run.error = error
        run.finished_at = utcnow()
        if task_id:
            run.task_id = task_id
        if tool_call_count is not None:
            run.tool_call_count = int(tool_call_count)  # PDF §125: kept for audit
        self.db.commit()

    def cancel(self, run_id: str, *, final_reply: str = "已取消。") -> AgentRun:
        """Stop an open run (V1.2 §90). Idempotent: an already-terminal run is
        returned unchanged. Only the run closes - business tasks keep running;
        the user can cancel those explicitly via cancel_task."""
        run = self.get(run_id)
        if run.status in _TERMINAL:
            return run
        run.status = STATUS_CANCELLED
        run.final_reply = final_reply
        run.finished_at = utcnow()
        self.db.commit()
        return run

    # ------------------------------------------------- WAITING_USER (PDF §46)

    def save_state(self, run_id: str, state: dict) -> None:
        """Persist the reasoning state of a still-open run (crash safety)."""
        run = self.get(run_id)
        if run.status in _TERMINAL:
            return
        run.state_json = _dump_state(state)
        run.tool_call_count = int(state.get("tool_call_count") or 0)
        self.db.commit()

    def mark_waiting_user(self, run_id: str, state: dict, question: str) -> None:
        """Park the run: the question becomes final_reply (the normal reply
        path delivers it); the state is stored for resume."""
        run = self.get(run_id)
        if run.status in _TERMINAL:
            return  # idempotent: a finished run can never wait for a user
        run.status = STATUS_WAITING_USER
        run.final_reply = question
        run.state_json = _dump_state(state)
        run.tool_call_count = int(state.get("tool_call_count") or 0)
        self.db.commit()

    def load_state(self, run_id: str) -> dict | None:
        run = self.get(run_id)
        if not run.state_json:
            return None
        try:
            return json.loads(run.state_json)
        except (TypeError, ValueError):
            return None

    def reopen(self, run_id: str) -> AgentRun:
        """WAITING_USER -> RUNNING when a resume takes the run back (§133).
        Idempotent-safe: only a WAITING_USER run reopens; anything else is
        returned unchanged so a terminal run can never start again."""
        run = self.get(run_id)
        if run.status != STATUS_WAITING_USER:
            return run
        run.status = STATUS_RUNNING
        run.final_reply = None
        self.db.commit()
        return run

    def find_resumable(self, conversation_id: str, sender_id: str = "") -> AgentRun | None:
        """Latest WAITING_USER run for this conversation (PDF §48)."""
        stmt = (
            select(AgentRun)
            .where(AgentRun.status == STATUS_WAITING_USER, AgentRun.conversation_id == conversation_id)
            .order_by(AgentRun.id.desc())
        )
        if sender_id:
            stmt = stmt.where(AgentRun.sender_id == sender_id)
        return self.db.scalars(stmt.limit(1)).first()
