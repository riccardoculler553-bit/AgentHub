"""AgentRunService: lifecycle of one user request (DingTalk message).

RUNNING -> SUCCESS (task succeeded / final reply delivered)
        -> FAILED  (no task created, task failed, or agent gave up waiting)
"""

import uuid

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.agent.db_models import AgentRun
from app.db.models import utcnow


def _new_run_id() -> str:
    return f"run_{uuid.uuid4().hex[:12]}"


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
            message_id=message_id,
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

    def finish(self, run_id: str, *, status: str, final_reply: str, task_id: str | None = None, error: str | None = None) -> None:
        run = self.get(run_id)
        if run.status != "RUNNING":
            return  # idempotent: late updates never overwrite a finished run
        run.status = status
        run.final_reply = final_reply
        run.error = error
        run.finished_at = utcnow()
        if task_id:
            run.task_id = task_id
        self.db.commit()
