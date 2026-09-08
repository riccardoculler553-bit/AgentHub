"""AgentService (V1.2 §91-§94): the business layer between API/DingTalk and
AgentRunner.

API  -> AgentService -> AgentRunner -> LangGraph  (never API -> graph.invoke)

Intake mirrors the MVP service: one user message -> one AgentRun row, replies
recorded on the row and pushed through the sender when configured. The graph
runs in a background task, so callers get a run_id immediately.

Conversation continuity (PDF §48): while a run for the same conversation is
WAITING_USER, the user's next message resumes THAT run instead of starting a
new one - the supplement lands in the restored AgentState.
"""

import asyncio
import logging
from typing import Protocol

from app.agent.core.runner import AgentRunner
from app.agent.llm.service import LLMService
from app.agent.runs import STATUS_WAITING_USER, AgentRunService
from app.agent.tools.registry import build_default_registry

logger = logging.getLogger(__name__)


class ReplySender(Protocol):
    async def send_reply(
        self, *, channel: str, conversation_id: str, text: str, webhook: str | None
    ) -> None: ...


class NotResumableError(Exception):
    """The run is not in WAITING_USER (or its state is unreadable)."""


class AgentService:
    def __init__(
        self,
        hub,
        sender: ReplySender | None = None,
        *,
        registry=None,
        llm=None,
    ) -> None:
        self.registry = registry or build_default_registry(hub)
        self.runner = AgentRunner(
            registry=self.registry, llm=llm or LLMService(), hub=hub
        )
        self.sender = sender

    # ------------------------------------------------------------------ intake

    def handle_message(
        self,
        *,
        text: str,
        channel: str = "dingtalk",
        message_id: str = "",
        conversation_id: str = "",
        sender_id: str = "",
        sender_name: str | None = None,
        reply_webhook: str | None = None,
    ) -> str:
        """DingTalk/API entry: create (or resume) a run and return its id now.

        - same message_id -> the existing run (Stream replays, retries)
        - a WAITING_USER run for this conversation -> the message resumes it
        - otherwise a new run is created
        """
        from app.db.database import SessionLocal

        with SessionLocal() as db:
            service = AgentRunService(db)
            if message_id:
                existing = service.get_by_message_id(message_id)
                if existing is not None:
                    return existing.run_id
            resumable = service.find_resumable(conversation_id, sender_id)
            if resumable is not None:
                run_id = resumable.run_id
                resume = True
            else:
                run = service.create(
                    channel=channel,
                    message_id=message_id,
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                    sender_name=sender_name,
                    input_text=text,
                    reply_webhook=reply_webhook,
                )
                run_id = run.run_id
                resume = False
        if resume:
            asyncio.create_task(self._resume_task(run_id, text))
        else:
            asyncio.create_task(
                self._execute_new(
                    run_id,
                    text,
                    channel=channel,
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                )
            )
        return run_id

    # ------------------------------------------------------- lifecycle (§93-94)

    def start_run(
        self, *, text: str, channel: str = "api", conversation_id: str = "", sender_id: str = ""
    ) -> str:
        """Create a run row and kick the graph off in the background."""
        from app.db.database import SessionLocal

        with SessionLocal() as db:
            run_id = (
                AgentRunService(db)
                .create(
                    channel=channel,
                    message_id="",
                    conversation_id=conversation_id,
                    sender_id=sender_id,
                    sender_name=None,
                    input_text=text,
                )
                .run_id
            )
        asyncio.create_task(
            self._execute_new(
                run_id,
                text,
                channel=channel,
                conversation_id=conversation_id,
                sender_id=sender_id,
            )
        )
        return run_id

    async def resume_run(self, run_id: str, message: str) -> dict:
        """Continue a WAITING_USER run with the user's follow-up. Raises
        NotResumableError when the run is open elsewhere or its state is gone."""
        from app.db.database import SessionLocal

        with SessionLocal() as db:
            svc = AgentRunService(db)
            row = svc.get(run_id)
            if row.status != STATUS_WAITING_USER:
                raise NotResumableError(f"run {run_id} is {row.status}, not WAITING_USER")
            state = svc.load_state(run_id)
        if state is None:
            raise NotResumableError(f"run {run_id} has no resumable state")
        # WAITING_USER -> RUNNING while the graph takes the run back (§133);
        # _finalize closes it out (or parks it again on the next ask_user).
        with SessionLocal() as db:
            AgentRunService(db).reopen(run_id)
        final = await self.runner.resume(state, message)
        await self._finalize(run_id, final)
        return final

    def schedule_resume(self, run_id: str, message: str) -> None:
        """Resume in the background (API callers never block on the graph)."""
        asyncio.create_task(self._resume_task(run_id, message))

    def cancel_run(self, run_id: str) -> str:
        """Stop an open run (V1.2 §90: the run closes; business tasks are NOT
        auto-cancelled - the user can do that explicitly via cancel_task)."""
        from app.db.database import SessionLocal

        with SessionLocal() as db:
            row = AgentRunService(db).cancel(run_id)
            return row.status

    # ------------------------------------------------------ background driving

    async def _execute_new(
        self,
        run_id: str,
        text: str,
        *,
        channel: str,
        conversation_id: str,
        sender_id: str,
    ) -> None:
        try:
            final = await self.runner.run(
                text,
                channel=channel,
                conversation_id=conversation_id,
                sender_id=sender_id,
                run_id=run_id,
            )
            await self._finalize(run_id, final)
        except Exception:
            logger.exception("agent run %s crashed", run_id)
            reply = "处理请求时出现系统错误，请稍后重试。"
            await self._send(run_id, reply)
            self._finish(run_id, status="FAILED", final_reply=reply, error="internal_error")

    async def _resume_task(self, run_id: str, message: str) -> None:
        try:
            await self.resume_run(run_id, message)
        except NotResumableError as exc:
            # the row was open when scheduled but is no longer resumable: a
            # concurrent cancel/resume owns it now - only close it out when it
            # is still parked (unreadable state must not hang in WAITING_USER).
            logger.warning("resume aborted for run %s: %s", run_id, exc)
            from app.db.database import SessionLocal

            with SessionLocal() as db:
                still_waiting = AgentRunService(db).get(run_id).status == STATUS_WAITING_USER
            if still_waiting:
                self._finish(
                    run_id,
                    status="FAILED",
                    final_reply="会话状态丢失，无法继续，请重新发起请求。",
                    error="state_missing",
                )
        except Exception:
            logger.exception("agent run %s crashed on resume", run_id)
            reply = "处理请求时出现系统错误，请稍后重试。"
            await self._send(run_id, reply)
            self._finish(run_id, status="FAILED", final_reply=reply, error="internal_error")

    async def _finalize(self, run_id: str, final: dict) -> None:
        """Map the final graph state onto the AgentRun row and reply."""
        reply = final.get("reply") or ""
        if final.get("paused"):
            # PDF §46: ask_user is a normal outcome - park for resume
            from app.db.database import SessionLocal

            with SessionLocal() as db:
                AgentRunService(db).mark_waiting_user(run_id, final, reply)
            await self._send(run_id, reply)
            return
        error = final.get("error") or {}
        status = "FAILED" if error else "SUCCESS"
        self._finish(
            run_id,
            status=status,
            final_reply=reply,
            error=error.get("error_code"),
            tool_call_count=final.get("tool_call_count"),
        )
        await self._send(run_id, reply)

    # ------------------------------------------------------------------ output

    async def _send(self, run_id: str, text: str) -> None:
        """Record first (replies survive sender outages), then push."""
        if not text:
            return
        from app.db.database import SessionLocal

        channel = conversation_id = ""
        webhook: str | None = None
        with SessionLocal() as db:
            run = AgentRunService(db).get(run_id)
            channel, conversation_id, webhook = run.channel, run.conversation_id, run.reply_webhook
        if self.sender is None:
            return
        try:
            await self.sender.send_reply(
                channel=channel, conversation_id=conversation_id, text=text, webhook=webhook
            )
        except Exception:
            logger.exception("reply send failed for run %s", run_id)

    def _finish(
        self,
        run_id: str,
        *,
        status: str,
        final_reply: str,
        error: str | None = None,
        tool_call_count: int | None = None,
    ) -> None:
        from app.db.database import SessionLocal

        with SessionLocal() as db:
            AgentRunService(db).finish(
                run_id,
                status=status,
                final_reply=final_reply,
                error=error,
                tool_call_count=tool_call_count,
            )
