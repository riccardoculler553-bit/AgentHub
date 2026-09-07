"""MvpAgentService: DingTalk message in -> AgentRun -> background graph run.

Async model (PDF §62-§65): the HTTP/WS caller gets an AgentRun immediately;
ACK and final replies are sent while the graph runs in a background task.
Replies are recorded on the AgentRun regardless of sender availability, so
tests and the dashboard can inspect them without DingTalk.
"""

import asyncio
import logging
from typing import Protocol

from app.agent.mvp.graph import build_mvp_graph
from app.agent.runs import AgentRunService

logger = logging.getLogger(__name__)


class ReplySender(Protocol):
    async def send_reply(
        self, *, channel: str, conversation_id: str, text: str, webhook: str | None
    ) -> None: ...


class MvpAgentService:
    def __init__(self, hub, sender: ReplySender | None = None) -> None:
        self.graph = build_mvp_graph(hub)
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
        """Persist the AgentRun and kick off background processing. Returns
        the run_id immediately (never blocks on the graph)."""
        from app.db.database import SessionLocal

        with SessionLocal() as db:
            run = AgentRunService(db).create(
                channel=channel,
                message_id=message_id,
                conversation_id=conversation_id,
                sender_id=sender_id,
                sender_name=sender_name,
                input_text=text,
                reply_webhook=reply_webhook,
            )
            run_id = run.run_id
        asyncio.create_task(self._process(run_id))
        return run_id

    # ---------------------------------------------------------------- pipeline

    async def _process(self, run_id: str) -> None:
        state: dict = {}
        try:
            from app.db.database import SessionLocal

            with SessionLocal() as db:
                state = {"text": AgentRunService(db).get(run_id).input_text}

            async for event in self.graph.astream(state, config={"recursion_limit": 30}):
                for node_name, update in event.items():
                    if not isinstance(update, dict):
                        continue
                    state.update(update)
                    if node_name == "resolve" and state.get("ack"):
                        await self._send(run_id, state["ack"], kind="ack")
                    if node_name == "build_reply":
                        reply = state.get("reply", "")
                        await self._send(run_id, reply, kind="final")
                        status = "SUCCESS" if state.get("task_status") == "SUCCESS" else "FAILED"
                        self._finish(run_id, status=status, final_reply=reply, state=state)
                        return
            # Graph ended without build_reply (defensive): close the run out.
            self._finish(run_id, status="FAILED", final_reply="处理中断，请稍后重试。", state=state)
        except Exception:
            logger.exception("MVP agent run %s crashed", run_id)
            reply = "处理请求时出现系统错误，请稍后重试。"
            await self._send(run_id, reply, kind="final")
            self._finish(run_id, status="FAILED", final_reply=reply, state=state, error="internal_error")

    # ------------------------------------------------------------------ output

    async def _send(self, run_id: str, text: str, *, kind: str) -> None:
        """Record the reply on the AgentRun, then push it through the sender
        (DingTalk sessionWebhook). DB record first: replies survive sender
        outages and are visible in tests/dashboard."""
        if not text:
            return
        from app.db.database import SessionLocal

        channel = conversation_id = ""
        webhook: str | None = None
        with SessionLocal() as db:
            service = AgentRunService(db)
            run = service.get(run_id)
            if kind == "ack":
                if not run.ack_reply:
                    run.ack_reply = text
                    db.commit()
            else:
                run.final_reply = text
                db.commit()
            channel, conversation_id, webhook = run.channel, run.conversation_id, run.reply_webhook
        if self.sender is None:
            return
        try:
            await self.sender.send_reply(
                channel=channel, conversation_id=conversation_id, text=text, webhook=webhook
            )
        except Exception:
            logger.exception("reply send failed for run %s", run_id)

    def _finish(self, run_id: str, *, status: str, final_reply: str, state: dict, error: str | None = None) -> None:
        from app.db.database import SessionLocal

        with SessionLocal() as db:
            AgentRunService(db).finish(
                run_id,
                status=status,
                final_reply=final_reply,
                task_id=state.get("task_id"),
                error=error,
            )
