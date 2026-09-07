"""DingTalkSender: reply into the originating conversation (PDF §53).

Replies go through the sessionWebhook carried by the incoming event - no
extra app credentials needed, and each reply is bound to the right group by
construction. Non-dingtalk channels and missing webhooks are no-ops (replies
stay recorded on the AgentRun).
"""

import logging

import httpx

logger = logging.getLogger(__name__)


class DingTalkSender:
    async def send_reply(
        self, *, channel: str, conversation_id: str, text: str, webhook: str | None
    ) -> None:
        if channel != "dingtalk":
            return  # api/test runs: reply persists on the AgentRun only
        if not webhook:
            logger.warning("dingtalk reply for %s dropped: no sessionWebhook", conversation_id)
            return
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(
                webhook,
                json={"msgtype": "text", "text": {"content": text}},
            )
            resp.raise_for_status()
            body = resp.json()
            if body.get("errcode") not in (0, None):
                raise RuntimeError(f"dingtalk webhook error: {body}")
