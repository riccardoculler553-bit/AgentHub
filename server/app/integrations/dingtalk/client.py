"""DingTalk Stream Mode client (PDF §5/§88).

Runs the official dingtalk-stream long connection; robot callbacks are parsed
into IncomingMessage and handed to the MvpAgentService. Started from the app
lifespan when DINGTALK_CLIENT_ID/SECRET are configured; the dependency is
optional so the server still boots without it.
"""

import asyncio
import logging

from app.core.config import settings
from app.integrations.dingtalk.models import IncomingMessage

logger = logging.getLogger(__name__)


class AgentHubBotHandler:
    """Adapter glue: dingtalk-stream callback -> IncomingMessage -> MVP agent.

    Kept as a mixin so the module imports even when dingtalk_stream is not
    installed (tests / bare deployments)."""

    async def handle_payload(self, data: dict, on_message) -> None:
        incoming = parse_and_filter(data)
        if incoming is None:
            return
        await on_message(incoming)


def parse_and_filter(data: dict) -> IncomingMessage | None:
    from app.integrations.dingtalk.parser import parse_incoming

    return parse_incoming(data)


def build_bot_handler(service):
    """Create the dingtalk_stream chatbot handler bound to `service`."""
    import dingtalk_stream

    class Handler(dingtalk_stream.ChatbotHandler):
        async def process(self, callback: dingtalk_stream.CallbackMessage):
            incoming = parse_and_filter(callback.data)
            if incoming is None:
                return dingtalk_stream.AckMessage.STATUS_OK, "ignored"
            try:
                service.handle_message(
                    text=incoming.text,
                    channel=incoming.channel,
                    message_id=incoming.message_id,
                    conversation_id=incoming.conversation_id,
                    sender_id=incoming.sender_id,
                    sender_name=incoming.sender_name,
                    reply_webhook=incoming.reply_webhook,
                )
            except Exception:
                logger.exception("failed to enqueue dingtalk message %s", incoming.message_id)
            return dingtalk_stream.AckMessage.STATUS_OK, "OK"

    return Handler()


async def start_dingtalk_bot(hub, service) -> "asyncio.Task | None":
    """Start the Stream client as a background task. Returns None when the
    integration is not configured or the SDK is unavailable."""
    if not (settings.dingtalk_client_id and settings.dingtalk_client_secret):
        logger.info("DingTalk integration disabled (no DINGTALK_CLIENT_ID/SECRET)")
        return None
    try:
        import dingtalk_stream
    except ImportError:
        logger.warning("dingtalk-stream not installed; DingTalk integration disabled")
        return None

    credential = dingtalk_stream.DingTalkStreamCredential(
        settings.dingtalk_client_id, settings.dingtalk_client_secret
    )
    client = dingtalk_stream.DingTalkStreamClient(credential)
    client.register_callback_handler(
        dingtalk_stream.chatbot.ChatbotMessage.TOPIC, build_bot_handler(service)
    )

    async def _run():
        while True:
            try:
                logger.info("DingTalk Stream client starting (robot=%s)", settings.dingtalk_robot_code)
                await client.start()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("DingTalk Stream client crashed; restarting in 10s")
                await asyncio.sleep(10)

    return asyncio.create_task(_run())
