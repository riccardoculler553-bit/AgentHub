"""DingTalk Stream Mode client (PDF §5/§88).

Raw long-connection protocol (ported from the proven dingtalk-xbot-audit
implementation, replacing the official dingtalk-stream SDK whose gateway
endpoint stopped delivering group @ callbacks for this app):

1. POST /v1.0/gateway/connections/open -> {endpoint, ticket}
2. WebSocket connect to endpoint?ticket=<ticket>
3. Frames: SYSTEM(ping/disconnect), EVENT, CALLBACK
4. ACK every frame with {code, headers{messageId, contentType}}
5. CALLBACK frames with the chatbot topic are parsed into IncomingMessage
   and handed to the MvpAgentService.

Started from the app lifespan when DINGTALK_CLIENT_ID/SECRET are configured;
the dependency is optional so the server still boots without it.
"""

import asyncio
import json
import logging
import socket
import time
import urllib.request
from urllib.parse import quote_plus

import websockets

from app.core.config import settings
from app.integrations.dingtalk.models import IncomingMessage

logger = logging.getLogger(__name__)

BOT_MESSAGE_TOPIC = "/v1.0/im/bot/messages/get"
OPENAPI_ENDPOINT = "https://api.dingtalk.com"


def _local_ip() -> str:
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.connect(("8.8.8.8", 80))
        return sock.getsockname()[0]
    except Exception:
        return ""
    finally:
        sock.close()


def _register_connection(client_id: str, client_secret: str) -> tuple[str, str]:
    """Step 1: ask DingTalk for a WebSocket endpoint and one-time ticket."""
    payload = {
        "clientId": client_id,
        "clientSecret": client_secret,
        "subscriptions": [{"type": "CALLBACK", "topic": BOT_MESSAGE_TOPIC}],
        "ua": "agenthub-stream/1.0",
        "localIp": _local_ip(),
    }
    req = urllib.request.Request(
        f"{OPENAPI_ENDPOINT}/v1.0/gateway/connections/open",
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read().decode("utf-8"))
    endpoint = (data or {}).get("endpoint") or ""
    ticket = (data or {}).get("ticket") or ""
    if not endpoint or not ticket:
        raise RuntimeError(f"open connection response missing endpoint/ticket: {data}")
    return endpoint, ticket


def _parse_frame(frame: dict) -> IncomingMessage | None:
    from app.integrations.dingtalk.parser import parse_incoming

    if frame.get("type") != "CALLBACK":
        return None
    topic = (frame.get("headers") or {}).get("topic")
    if topic != BOT_MESSAGE_TOPIC:
        return None
    data = frame.get("data")
    if isinstance(data, str):
        try:
            data = json.loads(data)
        except json.JSONDecodeError:
            logger.warning("dingtalk callback data is not json: %.160s", data)
            return None
    if not isinstance(data, dict):
        return None
    return parse_incoming(data)


class GroupThrottle:
    """At most one trigger per conversation within a rolling window
    (参考 dingtalk-xbot-audit MessageHandler.GroupThrottle)."""

    def __init__(self, seconds: int) -> None:
        self.seconds = max(0, seconds)
        self._last: dict[str, float] = {}

    def allow(self, conversation_id: str) -> bool:
        if self.seconds <= 0:
            return True
        now = time.monotonic()
        last = self._last.get(conversation_id)
        if last is not None and now - last < self.seconds:
            return False
        self._last[conversation_id] = now
        return True


async def _send_throttled_notice(service, incoming: IncomingMessage) -> None:
    if service.sender is None:
        return
    try:
        await service.sender.send_reply(
            channel=incoming.channel,
            conversation_id=incoming.conversation_id,
            text="操作太频繁，请稍后再试。",
            webhook=incoming.reply_webhook,
        )
    except Exception:
        logger.exception("failed to send throttled notice")


async def _handle_frame(ws, frame: dict, service, throttle: GroupThrottle) -> None:
    headers = frame.get("headers") or {}
    logger.info(
        "dingtalk frame: type=%s topic=%s messageId=%s",
        frame.get("type"),
        headers.get("topic"),
        headers.get("messageId"),
    )
    # ACK every frame first (protocol requirement, mirrors the reference impl).
    ack = {
        "code": 200,
        "message": "OK",
        "headers": {
            "contentType": "application/json",
            "messageId": headers.get("messageId"),
        },
    }
    await ws.send(json.dumps(ack))

    incoming = _parse_frame(frame)
    if incoming is None:
        return
    logger.info(
        "dingtalk message: conv=%s sender=%s(%s) msg_id=%s text=%r",
        incoming.conversation_id,
        incoming.sender_name,
        incoming.sender_id,
        incoming.message_id,
        incoming.text,
    )
    if not throttle.allow(incoming.conversation_id):
        logger.info("dingtalk message throttled: conv=%s", incoming.conversation_id)
        await _send_throttled_notice(service, incoming)
        return
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


async def _stream_loop(service) -> None:
    credential_error_streak = 0
    while True:
        try:
            endpoint, ticket = await asyncio.to_thread(
                _register_connection,
                settings.dingtalk_client_id,
                settings.dingtalk_client_secret,
            )
            logger.info("DingTalk Stream client starting (robot=%s)", settings.dingtalk_robot_code)
            logger.info("dingtalk open connection, endpoint=%s", endpoint)
            throttle = GroupThrottle(settings.dingtalk_throttle_seconds)
            async with websockets.connect(
                f"{endpoint}?ticket={quote_plus(ticket)}", max_size=4 * 1024 * 1024
            ) as ws:
                logger.info("DingTalk connected via %s", endpoint)
                credential_error_streak = 0
                async for raw in ws:
                    try:
                        frame = json.loads(raw)
                    except json.JSONDecodeError:
                        logger.warning("invalid frame from DingTalk gateway")
                        continue
                    await _handle_frame(ws, frame, service, throttle)
        except asyncio.CancelledError:
            raise
        except Exception:
            credential_error_streak += 1
            delay = 10 if credential_error_streak < 3 else 60
            logger.exception("DingTalk Stream client crashed; restarting in %ss", delay)
            await asyncio.sleep(delay)


async def start_dingtalk_bot(hub, service) -> "asyncio.Task | None":
    """Start the Stream client as a background task. Returns None when the
    integration is not configured."""
    if not (settings.dingtalk_client_id and settings.dingtalk_client_secret):
        logger.info("DingTalk integration disabled (no DINGTALK_CLIENT_ID/SECRET)")
        return None
    return asyncio.create_task(_stream_loop(service))
