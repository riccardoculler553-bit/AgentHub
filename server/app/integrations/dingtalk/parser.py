"""DingTalk robot callback payload -> IncomingMessage (PDF §16-§17).

Stream Mode robot callbacks only deliver group messages that @-mention the
bot. Non-mentioned / empty messages return None and are dropped.
"""

import re

from app.integrations.dingtalk.models import IncomingMessage

_LEADING_AT = re.compile(r"^@\S+\s*")


def parse_incoming(payload: dict) -> IncomingMessage | None:
    text = str((payload.get("text") or {}).get("content", "")).strip()
    text = _LEADING_AT.sub("", text).strip()
    if not text:
        return None

    # The platform only delivers group callbacks that @-mention the bot, so
    # no extra atUsers verification is needed (matching the reference
    # implementation). atUsers ids do not always equal the robot code, and a
    # strict check silently drops valid group mentions.

    return IncomingMessage(
        channel="dingtalk",
        message_id=str(payload.get("msgId", "")),
        conversation_id=str(payload.get("conversationId", "")),
        sender_id=str(payload.get("senderStaffId") or payload.get("senderId", "")),
        sender_name=str(payload.get("senderNick")) or None,
        text=text,
        mentioned_bot=True,
        reply_webhook=str(payload.get("sessionWebhook")) or None,
    )
