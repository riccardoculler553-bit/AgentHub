"""DingTalk robot callback payload -> IncomingMessage (PDF §16-§17).

Stream Mode robot callbacks only deliver group messages that @-mention the
bot, but the atUsers list still lets us verify when the robot code is known.
Non-mentioned / empty messages return None and are dropped.
"""

import re

from app.core.config import settings
from app.integrations.dingtalk.models import IncomingMessage

_LEADING_AT = re.compile(r"^@\S+\s*")


def parse_incoming(payload: dict) -> IncomingMessage | None:
    text = str((payload.get("text") or {}).get("content", "")).strip()
    text = _LEADING_AT.sub("", text).strip()
    if not text:
        return None

    robot_code = settings.dingtalk_robot_code or ""
    at_users = payload.get("atUsers") or []
    if at_users and robot_code:
        mentioned = any(
            str(u.get("dingtalkId")) == robot_code or str(u.get("staffId")) == robot_code
            for u in at_users
        )
    else:
        # Group robot callbacks only arrive for @-mentions; single chats are
        # always direct. Without atUsers data there is nothing to verify.
        mentioned = True
    if not mentioned:
        return None

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
