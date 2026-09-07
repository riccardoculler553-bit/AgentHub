"""Internal message model crossing the adapter boundary (PDF §15)."""

from pydantic import BaseModel


class IncomingMessage(BaseModel):
    channel: str = "dingtalk"
    message_id: str = ""
    conversation_id: str = ""
    sender_id: str = ""
    sender_name: str | None = None
    text: str = ""
    mentioned_bot: bool = True
    reply_webhook: str | None = None
