"""TokenService: issue / verify / revoke device tokens. Plaintext never stored."""

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from app.core.exceptions import TokenInvalid, TokenRevoked
from app.core.security import generate_device_token, hash_token
from app.db.models import DeviceToken, utcnow


class TokenService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def issue_for_device(self, device_id: str) -> str:
        """Generate a new token, persist only its hash, return the plaintext once."""
        token = generate_device_token()
        self.db.add(DeviceToken(device_id=device_id, token_hash=hash_token(token), created_at=utcnow()))
        return token

    def verify(self, token: str) -> DeviceToken:
        row = self.db.scalars(select(DeviceToken).where(DeviceToken.token_hash == hash_token(token))).first()
        if row is None:
            raise TokenInvalid()
        if row.revoked_at is not None:
            raise TokenRevoked()
        if row.expires_at is not None and row.expires_at < utcnow():
            raise TokenRevoked()
        row.last_used_at = utcnow()
        return row

    def revoke_for_device(self, device_id: str) -> int:
        result = self.db.execute(
            update(DeviceToken)
            .where(DeviceToken.device_id == device_id, DeviceToken.revoked_at.is_(None))
            .values(revoked_at=utcnow())
        )
        return result.rowcount
