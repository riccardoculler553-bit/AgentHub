"""RegistrationService: one-time registration code lifecycle."""

import uuid
from datetime import timedelta

from sqlalchemy import update
from sqlalchemy.orm import Session

from app.core.config import settings
from app.core.exceptions import RegistrationCodeExpired, RegistrationCodeInvalid, RegistrationCodeUsed
from app.core.security import generate_registration_code, hash_token
from app.db.models import DeviceRegistrationCode, utcnow


class RegistrationService:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create_code(self, user_id: int) -> tuple[DeviceRegistrationCode, str]:
        """Create a one-time code. Returns (row, plaintext_code) - plaintext is shown once."""
        code = generate_registration_code()
        row = DeviceRegistrationCode(
            registration_id=str(uuid.uuid4()),
            code_hash=hash_token(code),
            user_id=user_id,
            expires_at=utcnow() + timedelta(seconds=settings.registration_code_ttl),
            created_at=utcnow(),
        )
        self.db.add(row)
        self.db.flush()
        return row, code

    def consume_code(self, code: str) -> DeviceRegistrationCode:
        """Atomically mark a valid (unused, unexpired) code as used, or raise a descriptive error."""
        normalized = code.strip().upper()
        code_hash = hash_token(normalized)
        claimed = self.db.execute(
            update(DeviceRegistrationCode)
            .where(
                DeviceRegistrationCode.code_hash == code_hash,
                DeviceRegistrationCode.used_at.is_(None),
                DeviceRegistrationCode.expires_at > utcnow(),
            )
            .values(used_at=utcnow())
        )
        if claimed.rowcount == 1:
            self.db.flush()
            return self.db.query(DeviceRegistrationCode).filter_by(code_hash=code_hash).one()

        # Claim failed - figure out why for a precise error message.
        row = self.db.query(DeviceRegistrationCode).filter_by(code_hash=code_hash).one_or_none()
        if row is None:
            raise RegistrationCodeInvalid()
        if row.used_at is not None:
            raise RegistrationCodeUsed()
        raise RegistrationCodeExpired()
