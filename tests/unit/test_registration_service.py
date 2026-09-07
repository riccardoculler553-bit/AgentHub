"""Unit tests: RegistrationService one-time code lifecycle."""

import pytest

from app.core.exceptions import RegistrationCodeExpired, RegistrationCodeInvalid, RegistrationCodeUsed
from app.core.security import generate_registration_code, hash_token
from app.db.database import SessionLocal
from app.db.models import DeviceRegistrationCode, utcnow
from app.registration.service import RegistrationService


def test_code_format():
    code = generate_registration_code()
    assert code.startswith("DL-")
    parts = code.split("-")
    assert len(parts) == 3 and len(parts[1]) == 4 and len(parts[2]) == 4


def test_create_and_consume():
    with SessionLocal() as db:
        service = RegistrationService(db)
        row, plaintext = service.create_code(user_id=1)
        db.commit()
        assert plaintext.startswith("DL-")
        assert row.code_hash == hash_token(plaintext)  # only the hash is stored

        consumed = service.consume_code(plaintext)
        assert consumed.registration_id == row.registration_id
        assert consumed.used_at is not None
        db.commit()


def test_consume_twice_rejected():
    with SessionLocal() as db:
        service = RegistrationService(db)
        _, plaintext = service.create_code(user_id=1)
        db.commit()
        service.consume_code(plaintext)
        db.commit()
        with pytest.raises(RegistrationCodeUsed):
            service.consume_code(plaintext)


def test_expired_code_rejected():
    with SessionLocal() as db:
        service = RegistrationService(db)
        row, plaintext = service.create_code(user_id=1)
        row.expires_at = utcnow()
        db.commit()
        with pytest.raises(RegistrationCodeExpired):
            service.consume_code(plaintext)


def test_unknown_code_rejected():
    with SessionLocal() as db:
        with pytest.raises(RegistrationCodeInvalid):
            RegistrationService(db).consume_code("DL-ZZZZ-ZZZZ")
