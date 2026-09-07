"""Unit tests: TokenService."""

import pytest

from app.auth.token import TokenService
from app.core.exceptions import TokenInvalid, TokenRevoked
from app.db.database import SessionLocal
from app.device.repository import DeviceRepository


def _make_device():
    with SessionLocal() as db:
        device = DeviceRepository(db).create(1, "unit-device", None, None, None)
        db.commit()
        return device.device_id


def test_issue_and_verify():
    device_id = _make_device()
    with SessionLocal() as db:
        tokens = TokenService(db)
        token = tokens.issue_for_device(device_id)
        db.commit()
        assert token.startswith("dl_dev_")

        row = tokens.verify(token)
        assert row.device_id == device_id


def test_verify_unknown_token_raises():
    with SessionLocal() as db:
        with pytest.raises(TokenInvalid):
            TokenService(db).verify("dl_dev_not_a_real_token")


def test_revoked_token_rejected():
    device_id = _make_device()
    with SessionLocal() as db:
        tokens = TokenService(db)
        token = tokens.issue_for_device(device_id)
        db.commit()
        assert tokens.revoke_for_device(device_id) == 1
        db.commit()
        with pytest.raises(TokenRevoked):
            tokens.verify(token)


def test_plaintext_never_stored():
    device_id = _make_device()
    with SessionLocal() as db:
        tokens = TokenService(db)
        token = tokens.issue_for_device(device_id)
        db.commit()
        from app.db.models import DeviceToken

        rows = db.query(DeviceToken).filter_by(device_id=device_id).all()
        assert len(rows) == 1
        assert rows[0].token_hash != token
        assert len(rows[0].token_hash) == 64  # sha256 hex
