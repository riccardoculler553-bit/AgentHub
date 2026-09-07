"""FastAPI dependencies for HTTP token authentication."""

from fastapi import Depends, Header
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.auth.token import TokenService
from app.core.exceptions import DeviceRevoked, TokenInvalid
from app.db.database import get_db
from app.db.models import Device


def _extract_bearer(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise TokenInvalid("missing Bearer token")
    return authorization[len("Bearer "):].strip()


def get_current_device(
    authorization: str | None = Header(default=None),
    db: Session = Depends(get_db),
    token_service: TokenService = Depends(get_token_service),
) -> Device:
    """Resolve the Device from a Bearer device token (reserved for protected HTTP APIs)."""
    token = _extract_bearer(authorization)
    token_row = token_service.verify(token)
    device = db.scalars(select(Device).where(Device.device_id == token_row.device_id)).first()
    if device is None:
        raise TokenInvalid()
    if device.revoked_at is not None:
        raise DeviceRevoked()
    return device
