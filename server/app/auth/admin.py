"""Admin authentication: X-Admin-Token guard for dashboard & management APIs.

Admin token is completely separate from device tokens:
- Device token: Bearer auth on /api/ws/device (device identity)
- Admin token:  X-Admin-Token header on management APIs (human/agent operator)

If AGENTHUB_ADMIN_TOKEN is empty, admin auth is disabled (local dev only).
"""

import hmac

from fastapi import Header, HTTPException, status

from app.core.config import settings


def require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    expected = settings.admin_token
    if not expected:
        return
    if x_admin_token is None or not hmac.compare_digest(x_admin_token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail={"code": "admin_unauthorized", "message": "missing or invalid X-Admin-Token"},
        )
