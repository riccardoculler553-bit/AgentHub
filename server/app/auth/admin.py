"""Admin authentication: X-Admin-Token guard for dashboard & management APIs.

Admin token is completely separate from device tokens:
- Device token: Bearer auth on /api/ws/device (device identity)
- Admin token:  X-Admin-Token header on management APIs (human/agent operator)

V1.6 P0 (0.7, audit H1): fail-closed. An empty AGENTHUB_ADMIN_TOKEN no longer
opens the management plane for everyone - only when the server is explicitly
bound to a loopback address (local dev). On any non-loopback bind (0.0.0.0,
tailscale, LAN) the management APIs answer 401 until a token is configured.

V1.6 P0 0.18: single-tenant RBAC with three roles (no tenant_id):
- admin    AGENTHUB_ADMIN_TOKEN    everything (revoke, publish, RBAC)
- operator AGENTHUB_OPERATOR_TOKEN dispatch/cancel/retry/messages + read
- viewer   AGENTHUB_VIEWER_TOKEN   read-only fleet metadata
require_admin / require_operator / require_viewer are hierarchical.
"""

import hmac

from fastapi import Header, HTTPException, status

from app.core.config import settings

_LOOPBACK_HOSTS = ("127.0.0.1", "::1", "localhost")

ROLE_TOKENS = {
    "admin": "admin_token",
    "operator": "operator_token",
    "viewer": "viewer_token",
}


def admin_fail_open_allowed() -> bool:
    """True only when the server is bound to loopback (local dev mode)."""
    return settings.host in _LOOPBACK_HOSTS


def _match(x_admin_token: str | None) -> str | None:
    """Return the highest role the presented token satisfies, else None."""
    if not x_admin_token:
        return None
    for role in ("admin", "operator", "viewer"):
        expected = getattr(settings, ROLE_TOKENS[role], "") or ""
        if expected and hmac.compare_digest(x_admin_token, expected):
            return role
    return None


def _fail_open_or(role: str) -> None:
    """Loopback open-mode bypass: with NO tokens configured at all and a
    loopback bind, every role passes (local dev convenience)."""
    if admin_fail_open_allowed() and not any(
        getattr(settings, ROLE_TOKENS[r], "") for r in ROLE_TOKENS
    ):
        return
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail={
            "code": "admin_unauthorized",
            "message": f"missing or invalid X-Admin-Token for role '{role}'",
        },
    )


def require_role(role: str, x_admin_token: str | None = Header(default=None)) -> None:
    """Hierarchical check: admin satisfies every role, operator satisfies
    operator+viewer, viewer satisfies viewer only."""
    matched = _match(x_admin_token)
    hierarchy = {"admin": ["admin"], "operator": ["admin", "operator"], "viewer": ["admin", "operator", "viewer"]}
    if matched is not None:
        if matched in hierarchy[role]:
            return
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"code": "forbidden", "message": f"this endpoint requires role '{role}' or higher"},
        )
    # No (or wrong) token: identical fail-closed semantics as require_admin.
    _fail_open_or(role)


def require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    """Admin-only endpoints. Hierarchical: a valid lower-role token yields
    403 (authenticated but unauthorized); a missing/unknown token yields the
    same fail-closed 401 semantics as before V1.6 0.18."""
    require_role("admin", x_admin_token)


def require_operator(x_admin_token: str | None = Header(default=None)) -> None:
    require_role("operator", x_admin_token)


def require_viewer(x_admin_token: str | None = Header(default=None)) -> None:
    require_role("viewer", x_admin_token)
