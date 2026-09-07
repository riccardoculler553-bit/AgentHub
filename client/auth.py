"""TokenManager: holds the device token; reserved for future refresh flow."""

import sys


class TokenManager:
    def __init__(self, token: str) -> None:
        self.token = token
        self.invalid = False

    def auth_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self.token}"}

    def mark_invalid(self) -> None:
        self.invalid = True

    def refresh(self) -> bool:
        """V1.0 tokens are long-lived; a refresh-token flow lands in V2.

        Returns False to signal "no refresh possible" so reconnect stops.
        """
        print("[auth] token rejected by server; automatic refresh not available in V1.0", file=sys.stderr)
        return False
