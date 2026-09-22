"""Server URL discovery for the bootstrap script (V1.7 doc §18).

The first version is deliberately simple: an explicit --server argument wins,
then the AGENTHUB_SERVER_URL environment variable; anything fancier (mDNS,
config lookup) is out of scope.
"""

import os


class DiscoveryError(RuntimeError):
    """Raised when no server URL can be resolved."""


def discover(server_arg: str | None) -> str:
    """Resolve the AgentHub server base URL.

    Priority: explicit --server argument > AGENTHUB_SERVER_URL env var.
    Raises DiscoveryError with an actionable message otherwise.
    """
    if server_arg and server_arg.strip():
        return server_arg.strip().rstrip("/")
    env_url = (os.getenv("AGENTHUB_SERVER_URL") or "").strip()
    if env_url:
        return env_url.rstrip("/")
    raise DiscoveryError(
        "no server URL: pass --server http://<host>:8000 "
        "(or set the AGENTHUB_SERVER_URL environment variable)"
    )
