"""Alias: the real implementation lives in client/service/windows_service.py.

Kept so `import worker.service.windows_service` works alongside the flat
`import service.windows_service` used by the standalone script.
"""

from service.windows_service import (  # noqa: F401
    CLIENT_MAIN,
    SERVICE_DESCRIPTION,
    SERVICE_DISPLAY_NAME,
    SERVICE_NAME,
    service_main,
)

__all__ = ["CLIENT_MAIN", "SERVICE_DESCRIPTION", "SERVICE_DISPLAY_NAME", "SERVICE_NAME", "service_main"]
