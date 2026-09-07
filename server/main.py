"""Convenience entrypoint: python server/main.py (run from project root)."""

import sys
from pathlib import Path

# Make `app` importable when launched from the project root
sys.path.insert(0, str(Path(__file__).resolve().parent))

import uvicorn  # noqa: E402

from app.core.config import settings  # noqa: E402

if __name__ == "__main__":
    uvicorn.run("app.main:app", host=settings.host, port=settings.port, log_level="info")
