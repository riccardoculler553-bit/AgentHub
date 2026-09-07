"""Database engine, session factory and declarative base."""

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker
from sqlalchemy.pool import StaticPool

from app.core.config import settings

_engine_kwargs: dict = {}
_connect_args: dict = {}
if settings.database_url.startswith("sqlite"):
    # StaticPool (one shared connection) is ONLY safe for pure in-memory SQLite
    # used single-threaded. Multi-threaded sessions on one shared DBAPI
    # connection corrupt transaction state (rollback discards other sessions'
    # uncommitted work). File-based SQLite uses the default pool: one
    # connection per thread, serialized by SQLite file locks.
    if settings.database_url.endswith(":memory:") or settings.database_url == "sqlite://":
        _engine_kwargs["poolclass"] = StaticPool
        _connect_args["check_same_thread"] = False
    else:
        # Wait up to 15s for a locked database instead of failing immediately.
        _connect_args["timeout"] = 15
else:
    _engine_kwargs["pool_pre_ping"] = True
    _engine_kwargs["pool_recycle"] = 3600

engine = create_engine(settings.database_url, connect_args=_connect_args, **_engine_kwargs)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, expire_on_commit=False)


class Base(DeclarativeBase):
    pass


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
