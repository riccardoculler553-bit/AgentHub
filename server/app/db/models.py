"""Persistent state models (MySQL is the authoritative state store).

Realtime WebSocket state lives in the in-memory ConnectionHub, never here.
"""

from datetime import UTC, datetime

from sqlalchemy import CHAR, BigInteger, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base

# SQLite (test backend) only autoincrements plain INTEGER primary keys
BigIntPK = BigInteger().with_variant(Integer(), "sqlite")


def utcnow() -> datetime:
    """Naive UTC timestamp (stored in MySQL DATETIME)."""
    return datetime.now(UTC).replace(tzinfo=None)


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(100), unique=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class Device(Base):
    __tablename__ = "devices"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(CHAR(36), unique=True)
    user_id: Mapped[int] = mapped_column(BigInteger, ForeignKey("users.id"), nullable=False)
    name: Mapped[str] = mapped_column(String(100))
    hostname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    platform: Mapped[str | None] = mapped_column(String(50), nullable=True)
    client_version: Mapped[str | None] = mapped_column(String(50), nullable=True)
    status: Mapped[str] = mapped_column(String(20), default="offline")
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class DeviceRegistrationCode(Base):
    __tablename__ = "device_registration_codes"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    registration_id: Mapped[str] = mapped_column(CHAR(36), unique=True)
    code_hash: Mapped[str] = mapped_column(String(128), unique=True)
    user_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class DeviceToken(Base):
    __tablename__ = "device_tokens"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(CHAR(36), nullable=False)
    token_hash: Mapped[str] = mapped_column(String(128), unique=True)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_used_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class WebsocketConnection(Base):
    __tablename__ = "websocket_connections"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    connection_id: Mapped[str] = mapped_column(CHAR(36), unique=True)
    device_id: Mapped[str] = mapped_column(CHAR(36), nullable=False)
    connected_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    disconnected_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    remote_ip: Mapped[str | None] = mapped_column(String(100), nullable=True)
    close_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
