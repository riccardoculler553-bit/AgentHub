"""Capability Runtime persistent models (V1.4 §60).

capabilities          = Capability Definition ("做什么")
capability_versions   = immutable version records ("1.2.0 是什么")
capability_packages   = downloadable ZIP artifacts ("怎么做") on server storage
worker_capabilities   = which Worker reports which installed version

AutomationCapability is business-facing; DeviceCapability (app.capability)
stays environment-facing. Never merge the two (V1.4 §23).
"""

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, ForeignKey, Integer, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import BigIntPK, utcnow


class AutomationCapability(Base):
    __tablename__ = "capabilities"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    # <domain>.<resource>.<action>, e.g. amazon.order.download
    name: Mapped[str] = mapped_column(String(128), unique=True)
    display_name: Mapped[str] = mapped_column(String(200), default="")
    description: Mapped[str] = mapped_column(String(1000), default="")
    # ATOMIC | BUSINESS
    type: Mapped[str] = mapped_column(String(16), default="ATOMIC")
    # YINGDAO | PYTHON | HTTP | LOCAL
    runtime_type: Mapped[str] = mapped_column(String(16))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    # latest PUBLISHED version (None until first publish)
    current_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    input_schema: Mapped[dict] = mapped_column(JSON, default=dict)
    output_schema: Mapped[dict] = mapped_column(JSON, default=dict)
    # READ | WRITE | ACTION (V1.4 §36 tool policy)
    risk_level: Mapped[str] = mapped_column(String(16), default="READ")
    requires_confirmation: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class CapabilityVersion(Base):
    __tablename__ = "capability_versions"
    __table_args__ = (UniqueConstraint("capability_name", "version", name="uq_capability_version"),)

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    # stores the capability NAME (readable FK, matches task/workflow conventions)
    capability_name: Mapped[str] = mapped_column(String(128), ForeignKey("capabilities.name"), index=True)
    version: Mapped[str] = mapped_column(String(32))
    package_id: Mapped[str] = mapped_column(String(64), ForeignKey("capability_packages.package_id"))
    # DRAFT | PUBLISHED | DEPRECATED | DISABLED (immutable version, §9)
    status: Mapped[str] = mapped_column(String(16), default="DRAFT", index=True)
    input_schema: Mapped[dict] = mapped_column(JSON, default=dict)
    output_schema: Mapped[dict] = mapped_column(JSON, default=dict)
    entrypoint: Mapped[str] = mapped_column(String(200), default="main")
    checksum: Mapped[str] = mapped_column(String(128), default="")
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class CapabilityPackage(Base):
    __tablename__ = "capability_packages"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    package_id: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(128), index=True)
    version: Mapped[str] = mapped_column(String(32))
    # server-relative storage path (storage/capability_packages/<package_id>.zip)
    storage_path: Mapped[str] = mapped_column(String(500))
    size: Mapped[int] = mapped_column(Integer, default=0)
    checksum: Mapped[str] = mapped_column(String(128), default="")
    runtime_type: Mapped[str] = mapped_column(String(16))
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class WorkerCapability(Base):
    __tablename__ = "worker_capabilities"
    __table_args__ = (
        UniqueConstraint("worker_id", "capability_name", "version", name="uq_worker_capability"),
    )

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    worker_id: Mapped[str] = mapped_column(String(36), index=True)
    capability_name: Mapped[str] = mapped_column(String(128))
    version: Mapped[str] = mapped_column(String(32))
    # INSTALLED | READY | DOWNLOADING | DOWNLOAD_FAILED | INSTALL_FAILED |
    # INVALID_PACKAGE | CHECKSUM_FAILED (V1.4 §53)
    status: Mapped[str] = mapped_column(String(16), default="READY")
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
