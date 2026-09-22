"""Worker plane persistent models (V1.7 §44-§47).

worker_environments - latest environment snapshot per device; the fingerprint
                      column detects environment drift between reports.
worker_processes    - persistent ProcessInstance registry (execution.mode=
                      service). One row per (device, capability, version)
                      instance the operator/Agent started.
"""

from datetime import datetime

from sqlalchemy import JSON, DateTime, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import BigIntPK, utcnow


class WorkerEnvironment(Base):
    __tablename__ = "worker_environments"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(String(36), unique=True, index=True)
    # full snapshot: os/cpu/memory/disk/python/yingdao/worker (§27 shape)
    snapshot: Mapped[dict] = mapped_column(JSON, default=dict)
    fingerprint: Mapped[str | None] = mapped_column(String(64), nullable=True)
    worker_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    hostname: Mapped[str | None] = mapped_column(String(255), nullable=True)
    collected_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class WorkerProcess(Base):
    __tablename__ = "worker_processes"
    __table_args__ = (
        UniqueConstraint("device_id", "capability", "version", name="uq_worker_process_target"),
    )

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    process_id: Mapped[str] = mapped_column(String(64), unique=True)
    device_id: Mapped[str] = mapped_column(String(36), index=True)
    capability: Mapped[str] = mapped_column(String(128))
    version: Mapped[str] = mapped_column(String(32))
    package_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # STARTING | RUNNING | STOPPING | STOPPED | FAILED
    status: Mapped[str] = mapped_column(String(16), default="STARTING", index=True)
    pid: Mapped[int | None] = mapped_column(nullable=True)
    restart_count: Mapped[int] = mapped_column(default=0)
    # who asked for it (admin / operator name / agent run)
    requested_by: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_error: Mapped[str | None] = mapped_column(String(500), nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_health_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
