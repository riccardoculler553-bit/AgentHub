"""Artifact persistent model (V1.4 §29)."""

from datetime import datetime

from sqlalchemy import BigInteger, DateTime, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import BigIntPK, utcnow


class Artifact(Base):
    __tablename__ = "artifacts"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    artifact_id: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(255), default="")
    # file | dataset | image | log | report
    type: Mapped[str] = mapped_column(String(32), default="file")
    mime_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    size: Mapped[int] = mapped_column(BigInteger, default=0)
    # server-relative path under storage/artifacts/YYYY/MM/
    storage_path: Mapped[str] = mapped_column(String(500))
    checksum: Mapped[str] = mapped_column(String(128), default="")
    source_worker_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    workflow_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    step_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
