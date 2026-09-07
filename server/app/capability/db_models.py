"""AgentHub Capability Registry persistent model.

Capability = WHAT a device can do. Reported by the Worker via the
device.capabilities envelope, consumed by Dispatcher/Validator.
"""

from datetime import datetime

from sqlalchemy import Boolean, DateTime, String, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import BigIntPK, utcnow


class DeviceCapability(Base):
    __tablename__ = "device_capabilities"
    __table_args__ = (UniqueConstraint("device_id", "command_name", name="uq_device_command"),)

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    device_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    command_name: Mapped[str] = mapped_column(String(128), nullable=False)
    version: Mapped[str] = mapped_column(String(32), default="1.0")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
