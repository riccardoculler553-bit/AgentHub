"""AgentHub Command Registry persistent model.

Command = WHAT the system may execute (name/version/schema/timeout).
It never contains business implementation - that belongs to Worker Executors.
"""

from datetime import datetime

from sqlalchemy import JSON, Boolean, DateTime, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import BigIntPK, utcnow


class Command(Base):
    __tablename__ = "commands"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    command_name: Mapped[str] = mapped_column(String(128), unique=True)
    version: Mapped[str] = mapped_column(String(32), default="1.0")
    description: Mapped[str] = mapped_column(String(500), default="")
    # echo | python | yingdao | ... (Executor type on the Worker side)
    executor_type: Mapped[str] = mapped_column(String(32))
    # Executor type specific config, e.g. {"script": "excel_merge.py"}
    executor_config: Mapped[dict] = mapped_column(JSON, default=dict)
    # Simple schema: {"param_name": "string|number|integer|boolean|object|array"}
    params_schema: Mapped[dict] = mapped_column(JSON, default=dict)
    timeout: Mapped[int] = mapped_column(Integer, default=600)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)
