"""Workflow persistent models (V1.3 §62/§148-§153).

workflows / workflow_steps  = definitions (immutable per (name, version))
workflow_runs / workflow_step_runs / workflow_events = executions

A WorkflowStepRun owns at most one current Task (§19); Task retries stay
inside the Task Engine as attempts (§20).
"""

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import BigIntPK, utcnow


class Workflow(Base):
    __tablename__ = "workflows"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    workflow_id: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(64), index=True)
    version: Mapped[str] = mapped_column(String(32), default="1.0.0")
    description: Mapped[str] = mapped_column(String(500), default="")
    # DRAFT / ENABLED / DISABLED (§108); only ENABLED workflows may run
    status: Mapped[str] = mapped_column(String(16), default="DRAFT")
    active_singleton: Mapped[bool] = mapped_column(default=False)
    risk_level: Mapped[str] = mapped_column(String(16), default="ACTION")
    requires_confirmation: Mapped[bool] = mapped_column(default=False)
    definition_json: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow, onupdate=utcnow)


class WorkflowStep(Base):
    __tablename__ = "workflow_steps"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    step_id: Mapped[str] = mapped_column(String(64), unique=True)
    workflow_id: Mapped[str] = mapped_column(String(64), ForeignKey("workflows.workflow_id"), index=True)
    # step name doubles as the context reference key ({{ steps.<name>... }})
    name: Mapped[str] = mapped_column(String(64))
    order_no: Mapped[int] = mapped_column(Integer, default=1)
    command: Mapped[str] = mapped_column(String(128))
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    device_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    # stop | retry
    on_failure: Mapped[str] = mapped_column(String(16), default="stop")
    retry_policy: Mapped[dict] = mapped_column(JSON, default=dict)
    enabled: Mapped[bool] = mapped_column(default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), unique=True)
    workflow_id: Mapped[str] = mapped_column(String(64), ForeignKey("workflows.workflow_id"), index=True)
    # denormalized for singleton checks / audit without a join
    workflow_name: Mapped[str] = mapped_column(String(64), index=True)
    workflow_version: Mapped[str] = mapped_column(String(32))
    # PENDING/RUNNING/SUCCESS/FAILED/CANCELLED
    status: Mapped[str] = mapped_column(String(16), default="PENDING", index=True)
    # agent | api | manual | system (scheduler reserved, §13)
    trigger_type: Mapped[str] = mapped_column(String(16), default="api")
    created_by: Mapped[str] = mapped_column(String(64), default="admin")
    # {"variables": {...}, "steps": {name: {"status","task_id","result"}}} (§95)
    context_json: Mapped[dict] = mapped_column(JSON, default=dict)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    current_step_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class WorkflowStepRun(Base):
    __tablename__ = "workflow_step_runs"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    step_run_id: Mapped[str] = mapped_column(String(64), unique=True)
    run_id: Mapped[str] = mapped_column(String(64), ForeignKey("workflow_runs.run_id"), index=True)
    workflow_step_id: Mapped[str] = mapped_column(String(64), ForeignKey("workflow_steps.step_id"))
    name: Mapped[str] = mapped_column(String(64))
    order_no: Mapped[int] = mapped_column(Integer, default=1)
    command: Mapped[str] = mapped_column(String(128))
    task_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    # PENDING/READY/RUNNING/SUCCESS/FAILED/CANCELLED/SKIPPED
    status: Mapped[str] = mapped_column(String(16), default="PENDING", index=True)
    retry_count: Mapped[int] = mapped_column(Integer, default=0)
    result: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class WorkflowEvent(Base):
    __tablename__ = "workflow_events"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    run_id: Mapped[str] = mapped_column(String(64), index=True)
    step_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # workflow.created/started/step_ready/step_started/step_success/
    # step_failed/step_retry/completed/failed/cancelled (§59)
    event_type: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)


def ensure_task_source_columns(engine) -> None:
    """Best-effort column ensure for existing production tables (§148/§154).

    Base.metadata.create_all creates the columns on fresh databases (tests,
    new installs), but never alters existing tables - MySQL production needs
    the explicit ADD COLUMN. Idempotent via inspector check."""
    from sqlalchemy import inspect, text

    inspector = inspect(engine)
    if "tasks" not in inspector.get_table_names():
        return
    existing = {c["name"] for c in inspector.get_columns("tasks")}
    statements = []
    if "source_type" not in existing:
        statements.append(
            "ALTER TABLE tasks ADD COLUMN source_type VARCHAR(32) NULL DEFAULT 'API'"
        )
    if "workflow_run_id" not in existing:
        statements.append("ALTER TABLE tasks ADD COLUMN workflow_run_id VARCHAR(64) NULL")
    if "workflow_step_run_id" not in existing:
        statements.append("ALTER TABLE tasks ADD COLUMN workflow_step_run_id VARCHAR(64) NULL")
    for stmt in statements:
        try:
            with engine.begin() as conn:
                conn.execute(text(stmt))
        except Exception:  # noqa: BLE001 - concurrent startup may race the ALTER
            pass
