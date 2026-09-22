"""AgentHub Task persistent models: tasks / task_steps / task_attempts / task_events.

Task = full business lifecycle (NOT a message). One task may span several steps,
each step may have several attempts (retries). task_events keeps the whole
lifecycle for dashboard timeline / audit / agent state recovery.
"""

from datetime import datetime

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.database import Base
from app.db.models import BigIntPK, utcnow


class Task(Base):
    __tablename__ = "tasks"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), unique=True)
    name: Mapped[str] = mapped_column(String(200), default="")
    created_by: Mapped[str] = mapped_column(String(64), default="admin")
    target_device_id: Mapped[str | None] = mapped_column(String(36), nullable=True, index=True)
    # PENDING/DISPATCHING/SENT/ACCEPTED/RUNNING/SUCCESS/FAILED/TIMEOUT/CANCELLED
    status: Mapped[str] = mapped_column(String(32), default="PENDING", index=True)
    # V1.3: where this task came from (API/AGENT/WORKFLOW/MANUAL, §32/§154)
    source_type: Mapped[str | None] = mapped_column(String(32), nullable=True, default="API")
    # V1.3 audit trail: reverse lookup Task -> WorkflowStepRun -> WorkflowRun (§31/§155)
    workflow_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    workflow_step_run_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # V1.4 §32/§73: LEGACY_COMMAND (V1.0-V1.3 commands) | CAPABILITY (Capability
    # Runtime). Old tasks keep working unchanged; capability tasks carry the
    # capability identity for the resolver and audit trail.
    execution_type: Mapped[str] = mapped_column(String(16), default="LEGACY_COMMAND")
    capability_name: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    capability_version: Mapped[str | None] = mapped_column(String(32), nullable=True)
    # V1.6 P0 0.13: the pin is complete only with package identity on the Task
    # row - name+version+package_id+checksum must agree with the dispatch
    # envelope and be queryable for the run history (they used to live only
    # in the transient envelope).
    package_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    package_checksum: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # artifact ids produced by this task (uploaded via the Artifact plane, §45)
    artifact_ids: Mapped[list] = mapped_column(JSON, default=list)
    priority: Mapped[int] = mapped_column(Integer, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, default=3)
    # V1.5: per-task timeout override (seconds); None = capability_default_timeout
    timeout_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    # Phase 2: when the task last entered PENDING (create/retry/rollback).
    # The offline-max-wait watchdog uses this, NOT created_at - a retried old
    # task must get a fresh dispatch window instead of timing out instantly.
    pending_since: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    scheduled_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    timeout_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TaskStep(Base):
    __tablename__ = "task_steps"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    step_id: Mapped[str] = mapped_column(String(64), unique=True)
    task_id: Mapped[str] = mapped_column(String(64), ForeignKey("tasks.task_id"), index=True)
    order_no: Mapped[int] = mapped_column(Integer, default=1)
    # device_id is inherited from the task in V1.0; nullable column reserved
    # for future cross-device workflows.
    device_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    command: Mapped[str] = mapped_column(String(128))
    params: Mapped[dict] = mapped_column(JSON, default=dict)
    # PENDING/RUNNING/SUCCESS/FAILED/TIMEOUT/CANCELLED
    status: Mapped[str] = mapped_column(String(32), default="PENDING")
    # V1.1: the attempt that currently owns this step. Device events whose
    # attempt_id differs are recorded as stale but never mutate state.
    current_attempt_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)


class TaskAttempt(Base):
    __tablename__ = "task_attempts"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    attempt_id: Mapped[str] = mapped_column(String(64), unique=True)
    task_id: Mapped[str] = mapped_column(String(64), ForeignKey("tasks.task_id"), index=True)
    step_id: Mapped[str] = mapped_column(String(64), ForeignKey("task_steps.step_id"), index=True)
    device_id: Mapped[str | None] = mapped_column(String(36), nullable=True)
    attempt_no: Mapped[int] = mapped_column(Integer, default=1)
    # DISPATCHING/SENT/ACCEPTED/RUNNING/SUCCESS/FAILED/TIMEOUT/CANCELLED
    status: Mapped[str] = mapped_column(String(32), default="DISPATCHING")
    dispatch_message_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    accepted_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    # V1.1: per-attempt deadline (task.timeout_at is refreshed per dispatch;
    # attempt.timeout_at makes the timeout bound to THIS attempt only).
    timeout_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    error_code: Mapped[str | None] = mapped_column(String(64), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
    # Phase 8: monotonic progress snapshot. progress_seq is the worker's
    # per-attempt counter; an event with seq <= progress_seq never overwrites
    # the snapshot, so the dashboard can never move backwards.
    progress_seq: Mapped[int | None] = mapped_column(Integer, nullable=True)
    progress_json: Mapped[dict | None] = mapped_column(JSON, nullable=True)


class TaskEvent(Base):
    __tablename__ = "task_events"

    id: Mapped[int] = mapped_column(BigIntPK, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(64), index=True)
    step_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    attempt_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    # task.created/dispatching/sent/accepted/running/progress/success/failed/
    # cancelled/timeout/retry_requested
    event_type: Mapped[str] = mapped_column(String(32))
    payload: Mapped[dict] = mapped_column(JSON, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow)
