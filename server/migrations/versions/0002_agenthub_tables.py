"""AgentHub V1.0 tables: commands, device_capabilities, tasks, task_steps, task_attempts, task_events

Revision ID: 0002
Revises: 0001
Create Date: 2026-09-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "commands",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("command_name", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False, server_default="1.0"),
        sa.Column("description", sa.String(length=500), nullable=False, server_default=""),
        sa.Column("executor_type", sa.String(length=32), nullable=False),
        sa.Column("executor_config", sa.JSON(), nullable=True),
        sa.Column("params_schema", sa.JSON(), nullable=True),
        sa.Column("timeout", sa.Integer(), nullable=False, server_default="600"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("command_name"),
    )

    op.create_table(
        "device_capabilities",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.String(length=36), nullable=False),
        sa.Column("command_name", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False, server_default="1.0"),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id", "command_name", name="uq_device_command"),
    )
    op.create_index("ix_device_capabilities_device_id", "device_capabilities", ["device_id"])

    op.create_table(
        "tasks",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=200), nullable=False, server_default=""),
        sa.Column("created_by", sa.String(length=64), nullable=False, server_default="admin"),
        sa.Column("target_device_id", sa.String(length=36), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="PENDING"),
        sa.Column("priority", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_attempts", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("scheduled_at", sa.DateTime(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("timeout_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_id"),
    )
    op.create_index("ix_tasks_status", "tasks", ["status"])
    op.create_index("ix_tasks_target_device_id", "tasks", ["target_device_id"])

    op.create_table(
        "task_steps",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("step_id", sa.String(length=64), nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("order_no", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("device_id", sa.String(length=36), nullable=True),
        sa.Column("command", sa.String(length=128), nullable=False),
        sa.Column("params", sa.JSON(), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="PENDING"),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.task_id"], name="fk_task_steps_task"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("step_id"),
    )

    op.create_table(
        "task_attempts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("attempt_id", sa.String(length=64), nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("step_id", sa.String(length=64), nullable=False),
        sa.Column("device_id", sa.String(length=36), nullable=True),
        sa.Column("attempt_no", sa.Integer(), nullable=False, server_default="1"),
        sa.Column("status", sa.String(length=32), nullable=False, server_default="DISPATCHING"),
        sa.Column("dispatch_message_id", sa.String(length=64), nullable=True),
        sa.Column("accepted_at", sa.DateTime(), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.Column("error_code", sa.String(length=64), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.ForeignKeyConstraint(["task_id"], ["tasks.task_id"], name="fk_task_attempts_task"),
        sa.ForeignKeyConstraint(["step_id"], ["task_steps.step_id"], name="fk_task_attempts_step"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("attempt_id"),
    )
    op.create_index("ix_task_attempts_task_id", "task_attempts", ["task_id"])
    op.create_index("ix_task_attempts_step_id", "task_attempts", ["step_id"])

    op.create_table(
        "task_events",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("task_id", sa.String(length=64), nullable=False),
        sa.Column("step_id", sa.String(length=64), nullable=True),
        sa.Column("attempt_id", sa.String(length=64), nullable=True),
        sa.Column("event_type", sa.String(length=32), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_task_events_task_id", "task_events", ["task_id"])


def downgrade() -> None:
    op.drop_index("ix_task_events_task_id", table_name="task_events")
    op.drop_table("task_events")
    op.drop_index("ix_task_attempts_step_id", table_name="task_attempts")
    op.drop_index("ix_task_attempts_task_id", table_name="task_attempts")
    op.drop_table("task_attempts")
    op.drop_table("task_steps")
    op.drop_index("ix_tasks_target_device_id", table_name="tasks")
    op.drop_index("ix_tasks_status", table_name="tasks")
    op.drop_table("tasks")
    op.drop_index("ix_device_capabilities_device_id", table_name="device_capabilities")
    op.drop_table("device_capabilities")
    op.drop_table("commands")
