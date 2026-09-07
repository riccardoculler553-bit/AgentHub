"""AgentHub V1.1 reliable execution: current_attempt_id, attempt.timeout_at,
agent_runs idempotency (UNIQUE channel+message_id)

Revision ID: 0004
Revises: 0003
Create Date: 2026-09-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # V1.1 §7: the attempt that currently owns a step; device events naming a
    # different attempt are recorded as stale but never mutate state.
    op.add_column("task_steps", sa.Column("current_attempt_id", sa.String(length=64), nullable=True))

    # V1.1 §14: per-attempt deadline (timeout bound to THIS attempt).
    op.add_column("task_attempts", sa.Column("timeout_at", sa.DateTime(), nullable=True))

    # V1.1 §21: DB-level idempotency for DingTalk retries / stream replays.
    # Empty message_id must become NULL first - unique indexes ignore NULLs,
    # so API-created runs (no message id) never collide with each other.
    op.execute("UPDATE agent_runs SET message_id = NULL WHERE message_id = ''")
    op.create_unique_constraint("uq_agent_run_message", "agent_runs", ["channel", "message_id"])


def downgrade() -> None:
    op.drop_constraint("uq_agent_run_message", "agent_runs", type_="unique")
    op.drop_column("task_attempts", "timeout_at")
    op.drop_column("task_steps", "current_attempt_id")
