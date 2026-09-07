"""AgentHub tables: agent_runs (one row per user request / DingTalk message)

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-07
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "agent_runs",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("channel", sa.String(length=32), nullable=False, server_default="dingtalk"),
        sa.Column("conversation_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("sender_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("sender_name", sa.String(length=128), nullable=True),
        sa.Column("message_id", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("input_text", sa.Text(), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="RUNNING"),
        sa.Column("task_id", sa.String(length=64), nullable=True),
        sa.Column("ack_reply", sa.Text(), nullable=False, server_default=""),
        sa.Column("final_reply", sa.Text(), nullable=True),
        sa.Column("reply_webhook", sa.Text(), nullable=True),
        sa.Column("error", sa.String(length=500), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("finished_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("run_id"),
    )
    op.create_index("ix_agent_runs_status", "agent_runs", ["status"])


def downgrade() -> None:
    op.drop_index("ix_agent_runs_status", table_name="agent_runs")
    op.drop_table("agent_runs")
