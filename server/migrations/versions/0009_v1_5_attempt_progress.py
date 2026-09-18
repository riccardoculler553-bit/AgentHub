"""V1.5 Phase 8: task_attempts.progress_json/progress_seq (monotonic progress
snapshot per attempt - the dashboard can never show stale/reordered progress).

Revision ID: 0009
Revises: 0008
Create Date: 2026-09-16
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0009"
down_revision: Union[str, None] = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("task_attempts", sa.Column("progress_seq", sa.Integer(), nullable=True))
    op.add_column("task_attempts", sa.Column("progress_json", sa.JSON(), nullable=True))


def downgrade() -> None:
    op.drop_column("task_attempts", "progress_json")
    op.drop_column("task_attempts", "progress_seq")
