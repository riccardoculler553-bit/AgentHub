"""V1.5 Phase 2: tasks.pending_since (fresh offline-max-wait window per
PENDING entry, so retried old tasks are not insta-timed-out by created_at).

Revision ID: 0008
Revises: 0007
Create Date: 2026-09-16
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0008"
down_revision: Union[str, None] = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("pending_since", sa.DateTime(), nullable=True))


def downgrade() -> None:
    op.drop_column("tasks", "pending_since")
