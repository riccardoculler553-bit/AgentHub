"""V1.5: per-task timeout override (tasks.timeout_seconds).

Revision ID: 0007
Revises: 0006
Create Date: 2026-09-14
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("timeout_seconds", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("tasks", "timeout_seconds")
