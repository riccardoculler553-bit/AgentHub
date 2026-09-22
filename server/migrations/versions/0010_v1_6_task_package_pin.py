"""V1.6 P0 0.13: tasks.package_id/package_checksum - the capability pin is
complete on the Task row (name+version+package_id+checksum), queryable for
the run history and verifiable against the dispatch envelope.

Revision ID: 0010
Revises: 0009
Create Date: 2026-09-21
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0010"
down_revision: Union[str, None] = "0009"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("tasks", sa.Column("package_id", sa.String(length=64), nullable=True))
    op.add_column("tasks", sa.Column("package_checksum", sa.String(length=64), nullable=True))


def downgrade() -> None:
    op.drop_column("tasks", "package_checksum")
    op.drop_column("tasks", "package_id")
