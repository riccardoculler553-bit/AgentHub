"""V1.6 P0 0.18: users.role - single-tenant RBAC role per identity
(admin | operator | viewer). HTTP enforcement is token-based (auth/rbac.py).

Revision ID: 0011
Revises: 0010
Create Date: 2026-09-21
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0011"
down_revision: Union[str, None] = "0010"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("users", sa.Column("role", sa.String(length=16), nullable=True, server_default="admin"))
    # backfill NULLs (rows created before this migration) to admin
    op.execute("UPDATE users SET role = 'admin' WHERE role IS NULL")


def downgrade() -> None:
    op.drop_column("users", "role")
