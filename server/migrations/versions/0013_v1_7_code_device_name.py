"""V1.7 follow-up: device_registration_codes.device_name - the operator-assigned
name rides with the enrollment code (register falls back to it).

Revision ID: 0013
Revises: 0012
Create Date: 2026-09-22
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0013"
down_revision: Union[str, None] = "0012"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "device_registration_codes",
        sa.Column("device_name", sa.String(length=100), nullable=True, server_default=""),
    )


def downgrade() -> None:
    op.drop_column("device_registration_codes", "device_name")
