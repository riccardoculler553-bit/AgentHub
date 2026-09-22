"""V1.7: worker_environments (environment inventory + drift fingerprint) and
worker_processes (persistent ProcessInstance registry).

Revision ID: 0012
Revises: 0011
Create Date: 2026-09-22
"""

from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "0012"
down_revision: Union[str, None] = "0011"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "capability_versions",
        sa.Column("config", sa.JSON(), nullable=True, server_default="{}"),
    )
    op.create_table(
        "worker_environments",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("device_id", sa.String(length=36), nullable=False, unique=True, index=True),
        sa.Column("snapshot", sa.JSON(), nullable=False),
        sa.Column("fingerprint", sa.String(length=64), nullable=True),
        sa.Column("worker_version", sa.String(length=32), nullable=True),
        sa.Column("hostname", sa.String(length=255), nullable=True),
        sa.Column("collected_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
    )
    op.create_table(
        "worker_processes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, primary_key=True),
        sa.Column("process_id", sa.String(length=64), nullable=False, unique=True),
        sa.Column("device_id", sa.String(length=36), nullable=False, index=True),
        sa.Column("capability", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("package_id", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=16), nullable=False, index=True),
        sa.Column("pid", sa.Integer(), nullable=True),
        sa.Column("restart_count", sa.Integer(), nullable=False),
        sa.Column("requested_by", sa.String(length=64), nullable=True),
        sa.Column("last_error", sa.String(length=500), nullable=True),
        sa.Column("started_at", sa.DateTime(), nullable=True),
        sa.Column("stopped_at", sa.DateTime(), nullable=True),
        sa.Column("last_health_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.UniqueConstraint("device_id", "capability", "version", name="uq_worker_process_target"),
    )


def downgrade() -> None:
    op.drop_table("worker_processes")
    op.drop_table("worker_environments")
    op.drop_column("capability_versions", "config")
