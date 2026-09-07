"""initial tables: users, devices, device_registration_codes, device_tokens, websocket_connections

Revision ID: 0001
Revises:
Create Date: 2026-09-05
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("username", sa.String(length=100), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("username"),
    )

    op.create_table(
        "devices",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.CHAR(length=36), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("name", sa.String(length=100), nullable=False),
        sa.Column("hostname", sa.String(length=255), nullable=True),
        sa.Column("platform", sa.String(length=50), nullable=True),
        sa.Column("client_version", sa.String(length=50), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="offline"),
        sa.Column("last_seen_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.ForeignKeyConstraint(["user_id"], ["users.id"], name="fk_devices_user"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("device_id"),
    )
    op.create_index("ix_devices_status", "devices", ["status"])

    op.create_table(
        "device_registration_codes",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("registration_id", sa.CHAR(length=36), nullable=False),
        sa.Column("code_hash", sa.String(length=128), nullable=False),
        sa.Column("user_id", sa.BigInteger(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=False),
        sa.Column("used_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("registration_id"),
        sa.UniqueConstraint("code_hash"),
    )

    op.create_table(
        "device_tokens",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("device_id", sa.CHAR(length=36), nullable=False),
        sa.Column("token_hash", sa.String(length=128), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.Column("last_used_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("revoked_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("token_hash"),
    )
    op.create_index("ix_device_tokens_device_id", "device_tokens", ["device_id"])

    op.create_table(
        "websocket_connections",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("connection_id", sa.CHAR(length=36), nullable=False),
        sa.Column("device_id", sa.CHAR(length=36), nullable=False),
        sa.Column("connected_at", sa.DateTime(), nullable=False),
        sa.Column("disconnected_at", sa.DateTime(), nullable=True),
        sa.Column("remote_ip", sa.String(length=100), nullable=True),
        sa.Column("close_code", sa.Integer(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("connection_id"),
    )
    op.create_index("ix_websocket_connections_device_id", "websocket_connections", ["device_id"])


def downgrade() -> None:
    op.drop_table("websocket_connections")
    op.drop_table("device_tokens")
    op.drop_table("device_registration_codes")
    op.drop_index("ix_devices_status", table_name="devices")
    op.drop_table("devices")
    op.drop_table("users")
