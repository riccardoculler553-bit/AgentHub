"""AgentHub V1.4: Capability Runtime tables + task capability columns

Revision ID: 0006
Revises: 0005
Create Date: 2026-09-09
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- capability definitions (§8/§60) ---
    op.create_table(
        "capabilities",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("display_name", sa.String(length=200), nullable=False, server_default=""),
        sa.Column("description", sa.String(length=1000), nullable=False, server_default=""),
        sa.Column("type", sa.String(length=16), nullable=False, server_default="ATOMIC"),
        sa.Column("runtime_type", sa.String(length=16), nullable=False),
        sa.Column("enabled", sa.Boolean(), nullable=False, server_default=sa.text("1")),
        sa.Column("current_version", sa.String(length=32), nullable=True),
        sa.Column("input_schema", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("output_schema", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("risk_level", sa.String(length=16), nullable=False, server_default="READ"),
        sa.Column("requires_confirmation", sa.Boolean(), nullable=False, server_default=sa.text("0")),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("updated_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name", name="uq_capability_name"),
    )

    # --- packages (§10/§60) ---
    op.create_table(
        "capability_packages",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("package_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("storage_path", sa.String(length=500), nullable=False),
        sa.Column("size", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("checksum", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("runtime_type", sa.String(length=16), nullable=False),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("package_id", name="uq_capability_package_id"),
    )
    op.create_index("ix_capability_packages_name", "capability_packages", ["name"])

    # --- immutable versions (§9/§60) ---
    op.create_table(
        "capability_versions",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("capability_name", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("package_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="DRAFT"),
        sa.Column("input_schema", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("output_schema", sa.JSON(), nullable=False, server_default="{}"),
        sa.Column("entrypoint", sa.String(length=200), nullable=False, server_default="main"),
        sa.Column("checksum", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("capability_name", "version", name="uq_capability_version"),
        sa.ForeignKeyConstraint(["capability_name"], ["capabilities.name"]),
        sa.ForeignKeyConstraint(["package_id"], ["capability_packages.package_id"]),
    )
    op.create_index("ix_capability_versions_capability_name", "capability_versions", ["capability_name"])
    op.create_index("ix_capability_versions_status", "capability_versions", ["status"])

    # --- worker-reported installed capabilities (§17/§60) ---
    op.create_table(
        "worker_capabilities",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("worker_id", sa.String(length=36), nullable=False),
        sa.Column("capability_name", sa.String(length=128), nullable=False),
        sa.Column("version", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=16), nullable=False, server_default="READY"),
        sa.Column("last_seen_at", sa.DateTime(), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("worker_id", "capability_name", "version", name="uq_worker_capability"),
    )
    op.create_index("ix_worker_capabilities_worker_id", "worker_capabilities", ["worker_id"])

    # --- artifacts (§29/§60) ---
    op.create_table(
        "artifacts",
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("artifact_id", sa.String(length=64), nullable=False),
        sa.Column("name", sa.String(length=255), nullable=False, server_default=""),
        sa.Column("type", sa.String(length=32), nullable=False, server_default="file"),
        sa.Column("mime_type", sa.String(length=128), nullable=True),
        sa.Column("size", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("storage_path", sa.String(length=500), nullable=False),
        sa.Column("checksum", sa.String(length=128), nullable=False, server_default=""),
        sa.Column("source_worker_id", sa.String(length=36), nullable=True),
        sa.Column("task_id", sa.String(length=64), nullable=True),
        sa.Column("workflow_run_id", sa.String(length=64), nullable=True),
        sa.Column("step_run_id", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(), nullable=False),
        sa.Column("expires_at", sa.DateTime(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("artifact_id", name="uq_artifact_id"),
    )
    op.create_index("ix_artifacts_task_id", "artifacts", ["task_id"])
    op.create_index("ix_artifacts_workflow_run_id", "artifacts", ["workflow_run_id"])
    op.create_index("ix_artifacts_source_worker_id", "artifacts", ["source_worker_id"])

    # --- minimal-invasive task extension (§32/§59) ---
    op.add_column(
        "tasks",
        sa.Column("execution_type", sa.String(length=16), nullable=False, server_default="LEGACY_COMMAND"),
    )
    op.add_column("tasks", sa.Column("capability_name", sa.String(length=128), nullable=True))
    op.add_column("tasks", sa.Column("capability_version", sa.String(length=32), nullable=True))
    op.add_column("tasks", sa.Column("artifact_ids", sa.JSON(), nullable=False, server_default="[]"))
    op.create_index("ix_tasks_capability_name", "tasks", ["capability_name"])

    # --- workflow step capability fields (§24) ---
    op.add_column("workflow_steps", sa.Column("capability_version", sa.String(length=32), nullable=True))
    op.add_column("workflow_step_runs", sa.Column("capability_version", sa.String(length=32), nullable=True))


def downgrade() -> None:
    op.drop_column("workflow_step_runs", "capability_version")
    op.drop_column("workflow_steps", "capability_version")
    op.drop_index("ix_tasks_capability_name", table_name="tasks")
    op.drop_column("tasks", "artifact_ids")
    op.drop_column("tasks", "capability_version")
    op.drop_column("tasks", "capability_name")
    op.drop_column("tasks", "execution_type")
    op.drop_index("ix_artifacts_source_worker_id", table_name="artifacts")
    op.drop_index("ix_artifacts_workflow_run_id", table_name="artifacts")
    op.drop_index("ix_artifacts_task_id", table_name="artifacts")
    op.drop_table("artifacts")
    op.drop_index("ix_worker_capabilities_worker_id", table_name="worker_capabilities")
    op.drop_table("worker_capabilities")
    op.drop_index("ix_capability_versions_status", table_name="capability_versions")
    op.drop_index("ix_capability_versions_capability_name", table_name="capability_versions")
    op.drop_table("capability_versions")
    op.drop_index("ix_capability_packages_name", table_name="capability_packages")
    op.drop_table("capability_packages")
    op.drop_table("capabilities")
