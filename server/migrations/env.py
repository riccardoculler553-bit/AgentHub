"""Alembic environment: reads the database URL from app settings."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from alembic import context  # noqa: E402
from sqlalchemy import engine_from_config, pool  # noqa: E402

from app.core.config import settings  # noqa: E402
from app.db.database import Base  # noqa: E402
from app.db import models  # noqa: F401,E402 - register models on Base
from app.command import db_models as command_models  # noqa: F401,E402 - AgentHub tables
from app.capability import db_models as capability_models  # noqa: F401,E402
from app.task import db_models as task_models  # noqa: F401,E402

config = context.config
# Escape '%' for configparser interpolation (passwords may be URL-encoded)
config.set_main_option("sqlalchemy.url", settings.database_url.replace("%", "%%"))

target_metadata = Base.metadata


def run_migrations_offline() -> None:
    context.configure(
        url=settings.database_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
