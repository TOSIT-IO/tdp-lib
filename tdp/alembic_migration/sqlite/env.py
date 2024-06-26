# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os

from alembic import context
from sqlalchemy import engine_from_config, pool

from tdp.core.models.base_model import BaseModel

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Model's MetaData object here for 'autogenerate' support.
target_metadata = BaseModel.metadata


def load_dotenv():
    """Load the .env file based on the TDP_ENV environment variable."""
    import os
    from pathlib import Path

    from dotenv import load_dotenv

    env_path = Path(os.environ.get("TDP_ENV", ".env"))
    if env_path.exists:
        load_dotenv(env_path)


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode. Not implemented."""
    raise NotImplementedError()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""

    # Get the configuration based on the current section name
    engine_config = config.get_section(config.config_ini_section, {})

    # Override the database URL with the one from the environment if not set
    if not "sqlalchemy.url" in engine_config:
        load_dotenv()
        if not (database_dsn := os.environ.get("TDP_ALEMBIC_SQLITE_DSN")):
            raise ValueError("TDP_ALEMBIC_SQLITE_DSN env var is missing")
        engine_config["sqlalchemy.url"] = database_dsn

    connectable = engine_from_config(
        engine_config,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    # Ensure that the provided database url is of the proper dialect
    if connectable.dialect.name != "sqlite":
        raise ValueError("You are not connected to an SQLite database")

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
