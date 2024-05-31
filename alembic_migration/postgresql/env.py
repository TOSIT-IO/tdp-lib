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


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode. Not implemented."""
    raise NotImplementedError()


def run_migrations_online() -> None:
    engine_config = config.get_section(config.config_ini_section, {})

    if not "sqlalchemy.url" in engine_config:
        if not (database_dsn := os.environ.get("TDP_ALEMBIC_POSTGRESQL_DSN")):
            raise ValueError("TDP_ALEMBIC_POSTGRESQL_DSN env var is missing")
        engine_config["sqlalchemy.url"] = database_dsn

    connectable = engine_from_config(
        engine_config,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    if connectable.dialect.name != "postgresql":
        raise ValueError("You are not connected to a PostgreSQL database")

    with connectable.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
