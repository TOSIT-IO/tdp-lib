# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import alembic_postgresql_enum  # noqa: F401
from alembic import context
from sqlalchemy import engine_from_config, pool

from alembic_migration.migration_basesettings import settings
from tdp.core.models.base_model import BaseModel

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Model's MetaData object here for 'autogenerate' support.
target_metadata = BaseModel.metadata

# Set the the database_dsn if not already set.
if config.get_main_option("sqlalchemy.url") == None:
    config.set_main_option("sqlalchemy.url", settings.TDP_DATABASE_DSN)


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=False,
        )

        with context.begin_transaction():
            context.run_migrations()


run_migrations_online()
