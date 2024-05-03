# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import alembic_postgresql_enum  # noqa: F401
from alembic import context
from dotenv import load_dotenv
from sqlalchemy import create_engine

from tdp.core.models.base_model import BaseModel

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Model's MetaData object here for 'autogenerate' support.
target_metadata = BaseModel.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    raise NotImplementedError()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    env_path = Path(os.environ.get("TDP_ENV", ".env"))
    if env_path.exists:
        load_dotenv(env_path)

    database_dsn = os.environ.get(
        "TDP_ALEMBIC_POSGRESQL_DSN", os.environ.get("TDP_DATABASE_DSN", None)
    )

    if database_dsn is None:
        raise ValueError("TDP_DATABASE_DSN env var is missing")

    connectable = create_engine(database_dsn)

    with connectable.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            render_as_batch=False,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
