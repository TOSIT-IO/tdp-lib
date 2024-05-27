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
    """Run migrations in 'offline' mode. Not implemented."""
    raise NotImplementedError()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode."""
    env_path = Path(os.environ.get("TDP_ENV", ".env"))
    if env_path.exists:
        load_dotenv(env_path)

    database_dsn = os.environ.get("TDP_ALEMBIC_POSGRESQL_DSN")

    if database_dsn is None:
        raise ValueError("TDP_ALEMBIC_POSGRESQL_DSN env var is missing")

    connectable = create_engine(database_dsn)

    if connectable.dialect.name != "postgresql":
        raise ValueError("You are not connected to a PostgreSQL database")

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
