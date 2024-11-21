# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import click
from alembic.command import stamp, upgrade
from alembic.config import Config
from sqlalchemy import MetaData, Table, create_engine

from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    validate_option,
    vars_option,
)
from tdp.core.models import init_database
from tdp.core.variables import ClusterVariables
from tdp.find_tdp_lib_root_folder import find_tdp_lib_root_folder

if TYPE_CHECKING:
    from tdp.core.collections import Collections

logging.getLogger("alembic").setLevel(logging.ERROR)


@click.command()
@click.option(
    "--overrides",
    envvar="TDP_OVERRIDES",
    required=False,
    type=click.Path(exists=True, resolve_path=True, path_type=Path),
    multiple=True,
    help="Path to TDP variables overrides. Can be used multiple times. Last one takes precedence.",
)
@collections_option
@database_dsn_option(create_engine=False)
@validate_option
@vars_option(exists=False)
def init(
    overrides: tuple[Path],
    collections: Collections,
    database_dsn: str,
    validate: bool,
    vars: Path,
):
    """Initialize the database and the TDP variables."""
    engine = create_engine(database_dsn)
    with engine.connect() as connection:
        tdp_lib_folder_path = find_tdp_lib_root_folder()
        if tdp_lib_folder_path and not (tdp_lib_folder_path / "alembic.ini").exists():
            raise click.ClickException("alembic.ini file could not be found.")

        alembic_config = Config(
            file_=f"{tdp_lib_folder_path}/alembic.ini",
            ini_section=engine.dialect.name,
        )
        alembic_config.set_main_option("sqlalchemy.url", database_dsn)

        try:
            alembic_version = Table("alembic_version", MetaData(), autoload_with=engine)
            db_revision_id_row = connection.execute(alembic_version.select()).fetchone()
            db_revision_id = db_revision_id_row[0] if db_revision_id_row else None

        except:
            db_revision_id = None

        # Upgrade database if new revision in migration folder, else create all tables or fail if revisions don't coincide.
        if db_revision_id:
            upgrade(config=alembic_config, revision="head")
            click.echo(f"Upgrade tables to revision ID: {db_revision_id}")
        else:
            init_database(engine)
            stamp(config=alembic_config, revision="head")
    # Create the cluster variables.
    cluster_variables = ClusterVariables.initialize_cluster_variables(
        collections, vars, overrides, validate=validate
    )
    for name, service_manager in cluster_variables.items():
        click.echo(f"{name}: {service_manager.version}")
