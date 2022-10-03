# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click

from tdp.cli.session import init_db
from tdp.cli.utils import collection_paths
from tdp.core.dag import Dag
from tdp.core.repository.repository import NoVersionYet
from tdp.core.service_manager import ServiceManager


@click.command(short_help="Init database / services in tdp vars")
@click.option(
    "--database-dsn",
    envvar="TDP_DATABASE_DSN",
    required=True,
    type=str,
    help=(
        "Database Data Source Name, in sqlalchemy driver form "
        "example: sqlite:////data/tdp.db or sqlite+pysqlite:////data/tdp.db. "
        "You might need to install the relevant driver to your installation (such "
        "as psycopg2 for postgresql)"
    ),
)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths,  # transforms list of path into Collections
    help=f"List of paths separated by your os' path separator ({os.pathsep})",
)
@click.option(
    "--vars",
    envvar="TDP_VARS",
    required=True,
    type=click.Path(resolve_path=True, path_type=Path),
    help="Path to the tdp vars",
)
@click.option(
    "--overrides",
    required=False,
    type=click.Path(exists=True, resolve_path=True, path_type=Path),
    help="Path to tdp vars overrides",
)
def init(database_dsn, collection_path, vars, overrides):
    dag = Dag(collection_path)
    init_db(database_dsn)
    service_managers = ServiceManager.initialize_service_managers(dag, vars, overrides)
    for name, service_manager in service_managers.items():
        click.echo(f"{name}: {service_manager.version}")
