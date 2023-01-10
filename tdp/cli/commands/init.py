# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import click

from tdp.cli.session import init_db
from tdp.cli.utils import collections, database_dsn, validate, vars
from tdp.core.variables import ClusterVariables


@click.command(short_help="Init database / services in tdp vars")
@click.option(
    "--overrides",
    envvar="TDP_OVERRIDES",
    required=False,
    type=click.Path(exists=True, resolve_path=True, path_type=Path),
    help="Path to tdp vars overrides",
)
@collections
@database_dsn
@validate
@vars
def init(overrides, collections, database_dsn, validate, vars):
    init_db(database_dsn)
    cluster_variables = ClusterVariables.initialize_cluster_variables(
        collections, vars, overrides, validate=validate
    )
    for name, service_manager in cluster_variables.items():
        click.echo(f"{name}: {service_manager.version}")
