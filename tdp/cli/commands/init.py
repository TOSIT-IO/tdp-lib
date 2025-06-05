# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click
from sqlalchemy import Engine

from tdp.cli.params.collections_option import collections_option
from tdp.cli.params.database_dsn_option import database_dsn_option
from tdp.cli.params.validate_option import validate_option
from tdp.cli.params.vars_option import vars_option
from tdp.core.models.base_model import init_database
from tdp.core.variables.cluster_variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.collections.collections import Collections


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
@database_dsn_option
@validate_option
@vars_option(exists=False)
def init(
    overrides: tuple[Path],
    collections: Collections,
    db_engine: Engine,
    validate: bool,
    vars: Path,
):
    """Initialize the database and the TDP variables."""
    init_database(db_engine)
    cluster_variables = ClusterVariables.initialize_cluster_variables(
        collections, vars, overrides, validate=validate
    )
    for name, service_manager in cluster_variables.items():
        click.echo(f"{name}: {service_manager.version}")
