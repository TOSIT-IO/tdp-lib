# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

from tdp.cli.init_db import init_db
from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    validate_option,
    vars_option,
)
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.collections import Collections


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
    database_dsn: str,
    validate: bool,
    vars: Path,
):
    """Initialize the database and the TDP variables."""
    init_db(database_dsn)
    cluster_variables = ClusterVariables.initialize_cluster_variables(
        collections, vars, overrides, validate=validate
    )
    for name, service_manager in cluster_variables.items():
        click.echo(f"{name}: {service_manager.version}")
