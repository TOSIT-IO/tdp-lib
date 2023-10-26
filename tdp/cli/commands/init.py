# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

from tdp.cli.session import init_db
from tdp.cli.utils import collections, database_dsn, validate, vars
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
@collections
@database_dsn
@validate
@vars(exists=False)
def init(
    overrides: tuple[Path],
    collections: Collections,
    database_dsn: str,
    validate: bool,
    vars: Path,
):
    """Initialize the database and the TDP variables."""
    init_db(database_dsn)
    cluster_variables = ClusterVariables.initialize_tdp_vars(
        collections, vars, overrides, validate=validate
    )
    for name, service_manager in cluster_variables.items():
        click.echo(f"{name}: {service_manager.version or 'no version yet'}")
