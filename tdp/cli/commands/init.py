# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click
from sqlalchemy import Engine

from tdp.cli.params import (
    collections_option,
    database_dsn_option,
    validate_option,
    vars_option,
)
from tdp.cli.params.overrides_option import overrides_option
from tdp.core.models import init_database
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command()
@overrides_option
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
