# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

from tdp.cli.params import (
    collections_option,
    conf_option,
    database_dsn_option,
    validate_option,
    vars_option,
)

if TYPE_CHECKING:
    from sqlalchemy import Engine

    from tdp.core.collections import Collections


@click.command()
@conf_option
@collections_option
@database_dsn_option
@validate_option
@vars_option(exists=False)
def init(
    conf: tuple[Path],
    collections: Collections,
    db_engine: Engine,
    validate: bool,
    vars: Path,
):
    """Initialize the database and the TDP variables."""

    from tdp.core.models import init_database
    from tdp.core.variables import ClusterVariables

    if not vars.exists():
        vars.mkdir(parents=True)
        click.echo(f"Created TDP variables directory: {vars}")

    init_database(db_engine)
    ClusterVariables.initialize_cluster_variables(
        collections, vars, conf, validate=validate
    )
