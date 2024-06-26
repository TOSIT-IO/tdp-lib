# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import click

from tdp.cli.params import collections_option, vars_option
from tdp.core.variables import ClusterVariables

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command()
@collections_option
@vars_option
def validate(collections: Collections, vars: Path):
    """Validate TDP variables against the loaded collections schemas."""
    ClusterVariables.get_cluster_variables(collections, vars, validate=True)
    click.echo("TDP variables are valid")
