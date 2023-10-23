# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import click

from tdp.cli.utils import collections, vars
from tdp.core.variables import ClusterVariables


@click.command()
@collections
@vars
def validate(collections, vars):
    """Validate tdp vars against the loaded collections schemas."""
    ClusterVariables.get_cluster_variables(collections, vars, validate=True)
    click.echo("TDP Vars are valid")
