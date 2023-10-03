# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0


import click

from tdp.cli.utils import collections
from tdp.core.dag import Dag


@click.command(short_help="List nodes from operations DAG")
@collections
def nodes(collections):
    try:
        dag = Dag(collections)
        endline = "\n- "
        operations = endline.join(
            f"{operation.name} {sorted(operation.host_names)}"
            for operation in dag.get_all_operations()
        )
        click.echo(f"Operation list:{endline}{operations}")
    
    except Exception as e:
        raise click.ClickException(e)
