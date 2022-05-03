# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from pathlib import Path

import click

from tdp.core.dag import Dag


@click.command(short_help="List nodes from components DAG")
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    type=Path,
    help="Path to tdp-collection",
)
def nodes(collection_path):
    dag = Dag.from_collection(collection_path)
    endline = "\n- "
    components = endline.join(component for component in dag.get_all_actions())
    click.echo(f"Component list:{endline}{components}")
