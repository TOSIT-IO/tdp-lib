# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click

from tdp.cli.utils import collection_paths
from tdp.core.dag import Dag


@click.command(short_help="List nodes from components DAG")
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths,  # transforms list of path into list of Collection
    help=f"List of paths separated by your os' path separator ({os.pathsep})",
)
def nodes(collection_path):
    dag = Dag.from_collections(collection_path)
    endline = "\n- "
    components = endline.join(component for component in dag.get_all_actions())
    click.echo(f"Component list:{endline}{components}")
