# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os
from pathlib import Path

import click

from tdp.cli.utils import collection_paths
from tdp.core.dag import Dag


@click.command(short_help="List nodes from operations DAG")
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
    operations = endline.join(operation for operation in dag.get_all_operations())
    click.echo(f"Operation list:{endline}{operations}")
