# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import os

import click

from tdp.cli.utils import collection_paths_to_collections
from tdp.core.dag import Dag


@click.command(short_help="List nodes from operations DAG")
@click.option(
    "--collection-path",
    "collections",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths_to_collections,  # transforms into Collections object
    help=f"List of paths separated by your os' path separator ({os.pathsep})",
)
def nodes(collections):
    dag = Dag(collections)
    endline = "\n- "
    operations = endline.join(operation for operation in dag.get_all_operations())
    click.echo(f"Operation list:{endline}{operations}")
