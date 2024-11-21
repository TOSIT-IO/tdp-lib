# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

from __future__ import annotations

import fnmatch
import importlib.util
import re
from typing import TYPE_CHECKING, Optional

import click
import networkx as nx

from tdp.cli.params import collections_option
from tdp.core.dag import Dag

if TYPE_CHECKING:
    from tdp.core.collections import Collections


@click.command()
@click.argument("nodes", nargs=-1)
@click.option(
    "-t",
    "--transitive-reduction",
    is_flag=True,
    help="Apply a transitive reduction on the DAG.",
)
@click.option(
    "-g",
    "--glob",
    "pattern_format",
    flag_value="glob",
    help="Each node argument will be process as glob pattern.",
)
@click.option(
    "-r",
    "--regex",
    "pattern_format",
    flag_value="regex",
    help="Each node argument will be process as regex pattern.",
)
@click.option(
    "-ct",
    "--color-to",
    help="List of nodes to color to, separated by commas (,).",
)
@click.option(
    "-cf",
    "--color-from",
    help="List of nodes to color from after applying `--color-to`, separed by commas (,).",
    type=str,
)
@click.option(
    "-c",
    "--cluster",
    is_flag=True,
    help="Group nodes into cluster inside each service.",
)
@collections_option
def dag(
    collections: Collections,
    cluster: bool,
    transitive_reduction: bool,
    pattern_format: Optional[str] = None,  # TODO use enum
    color_to: Optional[str] = None,
    color_from: Optional[str] = None,
    nodes: Optional[str] = None,
):
    """Compute and display a graph of the DAG.

    Add node names to get a subgraph.
    """
    show = import_show()
    dag = Dag(collections)
    graph = dag.graph
    if nodes:
        if pattern_format:
            nodes_expanded = []
            for node in nodes:
                if pattern_format == "glob":
                    nodes_expanded.extend(fnmatch.filter(graph.nodes, node))
                elif pattern_format == "regex":
                    compiled_regex = re.compile(node)
                    nodes_expanded.extend(filter(compiled_regex.match, graph.nodes))
                else:
                    raise ValueError("pattern_format invalid")
            if not nodes_expanded:
                raise ValueError(f"No nodes found with {nodes}.")
            nodes = nodes_expanded

        # Nx ancestors only returns the ancestor, and node the selected node
        # Add them to simplify visualization
        ancestors = set(nodes)
        for node in nodes:
            ancestors.update(nx.ancestors(graph, node))
        graph = graph.subgraph(ancestors)
    if transitive_reduction:
        graph = nx.transitive_reduction(graph)
    nodes_to_color = set()
    if color_to:
        nodes_to_color.update(
            list(
                map(
                    lambda o: o.name.name,
                    dag.get_operations_to_nodes(color_to.split(",")),
                )
            )
        )
    if color_from:
        nodes_from = list(
            map(
                lambda o: o.name.name,
                dag.get_operations_from_nodes(color_from.split(",")),
            )
        )
        if nodes_to_color:
            nodes_to_color = nodes_to_color.intersection(nodes_from)
        else:
            nodes_to_color = nodes_from
    show(graph, nodes_to_color, cluster)


def import_show():
    for package in ["matplotlib", "pydot"]:
        if importlib.util.find_spec(package) is None:
            raise click.ClickException(
                "You need to install the 'visualization' extras to be able to use the "
                + "`tdp dag` command. Run `poetry install --extras visualization`."
            )

    from tdp.core.dag_dot import show

    return show
