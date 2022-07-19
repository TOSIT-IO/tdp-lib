# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import fnmatch
import os
import re

import click
import networkx as nx

from tdp.cli.utils import collection_paths
from tdp.core.dag import Dag

SHORT_DAG_SUMMARY = "Compute and display a graph."

DAG_SUMMARY = SHORT_DAG_SUMMARY + " Add node names to get a subgraph to the nodes."


@click.command(help=DAG_SUMMARY, short_help=SHORT_DAG_SUMMARY)
@click.argument("nodes", nargs=-1)
@click.option(
    "--collection-path",
    envvar="TDP_COLLECTION_PATH",
    required=True,
    callback=collection_paths,  # transforms list of path into Collections
    help=f"List of paths separated by your os' path separator ({os.pathsep})",
)
@click.option(
    "-t",
    "--transitive-reduction",
    is_flag=True,
    help="Apply a transitive reduction on the DAG",
)
@click.option(
    "-g",
    "--glob",
    "pattern_format",
    flag_value="glob",
    help="Each node argument will be process as glob pattern",
)
@click.option(
    "-r",
    "--regex",
    "pattern_format",
    flag_value="regex",
    help="Each node argument will be process as regex pattern",
)
@click.option(
    "-ct",
    "--color-to",
    help="List of node to color to, separated with a comma (,)",
)
@click.option(
    "-cf",
    "--color-from",
    help="Nodes that will be colored after applying get_operations_to_nodes, separed with a comma (,)",
    type=str,
)
@click.option(
    "-c",
    "--cluster",
    is_flag=True,
    help="Group node into cluster inside each service",
)
def dag(
    collection_path,
    nodes,
    transitive_reduction,
    pattern_format,
    color_to,
    color_from,
    cluster,
):
    show = import_show()
    dag = Dag(collection_path)
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
                raise ValueError(f"No nodes found with {nodes}")
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
        nodes_to_color.update(dag.get_operations_to_nodes(color_to.split(",")))
    if color_from:
        nodes_from = dag.get_operations_from_nodes(color_from.split(","))
        if nodes_to_color:
            nodes_to_color = nodes_to_color.intersection(nodes_from)
        else:
            nodes_to_color = nodes_from
    show(graph, nodes_to_color, cluster)


def import_show():
    try:
        import matplotlib
        import pydot
    except ImportError as e:
        raise click.ClickException(
            "You need to install the 'visualization' extras to be able to use the dag command. Run 'poetry install --extras visualization'"
        ) from e
    from tdp.core.dag_dot import show

    return show
