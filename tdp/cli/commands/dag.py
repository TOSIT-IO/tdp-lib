# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import fnmatch
import re

import click
import networkx as nx

from tdp.cli.context import pass_dag
from tdp.core.dag import Dag
from tdp.core.dag_dot import show

try:
    import matplotlib
    import pydot
except ImportError as e:
    raise RuntimeError(
        "You need to install the 'visualization' extras to be able to use the dag command. Run 'poetry install --extras visualization'"
    ) from e

SHORT_DAG_SUMMARY = "Compute and display a graph."

DAG_SUMMARY = SHORT_DAG_SUMMARY + " Add node names to get a subgraph to the nodes."


@click.command(help=DAG_SUMMARY, short_help=SHORT_DAG_SUMMARY)
@click.argument("nodes", nargs=-1)
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
@pass_dag
def dag(dag, nodes, transitive_reduction, pattern_format):
    dag = Dag()
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
    show(graph)
