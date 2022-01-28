import argparse
import fnmatch
import re
import networkx as nx

from tdp.core.dag import Dag
from tdp.core.dag_dot import show

try:
    import matplotlib
    import pydot
except ImportError as e:
    raise RuntimeError(
        "You need to install the 'visualization' extras to be able to use the devtools. Run 'poetry install --extras visualization'"
    ) from e


DAG_SUMMARY = (
    "Compute and display a graph. Add node names to get a subgraph to the nodes."
)


def debug_dag_args():
    parser = argparse.ArgumentParser(description=DAG_SUMMARY)
    parser.description
    group_pattern_format = parser.add_mutually_exclusive_group()
    group_pattern_format.add_argument(
        "-g",
        action="store_true",
        help="Each node argument will be process as glob pattern",
    )
    group_pattern_format.add_argument(
        "-r",
        action="store_true",
        help="Each node argument will be process as regex pattern",
    )
    parser.add_argument(
        "nodes",
        nargs="*",
        default=None,
        help="Nodes on which to produce ancestors graph",
    )
    return parser


def debug_dag(nodes=None, pattern_format=None):
    f"""{DAG_SUMMARY}

    This function will lookup in the program arguments to check wether there's a node if no node is specified.
    Args:
        nodes (List[str], optional): Nodes on which to compute a graph. Defaults to None.
        pattern_format (str, optional): Pattern format for nodes argument: glob or regex
    """
    if not nodes:
        args = debug_dag_args().parse_args()
        nodes = args.nodes

        if not pattern_format:
            if args.g:
                pattern_format = "glob"
            elif args.r:
                pattern_format = "regex"

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
    show(graph)
