import argparse
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
    "Compute and display a graph. Add a node name to get a subgraph to the node."
)


def debug_dag_args():
    parser = argparse.ArgumentParser(description=DAG_SUMMARY)
    parser.description
    parser.add_argument(
        "node",
        type=str,
        nargs="?",
        default=None,
        help="Node on which to produce ancestors graph",
    )
    return parser


def debug_dag(node=None):
    f"""{DAG_SUMMARY}

    This function will lookup in the program arguments to check wether there's a node if no node is specified.
    Args:
        node (str, optional): Node on which to compute a graph. Defaults to None.
    """
    if not node:
        node = debug_dag_args().parse_args().node

    dag = Dag()
    graph = dag.graph
    if node:
        ancestors = nx.ancestors(graph, node)
        # Nx ancestors only returns the ancestor, and node the selected node
        # Add it to simplify visualization
        ancestors.add(node)
        graph = graph.subgraph(ancestors)
    show(graph)
