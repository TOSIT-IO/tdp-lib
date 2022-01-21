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
    "Compute and display a graph. Add node names to get a subgraph to the nodes."
)


def debug_dag_args():
    parser = argparse.ArgumentParser(description=DAG_SUMMARY)
    parser.description
    parser.add_argument(
        "nodes",
        nargs="*",
        default=None,
        help="Nodes on which to produce ancestors graph",
    )
    return parser


def debug_dag(nodes=None):
    f"""{DAG_SUMMARY}

    This function will lookup in the program arguments to check wether there's a node if no node is specified.
    Args:
        nodes (List[str], optional): Nodes on which to compute a graph. Defaults to None.
    """
    if not nodes:
        nodes = debug_dag_args().parse_args().nodes

    dag = Dag()
    graph = dag.graph
    if nodes:
        # Nx ancestors only returns the ancestor, and node the selected node
        # Add them to simplify visualization
        ancestors = set(nodes)
        for node in nodes:
            ancestors.update(nx.ancestors(graph, node))
        graph = graph.subgraph(ancestors)
    show(graph)
