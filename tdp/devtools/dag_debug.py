import networkx as nx
import sys

from tdp.core.dag import Dag
from tdp.core.dag_dot import show

try:
    import matplotlib
    import pydot
except ImportError as e:
    raise RuntimeError(
        "You need to install the 'visualization' extras to be able to use the devtools. Run 'poetry install --extras visualization'"
    ) from e


def debug_dag(node=None):
    """Compute and display a graph. Add a node name to get a subgraph to the node.

    This method will lookup in the program arguments to check wether there's a node if no node is specified.
    Args:
        node (str, optional): Node to which compute a graph. Defaults to None.
    """
    dag = Dag()
    if not node and len(sys.argv) == 2:
        node = sys.argv[1]
    graph = dag.graph
    if node:
        graph = graph.subgraph(nx.ancestors(graph, node))
    show(graph)
