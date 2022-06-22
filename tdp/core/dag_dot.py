# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import networkx as nx

from tdp.core.operation import Operation


# Needed :
#   pip install pydot
def to_pydot(graph, nodes_to_color=None, cluster_service=False):
    if not nodes_to_color:
        nodes_to_color = []
    pydot_graph = nx.nx_pydot.to_pydot(graph)

    dot_nodes = pydot_graph.get_nodes()
    dot_edges = pydot_graph.get_edges()
    for dot_node in dot_nodes:
        pydot_graph.del_node(dot_node)
    for dot_edge in dot_edges:
        pydot_graph.del_edge((dot_edge.get_source(), dot_edge.get_destination()))

    # Hack to add node defaults at the first position
    pydot_graph.set_node_defaults(shape="box", fontname="Roboto")
    for dot_node in dot_nodes:
        if dot_node.get_name().strip('"') in nodes_to_color:
            dot_node.set_color("indianred")
            dot_node.add_style("filled")
        pydot_graph.add_node(dot_node)
    for dot_edge in dot_edges:
        if (
            dot_edge.get_source().strip('"') in nodes_to_color
            and dot_edge.get_destination().strip('"') in nodes_to_color
        ):
            dot_edge.set_color("indianred")
        pydot_graph.add_edge(dot_edge)

    if cluster_service:
        import pydot

        subgraphs = {}
        for dot_node in dot_nodes:
            # Dot node name can be quoted, remove it
            operation_name = dot_node.get_name().strip('"')
            operation = Operation(operation_name)
            subgraphs.setdefault(
                operation.service,
                pydot.Cluster(
                    operation.service, label=operation.service, fontname="Roboto"
                ),
            ).add_node(pydot.Node(operation_name))

        for service_name, subgraph in sorted(subgraphs.items()):
            pydot_graph.add_subgraph(subgraph)

    return pydot_graph


# Needed :
#   pip install matplotlib
#   apt install graphviz
def show(graph, *args, **kwargs):
    if isinstance(graph, nx.classes.Graph):
        graph = to_pydot(graph, *args, **kwargs)

    import io

    import matplotlib.image as mpimg
    import matplotlib.pyplot as plt

    dot_png = graph.create(format="png")

    sio = io.BytesIO()
    sio.write(dot_png)
    sio.seek(0)
    img = mpimg.imread(sio)

    imgplot = plt.imshow(img)
    plt.show()
