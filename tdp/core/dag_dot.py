# Copyright 2022 TOSIT.IO
# SPDX-License-Identifier: Apache-2.0

import networkx as nx


# Needed :
#   pip install pydot
def to_pydot(graph, nodes_to_color=None):
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
        if dot_node.get_name() in nodes_to_color:
            dot_node.set_color("indianred")
            dot_node.add_style("filled")
        pydot_graph.add_node(dot_node)
    for dot_edge in dot_edges:
        if (
            dot_edge.get_source() in nodes_to_color
            and dot_edge.get_destination() in nodes_to_color
        ):
            dot_edge.set_color("indianred")
        pydot_graph.add_edge(dot_edge)

    return pydot_graph


# Needed :
#   pip install matplotlib
#   apt install graphviz
def show(graph, nodes_to_color=None):
    if isinstance(graph, nx.classes.Graph):
        graph = to_pydot(graph, nodes_to_color)

    import io
    import matplotlib.pyplot as plt
    import matplotlib.image as mpimg

    dot_png = graph.create(format="png")

    sio = io.BytesIO()
    sio.write(dot_png)
    sio.seek(0)
    img = mpimg.imread(sio)

    imgplot = plt.imshow(img)
    plt.show()
