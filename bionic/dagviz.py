from collections import defaultdict
from io import BytesIO

import pydot
import hsluv
from PIL import Image


def hpluv_color_dict(keys, saturation, lightness):
    '''
    Given a list of arbitary keys, generates a dict mapping those keys to a set
    of evenly-spaced, perceptually uniform colors with the specified saturation
    and lightness.
    '''

    n = len(keys)
    color_strs = [
        hsluv.hpluv_to_hex([(360 * (i / float(n))), saturation, lightness])
        for i in range(n)
    ]
    return dict(zip(keys, color_strs))


def dot_from_graph(graph, vertical=False, curvy_lines=False):
    '''
    Given a NetworkX directed acyclic graph, returns a Pydot object which can
    be visualized using GraphViz.
    '''

    dot = pydot.Dot(
        graph_type='digraph',
        splines='spline' if curvy_lines else 'line',
        outputorder='edgesfirst',
        rankdir='TB' if vertical else 'LR',
    )

    node_lists_by_cluster = defaultdict(list)
    for node in graph.nodes():
        resource_name = graph.nodes[node]['resource_name']
        node_lists_by_cluster[resource_name].append(node)

    resource_names = list(set(
        graph.nodes[node]['resource_name']
        for node in graph.nodes()
    ))
    color_strs_by_resource_name = hpluv_color_dict(
        resource_names, saturation=99, lightness=90)

    def name_from_node(node):
        return graph.nodes[node]['name']

    for cluster, node_list in node_lists_by_cluster.items():
        sorted_nodes = list(sorted(
            node_list, key=lambda node: graph.nodes[node]['task_ix']))

        subdot = pydot.Cluster(cluster, style='invis')

        for node in sorted_nodes:
            resource_name = graph.nodes[node]['resource_name']
            subdot.add_node(pydot.Node(
                name_from_node(node),
                style='filled',
                fillcolor=color_strs_by_resource_name[resource_name],
                shape='box',
            ))

        dot.add_subgraph(subdot)

    for pred_node in graph.nodes():
        for succ_node in graph.successors(pred_node):
            dot.add_edge(pydot.Edge(
                name_from_node(pred_node),
                name_from_node(succ_node),
                arrowhead='open',
                tailport='s' if vertical else 'e',
            ))

    return dot


def image_from_dot(dot):
    '''
    Given a pydot graph object, renders it into a Pillow Image.
    '''

    return Image.open(BytesIO(dot.create_png()))
