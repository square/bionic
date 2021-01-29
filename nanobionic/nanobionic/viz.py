
import hsluv
import networkx as nx
import pydot

from . import core


def deps_for_desc(desc):
    return core.rule_for_desc(desc).dep_descs


def graph_from_descs(*descs):
    graph = nx.DiGraph()
    
    def add_desc_to_graph(desc):
        if desc in graph.nodes:
            return
        graph.add_node(desc)
        for dep_desc in deps_for_desc(desc):
            add_desc_to_graph(dep_desc)
            graph.add_edge(dep_desc, desc)
    
    for desc in descs:
        add_desc_to_graph(desc)
    return graph


def hue_for_desc(desc):
    if desc.startswith("<") and desc.endswith(">"):
        return 200
    elif desc.endswith("/future"):
        return 100
    elif desc.endswith("/function"):
        return 80
    if desc.endswith("/digest"):
        return 0
    elif desc.endswith("/provenance"):
        return 30
    elif desc.endswith("artifact"):
        return 280
    else:
        return 170


def dot_from_graph(graph):
    dot = pydot.Dot(
        graph_type='digraph',
        ranksep="0.1",
    )
    
    def clean_name(desc):
        return f'"{desc}"'
    
    def dotify_node(node):
        hue = hue_for_desc(node)
        color = hsluv.hpluv_to_hex([hue, 99, 90])        
        shape = "box" if node.endswith("artifact") else "oval"
        penwidth = 2 if node.startswith("<") and node.endswith(">") else 1
        return pydot.Node(clean_name(node), style="filled", fillcolor=color, shape=shape, penwidth=penwidth)
    
    def dotify_edge(pred, succ):
        if succ == pred + "/function":
            style = "dotted"
        elif succ == pred + "/future":
            style = "dashed"
        else:
            style = "solid"
        arrowhead = "empty"
        return pydot.Edge(clean_name(pred), clean_name(succ), style=style, arrowhead=arrowhead)
    
    cluster_style = "invis"
    clusters_by_node = {
        node: pydot.Cluster(str(ix), style=cluster_style)
        for ix, node in enumerate(sorted(graph.nodes()))
    }

    for node in graph.nodes():
        cluster = clusters_by_node[node]
        cluster.add_node(dotify_node(node))
        if node.endswith("/provenance"):
            sup, _ = node.rsplit("/", 1)
            sup_cluster = clusters_by_node[sup]
            sup_cluster.add_subgraph(cluster)
        else:
            dot.add_subgraph(cluster)
    
    
    for node in graph.nodes():
        for edge in graph.edges(node):
            dot.add_edge(dotify_edge(*edge))
    
    return dot


class Svg:
    def __init__(self, xml_bytes):
        self._xml_bytes = xml_bytes
    
    def _repr_svg_(self):
        return self._xml_bytes.decode("utf8")


def svg_from_dot(dot):
    return Svg(dot.create_svg())


def svg_for_descs(*descs):
    return svg_from_dot(dot_from_graph(graph_from_descs(*descs)))
