"""
Contains code for visualizing Bionic flow graphs.  Importing this module will
pull in several optional dependencies as well.  The external Graphviz library
is also required.
"""

from pathlib import Path
from collections import defaultdict
from io import BytesIO, IOBase

from .deps.optdep import import_optional_dependency
from .util import rewrap_docstring

module_purpose = "rendering the flow DAG"
hsluv = import_optional_dependency("hsluv", purpose=module_purpose)
pydot = import_optional_dependency("pydot", purpose=module_purpose)
Image = import_optional_dependency("PIL.Image", purpose=module_purpose)


class FlowImage:
    def __init__(self, pydot_graph):
        """
        Given a pydot graph object, create Pillow Image or SVG represented as XML text.
        Replicates the PIL APIs for save and show, for both PIL-supported and SVG formats.
        """
        self._pil_image = Image.open(BytesIO(pydot_graph.create_png()))
        self._xml_bytes = pydot_graph.create_svg()

    def save(self, fp, format=None, **params):
        """
        Save flow visualization to filename, Path, or file object. If file object is passed,
        must also pass format. Pass additional keyword options supported by PIL using params.
        Args:
            fp: Filename (string), pathlib.Path object, or file object
            format: format parameters
            **params: additional keyword options supported by PIL
        """
        is_file_object = isinstance(fp, IOBase)
        use_svg = (format == "svg") or (
            format is None and not is_file_object and Path(fp).suffix == ".svg"
        )
        if use_svg:
            if is_file_object:
                fp.write(self._xml_bytes)
            else:
                with open(fp, "wb") as file:
                    file.write(self._xml_bytes)
        else:
            self._pil_image.save(fp, format, **params)

    def show(self):
        """Show image using PIL"""
        self._pil_image.show()

    def _repr_svg_(self):
        """Rich display image as SVG in IPython notebook or Qt console."""
        return self._xml_bytes.decode("utf8")


def hpluv_color_dict(keys, saturation, lightness):
    """
    Given a list of arbitary keys, generates a dict mapping those keys to a set
    of evenly-spaced, perceptually uniform colors with the specified saturation
    and lightness.
    """

    n = len(keys)
    color_strs = [
        hsluv.hpluv_to_hex([(360 * (i / float(n))), saturation, lightness])
        for i in range(n)
    ]
    return dict(zip(keys, color_strs))


def dot_from_graph(graph, vertical=False, curvy_lines=False, name=None):
    """
    Given a NetworkX directed acyclic graph, returns a Pydot object which can
    be visualized using GraphViz.
    """

    if name is None:
        graph_name = ""
    else:
        graph_name = name

    dot = pydot.Dot(
        graph_name=graph_name,
        graph_type="digraph",
        splines="spline" if curvy_lines else "line",
        outputorder="edgesfirst",
        rankdir="TB" if vertical else "LR",
    )

    node_lists_by_cluster = defaultdict(list)
    for node in graph.nodes():
        descriptor = graph.nodes[node]["descriptor"]
        node_lists_by_cluster[descriptor].append(node)

    descriptors = list(set(graph.nodes[node]["descriptor"] for node in graph.nodes()))
    color_strs_by_descriptor = hpluv_color_dict(
        descriptors, saturation=99, lightness=90
    )

    def name_from_node(node):
        return graph.nodes[node]["name"]

    def doc_from_node(node):
        return graph.nodes[node].get("doc")

    for cluster, node_list in node_lists_by_cluster.items():
        sorted_nodes = list(
            sorted(node_list, key=lambda node: graph.nodes[node]["task_ix"])
        )

        subdot = pydot.Cluster(cluster, style="invis")

        for node in sorted_nodes:
            descriptor = graph.nodes[node]["descriptor"]
            doc = doc_from_node(node)
            dot_node = pydot.Node(
                name_from_node(node),
                style="filled",
                fillcolor=color_strs_by_descriptor[descriptor],
                shape="box",
            )
            if doc:
                tooltip = rewrap_docstring(doc)
                dot_node.set("tooltip", tooltip)
            subdot.add_node(dot_node)

        dot.add_subgraph(subdot)

    for pred_node in graph.nodes():
        for succ_node in graph.successors(pred_node):
            dot.add_edge(
                pydot.Edge(
                    name_from_node(pred_node),
                    name_from_node(succ_node),
                    arrowhead="open",
                    tailport="s" if vertical else "e",
                )
            )

    return dot
