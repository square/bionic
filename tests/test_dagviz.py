"""
Tests for dagviz and FlowImage class.
"""

import pytest
from xml.etree import ElementTree as ET
import pydot
import networkx as nx
import bionic
import bionic.dagviz as dagviz
from PIL import Image


@pytest.fixture
def flow():
    """Create FlowImage fixture for testing"""
    builder = bionic.FlowBuilder("hello_world")
    builder.assign("greeting", "hello world", doc="a friendly greeting")
    return builder.build()


@pytest.fixture
def flow_image(flow):
    return flow.render_dag()


def get_pydot_attributes(index, dot):
    """Helper function to get attributes from pydot graph given index"""
    return dot.get_subgraphs()[index].get_nodes()[0].get_attributes()


def test_save_flowimage_file_path(tmp_path, flow_image):
    """When a file path is given as input, and type is supported by PIL
    check that output image format is preserved."""
    filepath = tmp_path / "test.png"
    flow_image.save(filepath)
    output = Image.open(filepath)
    assert output.format == "PNG"


def test_save_flowimage_file_path_svg(tmp_path, flow_image):
    """When a file path is given as input and svg as the format"""
    filepath = tmp_path / "test.svg"
    flow_image.save(filepath)
    output_text = (tmp_path / "test.svg").read_text()
    try:
        ET.fromstring(output_text)
    except ET.ParseError:
        pytest.fail(
            "output from saving SVG to file object not well formed XML {}".format(
                output_text
            )
        )


def test_save_flowimage_file_object(tmp_path, flow_image):
    """When a file object is given as input, use PIL interface to save"""
    with open(tmp_path / "test.png", "wb") as file_object:
        flow_image.save(file_object, format="png")
    output = Image.open(tmp_path / "test.png")
    assert output.format == "PNG"


def test_save_flowimage_file_object_svg(tmp_path, flow_image):
    """When a file object is given as input and file is svg, use builtin interface to save"""
    with open(tmp_path / "test.svg", "wb") as file_object:
        flow_image.save(file_object, format="svg")
    output_text = (tmp_path / "test.svg").read_text()
    try:
        ET.fromstring(output_text)
    except ET.ParseError:
        pytest.fail(
            "output from saving SVG to file object not well formed XML {}".format(
                output_text
            )
        )


def test_doc_propagated_to_tooltip(flow):
    """Check that docs are propagated to tooltips"""
    G = flow._deriver.export_dag(False)
    dot = dagviz.dot_from_graph(G)
    assert isinstance(dot, pydot.Dot)
    assert get_pydot_attributes(0, dot)["tooltip"] == "a friendly greeting"


def test_missing_doc_empty_tooltip():
    """When doc is missing, tooltip is missing"""
    G = nx.DiGraph()
    G.add_node(0, name="foo", task_ix=0, entity_name="buzz")
    dot = dagviz.dot_from_graph(G)
    # assert tooltip is missing
    assert not hasattr(get_pydot_attributes(0, dot), "tooltip")
