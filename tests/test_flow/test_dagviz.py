"""
Tests for dagviz and FlowImage class.
"""

import pytest
from xml.etree import ElementTree as ET
from PIL import Image

import bionic as bn
from bionic import dagviz


@pytest.fixture
def flow(builder):
    builder.assign("first_name", values=["Alice", "Bob"])
    builder.assign("last_name", "Smith")

    @builder
    @bn.outputs("full_name", "initials")
    @bn.docs(
        """The full name.""",
        """Just the initials.""",
    )
    def _(first_name, last_name):
        return f"{first_name} {last_name}", f"{first_name[0]}{last_name[0]}"

    @builder
    @bn.gather(over="full_name")
    @bn.returns("all_names,")
    def _(gather_df):
        """Comma-separated list of names."""
        return ", ".join(gather_df["full_name"])

    return builder.build()


@pytest.fixture
def flow_image(flow):
    return flow.render_dag()


@pytest.fixture
def flow_graph(flow):
    return flow._deriver.export_dag()


@pytest.fixture
def flow_dot(flow_graph):
    return dagviz.dot_from_graph(flow_graph)


def nodes_by_name_from_dot(dot):
    return {
        node.get_name(): node
        for subgraph in dot.get_subgraphs()
        for node in subgraph.get_nodes()
    }


def test_dag_size(flow_graph):
    assert len(flow_graph.nodes) == 11


def test_dot_names_and_colors(flow_dot):
    nodes = nodes_by_name_from_dot(flow_dot)
    same_color_name_groups = [
        # We've wrapped all our names in quotes to work around pydot. However, they're
        # not visible in the visualization.
        ['"first_name[0]"', '"first_name[1]"'],
        ['"last_name"'],
        [
            '"<full_name, initials>[0]"',
            '"<full_name, initials>[1]"',
            '"full_name[0]"',
            '"full_name[1]"',
            '"initials[0]"',
            '"initials[1]"',
        ],
        ['"<all_names,>"', '"all_names"'],
    ]

    all_names = [name for name_group in same_color_name_groups for name in name_group]
    assert set(nodes.keys()) == set(all_names)

    all_group_colors = set()
    for name_group in same_color_name_groups:
        group_colors = set(nodes[name].get_fillcolor() for name in name_group)
        assert len(group_colors) == 1
        (group_color,) = group_colors
        assert group_color not in all_group_colors
        all_group_colors.add(group_color)


def test_dot_tooltips(flow_dot):
    nodes = nodes_by_name_from_dot(flow_dot)
    assert nodes['"last_name"'].get_tooltip() is None
    assert nodes['"all_names"'].get_tooltip() == "Comma-separated list of names."
    assert nodes['"initials[0]"'].get_tooltip() == "Just the initials."
    assert nodes['"initials[1]"'].get_tooltip() == "Just the initials."
    assert (
        nodes['"<full_name, initials>[0]"'].get_tooltip()
        == "(Intermediate value) A Python tuple with 2 values."
    )


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
