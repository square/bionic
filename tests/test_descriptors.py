import pytest

from bionic.exception import MalformedDescriptorError
from bionic.descriptors.parsing import (
    dnode_from_descriptor,
    entity_dnode_from_descriptor,
)
from bionic.descriptors.ast import EntityNode, TupleNode

from .helpers import assert_re_matches

E = EntityNode
T = lambda *children: TupleNode(children)  # noqa: E731
parse = dnode_from_descriptor


def check_roundtrip(descs, dnode):
    """
    Checks that a group of descriptor strings all parse to the provided descriptor node,
    and that that dnode can be converted back to the first descriptor string.
    """

    for desc in descs:
        assert parse(desc) == dnode
    assert dnode.to_descriptor() == descs[0]


def test_parsing_and_unparsing():
    check_roundtrip(
        descs=["x", "(x)", "((x))", " x "], dnode=E("x"),
    )
    check_roundtrip(
        descs=["()", "(())", "( )"], dnode=T(),
    )
    check_roundtrip(
        descs=["x,", "(x,)", "x ,"], dnode=T(E("x")),
    )
    check_roundtrip(
        descs=["x, y", "x,y", "x,  y", "x, y,", "(x,y)", "(x), y"],
        dnode=T(E("x"), E("y")),
    )
    check_roundtrip(
        descs=["x, y, z", "x,y,z", "x, y, z,", "(x,y,z)"],
        dnode=T(E("x"), E("y"), E("z")),
    )
    check_roundtrip(
        descs=["x, (y,)", "x,(y,)", "(x), (y,)", "x,( y ,)"],
        dnode=T(E("x"), T(E("y"))),
    )
    check_roundtrip(
        descs=["x, (y, z)", "x,(y,  z)", "x,(y,z,),"],
        dnode=T(E("x"), T(E("y"), E("z"))),
    )
    check_roundtrip(
        descs=["(x,), (y, z)", "((x,)),((y,z))"], dnode=T(T(E("x")), T(E("y"), E("z"))),
    )
    check_roundtrip(
        descs=["w, (x, (y, z))", "w,(x,(y,z,),),", "(w),(x, ( y , z , ))"],
        dnode=T(E("w"), T(E("x"), T(E("y"), E("z")))),
    )


@pytest.mark.parametrize("descriptor", ["data", "data2", "data_data", "__1"])
def test_valid_descriptor_names(descriptor):
    assert parse(descriptor) == E(descriptor)


def check_failure(descs, pattern):
    """
    Checks that each descriptor string fails to parse, with an error message matching
    the provided pattern.
    """
    for desc in descs:
        with pytest.raises(MalformedDescriptorError) as excinfo:
            parse(desc)
        assert_re_matches(pattern, str(excinfo.value))


def test_malformed_descriptors():
    check_failure(
        descs=["", "   "], pattern=".*empty.*",
    )
    check_failure(
        descs=["9", "-", "(/)", "x + y", "x, y, 7"], pattern=".*illegal character.*",
    )
    check_failure(
        descs=["x y", "x, y z", " (x y)"],
        pattern=".*unexpected name.*following.*complete.*",
    )
    check_failure(
        descs=[",x", "x, (,y)"], pattern=".*unexpected ','.*no preceding.*",
    )
    check_failure(
        descs=["x,,"], pattern=".*unexpected ','.*immediately following another.*",
    )
    check_failure(
        descs=["x()", "(x, y) (z)"],
        pattern=".*unexpected '\\('.*following.*complete.*",
    )
    check_failure(
        descs=["x)", "(x, (y, z)))"], pattern=".*unexpected '\\)'.*no matching '\\('.*",
    )
    check_failure(
        descs=["(", "(x, (y, z)"], pattern=".*'\\('.*no matching '\\)'.*",
    )


def test_exact_error_message():
    with pytest.raises(MalformedDescriptorError) as excinfo:
        parse("x-")
    assert str(excinfo.value) == (
        "Unable to parse descriptor 'x-': illegal character '-' (at position 1)"
    )

    with pytest.raises(MalformedDescriptorError) as excinfo:
        parse("x, y,,")
    assert str(excinfo.value) == (
        "Unable to parse descriptor 'x, y,,': "
        "found unexpected ',' (at position 5) immediately following another ','"
    )


def test_descriptor_to_entity_name():
    assert E("x").to_entity_name() == "x"

    with pytest.raises(TypeError):
        T(E("x")).to_entity_name()


def test_entity_dnode_from_descriptor():
    assert entity_dnode_from_descriptor("x") == E("x")
    with pytest.raises(ValueError):
        entity_dnode_from_descriptor("x, y")


@pytest.fixture
def dnodes():
    return [
        E("x"),
        E("y"),
        T(),
        T(E("x")),
        T(E("x"), E("x")),
        T(E("x"), E("y")),
        T(T(E("x"))),
    ]


def test_dnode_equality(dnodes):
    for ix1, dnode1 in enumerate(dnodes):
        for ix2, dnode2 in enumerate(dnodes):
            if ix1 == ix2:
                assert dnode1 == dnode2
            else:
                assert dnode1 != dnode2


def test_dnode_hashing(dnodes):
    dnode_dict = {}
    for dnode in dnodes:
        assert dnode not in dnode_dict
        dnode_dict[dnode] = dnode
        assert dnode_dict[dnode] == dnode
