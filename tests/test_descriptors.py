import pytest

import attr

from bionic.exception import MalformedDescriptorError
from bionic.descriptors.parsing import (
    dnode_from_descriptor,
    entity_dnode_from_descriptor,
    nondraft_dnode_from_descriptor,
)
from bionic.descriptors.ast import DescriptorNode, DraftNode, EntityNode, TupleNode

from .helpers import assert_re_matches, equal_when_sorted

E = EntityNode
T = lambda *children: TupleNode(children)  # noqa: E731
D = DraftNode
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
        descs=["x", "(x)", "((x))", " x "],
        dnode=E("x"),
    )
    check_roundtrip(
        descs=["()", "(())", "( )"],
        dnode=T(),
    )
    check_roundtrip(
        descs=["x,", "(x,)", "x ,"],
        dnode=T(E("x")),
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
        descs=["(x,), (y, z)", "((x,)),((y,z))"],
        dnode=T(T(E("x")), T(E("y"), E("z"))),
    )
    check_roundtrip(
        descs=["w, (x, (y, z))", "w,(x,(y,z,),),", "(w),(x, ( y , z , ))"],
        dnode=T(E("w"), T(E("x"), T(E("y"), E("z")))),
    )
    check_roundtrip(
        descs=["<x>", "< x >", "(<x>)", "<(x)>"],
        dnode=D(E("x")),
    )
    check_roundtrip(
        descs=["<()>"],
        dnode=D(T()),
    )
    check_roundtrip(
        descs=["<x>,", "<(x)>,"],
        dnode=T(D(E("x"))),
    )
    check_roundtrip(
        descs=["<x,>", "<(x,)>"],
        dnode=D(T(E("x"))),
    )
    check_roundtrip(
        descs=["<x>, y", "(<x>), y", "(<x>, y)"],
        dnode=T(D(E("x")), E("y")),
    )
    check_roundtrip(
        descs=["x, <y>", "x, (<y>)", "(x, <y>)"],
        dnode=T(E("x"), D(E("y"))),
    )
    check_roundtrip(
        descs=["<x>, <y>", "(<x>), < y >"],
        dnode=T(D(E("x")), D(E("y"))),
    )
    check_roundtrip(
        descs=["<x, y>", "(<x, y>)", "<(x, y)>"],
        dnode=D(T(E("x"), E("y"))),
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
        descs=["", "   "],
        pattern=".*empty.*",
    )
    check_failure(
        descs=["9", "-", "(/)", "x + y", "x, y, 7"],
        pattern=".*illegal character.*",
    )
    check_failure(
        descs=["x y", "x, y z", " (x y)"],
        pattern=".*unexpected name.*following.*complete.*",
    )
    check_failure(
        descs=[",x", "x, (,y)"],
        pattern=".*unexpected ','.*no preceding.*",
    )
    check_failure(
        descs=["x,,"],
        pattern=".*unexpected ','.*immediately following another.*",
    )
    check_failure(
        descs=["x()", "(x, y) (z)"],
        pattern=".*unexpected '\\('.*following.*complete.*",
    )
    check_failure(
        descs=["x)", "(x, (y, z)))"],
        pattern=".*unexpected '\\)'.*no matching '\\('.*",
    )
    check_failure(
        descs=["(", "(x, (y, z)"],
        pattern=".*'\\('.*no matching '\\)'.*",
    )
    check_failure(
        descs=["<", "<x"],
        pattern=".*'<'.*no matching '>'.*",
    )
    check_failure(
        descs=[">", "x>", ">x"],
        pattern=".*'>'.*no matching '<'.*",
    )
    check_failure(
        descs=["x<", "x <y>", "<x> y", "<x> <y>", "<x,> y", "<x(>)"],
        pattern=".*unexpected.*following.*complete.*",
    )
    check_failure(
        descs=["<,x>"],
        pattern=".*unexpected ','.*no preceding.*",
    )
    check_failure(
        descs=["(<)x>", "(<x)>"],
        pattern=".*unexpected '\\)'.*expected '>'.*",
    )
    check_failure(
        descs=["<(x>)"],
        pattern=".*unexpected '>'.*expected '\\)'.*",
    )
    check_failure(
        descs=["<>", "< >"],
        pattern=".*no expression between.*'<'.*'>'.*",
    )
    check_failure(
        descs=[
            "<<x>>",
            "<(x, <y>)>",
            "w, <x, (y, <z>)>",
        ],
        pattern=".*'<'.*nested.*",
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


def test_entity_type_checks():
    @attr.s
    class TypeExample:
        descriptor = attr.ib()
        call_is_type = attr.ib()
        call_assume_type = attr.ib()

    DN = DescriptorNode
    examples = [
        TypeExample("x", DN.is_entity, DN.assume_entity),
        TypeExample("x, y", DN.is_tuple, DN.assume_tuple),
        TypeExample("<x>", DN.is_draft, DN.assume_draft),
    ]

    for example in examples:
        dnode = dnode_from_descriptor(example.descriptor)
        assert example.call_is_type(dnode)
        assert example.call_assume_type(dnode) is dnode

        for other_example in examples:
            if other_example is example:
                continue
            assert not other_example.call_is_type(dnode)
            with pytest.raises(TypeError):
                other_example.call_assume_type(dnode)


def test_entity_dnode_from_descriptor():
    assert entity_dnode_from_descriptor("x") == E("x")
    with pytest.raises(ValueError):
        entity_dnode_from_descriptor("x, y")


def test_nondraft_dnode_from_descriptor():
    assert nondraft_dnode_from_descriptor("x") == E("x")
    assert nondraft_dnode_from_descriptor("x, y") == T(E("x"), E("y"))
    with pytest.raises(MalformedDescriptorError):
        nondraft_dnode_from_descriptor("<x>")
    with pytest.raises(MalformedDescriptorError):
        nondraft_dnode_from_descriptor("<x>, <y>")


@pytest.mark.parametrize(
    "descriptor, expected_entity_names",
    [
        ("()", []),
        ("x", ["x"]),
        ("x,", ["x"]),
        ("x, x", ["x", "x"]),
        ("x, y", ["x", "y"]),
        ("x, (y, z)", ["x", "y", "z"]),
        ("x, (y, x)", ["x", "y", "x"]),
    ],
)
def test_all_entity_names(descriptor, expected_entity_names):
    assert equal_when_sorted(
        parse(descriptor).all_entity_names(), expected_entity_names
    )


@pytest.mark.parametrize(
    "descriptor, expected_edited_descriptor",
    [
        ("()", "()"),
        ("x", "X"),
        ("x, y", "X, Y"),
        ("<x>", "<X>"),
        ("<x, y>", "<X, Y>"),
        ("w, <x, (y, z)>", "W, <X, (Y, Z)>"),
    ],
)
def test_edit_uppercase(descriptor, expected_edited_descriptor):
    def to_uppercase(dnode):
        if dnode.is_entity():
            return E(dnode.name.upper())
        else:
            return dnode

    assert (
        parse(descriptor).edit(to_uppercase).to_descriptor()
        == expected_edited_descriptor
    )


@pytest.mark.parametrize(
    "descriptor, expected_edited_descriptor",
    [
        ("()", "()"),
        ("x", "x"),
        ("<x>", "x"),
        ("x, y", "x, y"),
        ("x, <y>", "x, y"),
        ("<x, y>", "x, y"),
        ("w, <x, (y, z)>", "w, (x, (y, z))"),
    ],
)
def test_edit_undraft(descriptor, expected_edited_descriptor):
    def undraft(dnode):
        if dnode.is_draft():
            return dnode.child
        else:
            return dnode

    assert parse(descriptor).edit(undraft).to_descriptor() == expected_edited_descriptor


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
        D(E("x")),
        D(E("y")),
        D(T()),
        D(T(E("x"))),
        T(D(E("x"))),
        D(T(E("x"), E("y"))),
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


def test_dnode_sorting(dnodes):
    def descs_from_dnodes(dnodes):
        return [dnode.to_descriptor() for dnode in dnodes]

    assert descs_from_dnodes(sorted(dnodes)) == sorted(descs_from_dnodes(dnodes))


def test_dnode_equality_and_hashing_are_not_by_identity():
    dnode1 = E("x")
    dnode2 = E("x")
    assert id(dnode1) != id(dnode2)
    assert dnode1 == dnode2
    assert hash(dnode1) == hash(dnode2)
