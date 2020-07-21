import pytest

from ..helpers import equal_when_sorted


def test_group_pairs():
    from bionic.utils.misc import group_pairs

    assert group_pairs([]) == []
    assert group_pairs([1, 2]) == [(1, 2)]
    assert group_pairs([1, 2, 3, 4, 5, 6]) == [(1, 2), (3, 4), (5, 6)]

    with pytest.raises(ValueError):
        group_pairs([1])
    with pytest.raises(ValueError):
        group_pairs([1, 2, 3])


def test_immutable_sequence():
    from bionic.utils.misc import ImmutableSequence

    class Seq(ImmutableSequence):
        def __init__(self, items):
            super(Seq, self).__init__(items)

    seq = Seq([1, 2, 3])

    assert seq[0] == 1
    assert seq[2] == 3
    assert seq[-2] == 2

    assert list(seq) == [1, 2, 3]
    assert len(seq) == 3

    assert 1 in seq
    assert 4 not in seq

    assert {seq: 7}[seq] == 7

    assert seq == Seq([1, 2, 3])
    assert seq != Seq([1, 3, 2])
    assert seq != [1, 2, 3]

    assert seq < Seq([1, 3, 2])
    assert seq <= Seq([1, 3, 2])
    assert Seq([1, 3, 2]) > seq
    assert Seq([1, 3, 2]) >= seq


def test_immutable_mapping():
    from bionic.utils.misc import ImmutableMapping

    class Mapping(ImmutableMapping):
        def __init__(self, values_by_key):
            super(Mapping, self).__init__(values_by_key)

    mapping = Mapping({"a": 1, "b": 2})

    assert mapping["a"] == 1
    assert mapping["b"] == 2
    with pytest.raises(KeyError):
        mapping["c"]

    assert mapping.get("a") == 1
    assert mapping.get("c") is None

    assert {mapping: 7}[mapping] == 7

    assert equal_when_sorted(list(mapping), ["a", "b"])
    assert dict(mapping) == {"a": 1, "b": 2}
    assert equal_when_sorted(list(mapping.keys()), ["a", "b"])
    assert equal_when_sorted(list(mapping.values()), [1, 2])
    assert equal_when_sorted(list(mapping.items()), [("a", 1), ("b", 2)])
    assert equal_when_sorted(list(mapping.keys()), ["a", "b"])
    assert equal_when_sorted(list(mapping.values()), [1, 2])
    assert equal_when_sorted(list(mapping.items()), [("a", 1), ("b", 2)])

    assert mapping == Mapping({"a": 1, "b": 2})
    assert mapping != {"a": 1, "b": 2}
    assert mapping != Mapping({"b": 1, "a": 2})
    assert mapping < Mapping({"b": 1, "a": 2})
    assert mapping <= Mapping({"b": 1, "a": 2})
    assert Mapping({"b": 1, "a": 2}) > mapping
    assert Mapping({"b": 1, "a": 2}) >= mapping


def test_oneline():
    from bionic.utils.misc import oneline

    assert oneline("one two") == "one two"
    assert oneline(" one two ") == "one two"
    assert oneline("\none\ntwo") == "one two"
    assert (
        oneline(
            """
       one
       two   three"""
        )
        == "one two   three"
    )
    assert (
        oneline(
            """
       one
       two

       three
       """
        )
        == "one two three"
    )


def test_clean_docstring():
    from bionic.utils.misc import rewrap_docstring

    assert rewrap_docstring("") == ""
    assert rewrap_docstring("test") == "test"
    assert rewrap_docstring("test one two") == "test one two"
    assert rewrap_docstring("test\none\ntwo") == "test one two"
    assert rewrap_docstring("test 1. 2.") == "test 1. 2."
    assert rewrap_docstring("test\n\none\ntwo") == "test\none two"

    doc = """
    test
    """
    assert rewrap_docstring(doc) == "test"

    doc = """
    test one
    two
    """
    assert rewrap_docstring(doc) == "test one two"

    doc = """
    test one
        two
    """
    assert rewrap_docstring(doc) == "test one two"

    doc = """
    test one

    two
    """
    assert rewrap_docstring(doc) == "test one\ntwo"

    doc = """
    test
    - one
    - two
    """
    assert rewrap_docstring(doc) == "test\n- one\n- two"

    doc = """
    test
    + one
    + two
    """
    assert rewrap_docstring(doc) == "test\n+ one\n+ two"

    doc = """
    test
    * one
    * two
    """
    assert rewrap_docstring(doc) == "test\n* one\n* two"

    doc = """
    test
    1. one
    2. two
    """
    assert rewrap_docstring(doc) == "test\n1. one\n2. two"

    doc = """
    test
    1) one
    2) two
    """
    assert rewrap_docstring(doc) == "test\n1) one\n2) two"

    doc = """
    test
    10) one
    20) two
    """
    assert rewrap_docstring(doc) == "test\n10) one\n20) two"

    doc = """
    test
    a) one
    b) two
    """
    assert rewrap_docstring(doc) == "test\na) one\nb) two"

    doc = """
    test
    1.0
    """
    assert rewrap_docstring(doc) == "test 1.0"

    doc = """
    test (one
    two)
    """
    assert rewrap_docstring(doc) == "test (one two)"

    doc = """
    test
    - one
    - two

    - three
    - four
    """
    assert rewrap_docstring(doc) == "test\n- one\n- two\n\n- three\n- four"

    doc = """
    test
    - one
    two
    """
    assert rewrap_docstring(doc) == "test\n- one two"

    doc = """
    test
    - one

    two
    """
    assert rewrap_docstring(doc) == "test\n- one\ntwo"
