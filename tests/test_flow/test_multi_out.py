import pytest

import bionic as bn


def test_no_doc(builder):
    @builder
    @bn.outputs("a", "b")
    def f():
        return 1, 2

    flow = builder.build()
    assert flow.entity_doc("a") is None
    assert flow.entity_doc("b") is None


def test_multi_docs(builder):
    @builder
    @bn.outputs("a", "b")
    @bn.docs("a doc", "b doc")
    def f():
        return 1, 2

    flow = builder.build()
    assert flow.entity_doc("a") == "a doc"
    assert flow.entity_doc("b") == "b doc"


def test_multi_docs_decorated_first(builder):
    @builder
    @bn.docs("a doc", "b doc")
    @bn.outputs("a", "b")
    def f():
        return 1, 2

    flow = builder.build()
    assert flow.entity_doc("a") == "a doc"
    assert flow.entity_doc("b") == "b doc"


def test_too_many_docs(builder):
    with pytest.raises(ValueError):

        @builder
        @bn.docs("a doc", "b doc")
        def f():
            return 1, 2


def test_too_few_docs(builder):
    with pytest.warns(Warning):

        @builder
        @bn.outputs("a", "b")
        def f():
            "a and b doc"
            return 1, 2

    flow = builder.build()
    assert flow.entity_doc("a") == "a and b doc"
    assert flow.entity_doc("b") == "a and b doc"


def test_multi_default_protocols(builder):
    @builder
    @bn.outputs("a", "b")
    def f():
        return 1, 2

    flow = builder.build()
    assert flow.entity_protocol("a") == bn.flow.DEFAULT_PROTOCOL
    assert flow.entity_protocol("b") == bn.flow.DEFAULT_PROTOCOL


def test_multi_custom_protocols(builder):
    protocol = bn.protocol.dillable()

    @builder
    @bn.outputs("a", "b")
    @protocol
    def f():
        return 1, 2

    flow = builder.build()
    assert flow.entity_protocol("a") == protocol
    assert flow.entity_protocol("b") == protocol


def test_multi_custom_protocols_decorated_first(builder):
    protocol = bn.protocol.dillable()

    @builder
    @protocol
    @bn.outputs("a", "b")
    def f():
        return 1, 2

    flow = builder.build()
    assert flow.entity_protocol("a") == protocol
    assert flow.entity_protocol("b") == protocol
