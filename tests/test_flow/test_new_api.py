"""
These tests are for experimental descriptor-based uses of Bionic's API.
"""

import pytest

import bionic as bn


def test_returns(builder):
    @builder
    @bn.returns("one")
    def _():
        return 1

    @builder
    @bn.returns("two,")
    def _():
        return (2,)

    @builder
    @bn.returns("three, four")
    def _():
        return 3, 4

    @builder
    @bn.returns("five, (six, seven)")
    def _():
        return 5, (6, 7)

    flow = builder.build()

    assert flow.get("one") == 1
    assert flow.get("two") == 2
    assert flow.get("three") == 3
    assert flow.get("four") == 4
    assert flow.get("five") == 5
    assert flow.get("six") == 6
    assert flow.get("seven") == 7


def test_accepts(builder):
    builder.assign("x", 2)
    builder.assign("y", 3)
    builder.assign("z", 4)

    @builder
    @bn.accepts(my_x="x")
    def x_plus_one(my_x):
        return my_x + 1

    @builder
    @bn.accepts(x_="x,")
    def x_plus_two(x_):
        (x,) = x_
        return x + 2

    @builder
    @bn.accepts(my_y="y", my_other_y="y")
    def x_plus_two_y(x, my_y, my_other_y):
        return x + my_y + my_other_y

    @builder
    @bn.accepts(x_y="x, y")
    def x_plus_y(x_y):
        x, y = x_y
        return x + y

    @builder
    @bn.accepts(my_x="x", my_y="y")
    def xy(my_x, my_y):
        return my_x * my_y

    @builder
    @bn.accepts(x_y_z="x, (y, z)")
    def x_plus_y_plus_z(x_y_z):
        x, (y, z) = x_y_z
        return x + y + z

    flow = builder.build()

    assert flow.get("x_plus_one") == 3
    assert flow.get("x_plus_two") == 4
    assert flow.get("x_plus_y") == 5
    assert flow.get("x_plus_two_y") == 8
    assert flow.get("xy") == 6
    assert flow.get("x_plus_y_plus_z") == 9


@pytest.mark.skip("Not implemented yet")
def test_get(builder):
    builder.assign("x", 2)
    builder.assign("y", 3)
    builder.assign("z", 4)

    flow = builder.build()

    assert flow.get("()") == ()
    assert flow.get("x,") == (2,)
    assert flow.get("x, x") == (2, 2)
    assert flow.get("x, y") == (2, 3)
    assert flow.get("x, (y, z)") == (2, (3, 4))
