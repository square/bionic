import pytest


@pytest.fixture(scope='function')
def preset_builder(builder):
    builder.declare('x')
    builder.declare('y')
    builder.declare('z')

    @builder
    def xy(x, y):
        return x * y

    @builder
    def yz(y, z):
        return y * z

    @builder
    def xy_plus_yz(xy, yz):
        return xy + yz

    return builder


def test_simple(preset_builder):
    builder = preset_builder

    builder.set('x', 2)
    builder.set('y', 3)
    builder.set('z', 4)

    flow = builder.build()

    assert flow.get('xy') == 6
    assert flow.get('yz') == 12
    assert flow.get('xy_plus_yz') == 18


def test_cartesian_product(preset_builder):
    builder = preset_builder

    builder.set('x', values=[2])
    builder.set('y', values=[3, 4])
    builder.set('z', values=[5, 6, 7])

    flow = builder.build()

    assert flow.get('xy', set) == {2*3, 2*4}  # noqa: E226
    assert flow.get('yz', set) == {3*5, 3*6, 3*7, 4*5, 4*6, 4*7}  # noqa: E226
    assert flow.get('xy_plus_yz', set) == {
        2*3+3*5, 2*3+3*6, 2*3+3*7, 2*4+4*5, 2*4+4*6, 2*4+4*7}  # noqa: E226


def test_empty(preset_builder):
    builder = preset_builder

    builder.set('y', 3)
    builder.set('z', values=[4, 5])

    flow = builder.build()

    assert flow.get('xy', set) == set()
    assert flow.get('yz', set) == {12, 15}
    assert flow.get('xy_plus_yz', set) == set()
