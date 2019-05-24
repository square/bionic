import pytest
from pytest import raises

import pandas as pd

import bionic as bn
from bionic.exception import UndefinedResourceError

from helpers import count_calls


@pytest.fixture(scope='function')
def preset_builder(builder):
    builder.declare('x')
    builder.assign('y', 1)
    builder.assign('z', values=[2, 3])

    @builder
    def f(x, y):
        return x + y

    @builder
    def g(y, z):
        return y + z

    builder.declare('p')
    builder.declare('q')
    builder.add_case('p', 4, 'q', 5)

    return builder


@pytest.fixture(scope='function')
def preset_flow(preset_builder):
    return preset_builder.build()


# -- Builder API tests.

def test_declare(preset_builder):
    builder = preset_builder

    builder.declare('w')
    builder.set('w', 7)

    assert builder.build().get('w') == 7

    with raises(ValueError):
        builder.declare('x')
    with raises(ValueError):
        builder.declare('y')
    with raises(ValueError):
        builder.declare('z')


def test_declare_protocol(builder):
    protocol = bn.protocol.dillable()
    builder.declare('n', protocol=protocol)
    assert builder.build().resource_protocol('n') == protocol


def test_set(preset_builder):
    builder = preset_builder

    builder.set('x', 5)
    assert builder.build().get('x') == 5

    builder.set('y', 6)
    assert builder.build().get('y') == 6

    builder.set('z', 7)
    assert builder.build().get('z') == 7

    builder.set('f', 8)
    assert builder.build().get('f') == 8


def test_set_multiple(preset_builder):
    builder = preset_builder

    builder.set('x', values=[5, 6])
    assert builder.build().get('x', set) == {5, 6}

    builder.set('y', values=[6, 7])
    assert builder.build().get('y', set) == {6, 7}

    builder.set('z', values=[7, 8])
    assert builder.build().get('z', set) == {7, 8}

    builder.set('f', values=[8, 9])
    assert builder.build().get('f', set) == {8, 9}


def test_assign_single(preset_builder):
    builder = preset_builder

    builder.assign('w', 7)
    assert builder.build().get('w') == 7

    with raises(ValueError):
        builder.assign('x', 7)
    with raises(ValueError):
        builder.assign('y', 7)
    with raises(ValueError):
        builder.assign('z', 7)
    with raises(ValueError):
        builder.assign('f', 7)


def test_assign_multiple(preset_builder):
    builder = preset_builder

    builder.assign('w', values=[1, 2])
    assert builder.build().get('w', set) == {1, 2}

    with raises(ValueError):
        builder.assign('x', values=[1, 2])
    with raises(ValueError):
        builder.assign('y', values=[1, 2])
    with raises(ValueError):
        builder.assign('z', values=[1, 2])
    with raises(ValueError):
        builder.assign('f', values=[1, 2])


def test_add_case(preset_builder):
    builder = preset_builder

    builder.add_case('x', 7)
    assert builder.build().get('x', set) == {7}

    builder.add_case('x', 8)
    assert builder.build().get('x', set) == {7, 8}

    builder.add_case('y', 7)
    assert builder.build().get('y', set) == {1, 7}

    builder.add_case('z', 7)
    assert builder.build().get('z', set) == {2, 3, 7}

    with raises(ValueError):
        builder.add_case('f', 7)

    with raises(UndefinedResourceError):
        builder.add_case('xxx', 7)

    builder.add_case('p', 4, 'q', 6)
    builder.add_case('p', 5, 'q', 6)
    assert builder.build().get('p', set) == {4, 5}
    assert builder.build().get('q', set) == {5, 6}

    with raises(ValueError):
        builder.add_case('p', 7)
    with raises(ValueError):
        builder.add_case('p', 4, 'q', 6)
    builder.declare('r')
    with raises(ValueError):
        builder.add_case('p', 1, 'q', 2, 'r', 3)


def test_then_set(preset_builder):
    builder = preset_builder

    builder.declare('a')
    builder.declare('b')
    builder.declare('c')
    builder.add_case('a', 1, 'b', 2).then_set('c', 3)
    builder.add_case('a', 4, 'b', 5).then_set('c', 6)

    assert builder.build().get('a', set) == {1, 4}
    assert builder.build().get('b', set) == {2, 5}
    assert builder.build().get('c', set) == {3, 6}

    builder.declare('d')
    case = builder.add_case('d', 1)
    with raises(ValueError):
        case.then_set('c', 1)
    with raises(ValueError):
        case.then_set('a', 1)
    with raises(UndefinedResourceError):
        case.then_set('xxx', 1)


def test_clear_cases(preset_builder):
    builder = preset_builder

    builder.clear_cases('x')
    builder.set('x', 7)
    assert builder.build().get('x') == 7

    builder.clear_cases('x')
    builder.set('x', values=[1, 2])
    assert builder.build().get('x', set) == {1, 2}

    builder.clear_cases('y')
    builder.set('y', 8)
    assert builder.build().get('y') == 8

    builder.clear_cases('y')
    builder.set('z', 9)
    assert builder.build().get('z') == 9

    builder.clear_cases('f')
    builder.set('f', 10)
    assert builder.build().get('f') == 10

    with raises(ValueError):
        builder.clear_cases('p')
    builder.clear_cases('p', 'q')


def test_delete(preset_builder):
    builder = preset_builder

    builder.delete('g')
    with raises(UndefinedResourceError):
        builder.build().get('g')
    builder.assign('g', 1)
    builder.build().get('g', set) == {1}

    builder.delete('z')
    with raises(UndefinedResourceError):
        builder.build().get('z', set)

    builder.delete('y')
    with raises(UndefinedResourceError):
        # This fails because f has been invalidated.
        builder.build()


def test_derive(builder):
    builder.assign('a', 1)
    builder.assign('b', 2)

    @builder.derive
    def h(a, b):
        return a + b

    @builder
    def also_h(a, b):
        return a + b

    assert builder.build().get('h') == 3
    assert builder.build().get('also_h') == 3

    builder.delete('a')

    with raises(UndefinedResourceError):
        builder.build().get('h')

# --- Flow API tests.


def test_get_single(preset_flow):
    flow = preset_flow

    with raises(ValueError):
        flow.get('x')

    assert flow.get('y') == 1

    with raises(ValueError):
        assert flow.get('z')
    with raises(ValueError):
        assert flow.get('f')

    assert flow.get('p') == 4
    assert flow.get('q') == 5


def test_get_multiple(preset_flow):
    flow = preset_flow

    assert flow.get('x', set) == set()
    assert flow.get('y', set) == {1}
    assert flow.get('z', set) == {2, 3}
    assert flow.get('f', set) == set()
    assert flow.get('g', set) == {3, 4}
    assert flow.get('p', set) == {4}
    assert flow.get('q', set) == {5}


def test_get_formats(preset_flow):
    flow = preset_flow

    for fmt in [list, 'list']:
        ys = flow.get('y', fmt)
        assert ys == [1]

        zs = flow.get('z', fmt)
        assert zs == [2, 3] or zs == [3, 2]

        ps = flow.get('p', fmt)
        assert ps == [4]

    for fmt in [set, 'set']:
        assert flow.get('y', fmt) == {1}
        assert flow.get('z', fmt) == {2, 3}
        assert flow.get('p', fmt) == {4}

    for fmt in [pd.Series, 'series']:
        y_series = flow.get('y', fmt)
        assert list(y_series) == [1]
        assert y_series.name == 'y'

        z_series = flow.get('z', fmt).sort_values()
        assert list(z_series) == [2, 3]
        assert z_series.name == 'z'
        # This is a convoluted way of accessing the index, but I don't want
        # the test to be sensitive to whether we output a regular index or a
        # MultiIndex.
        z_series_index_df = z_series.index.to_frame()
        assert list(z_series_index_df.columns) == ['z']
        assert list(z_series_index_df['z']) == [2, 3]

        p_series = flow.get('p', fmt)
        assert list(p_series) == [4]
        assert p_series.name == 'p'
        p_series_index_df = p_series.index.to_frame()
        assert list(sorted(p_series_index_df.columns)) == ['p', 'q']
        assert list(p_series_index_df['p']) == [4]
        assert list(p_series_index_df['q']) == [5]


def test_setting(preset_flow):
    flow = preset_flow

    assert flow.get('y') == 1
    assert flow.setting('y', 2).get('y') == 2
    assert flow.setting('y', values=[3, 4]).get('y', set) == {3, 4}

    with raises(UndefinedResourceError):
        flow.setting('xxx', 1)

    assert flow.get('y') == 1


def test_adding_case(preset_flow):
    flow = preset_flow

    assert flow.get('x', set) == set()
    assert flow.adding_case('x', 1).get('x', set) == {1}

    assert flow.get('p', set) == {4}
    assert flow.adding_case('p', 4, 'q', 6).get('q', set) == {5, 6}
    assert flow\
        .adding_case('p', 4, 'q', 6)\
        .adding_case('p', 4, 'q', 7)\
        .get('q', set) == {5, 6, 7}

    with raises(ValueError):
        flow.adding_case('p', 3)

    assert flow.get('x', set) == set()
    assert flow.get('p', set) == {4}
    assert flow.get('q', set) == {5}


def test_then_setting(builder):
    builder.declare('a')
    builder.declare('b')
    builder.declare('c')

    flow0 = builder.build()

    flow1 = flow0\
        .adding_case('a', 1, 'b', 2)\
        .then_setting('c', 3)\

    flow2 = flow1\
        .adding_case('a', 4, 'b', 5)\
        .then_setting('c', 6)\

    assert flow0.get('a', set) == set()
    assert flow0.get('b', set) == set()
    assert flow0.get('c', set) == set()

    assert flow1.get('a', set) == {1}
    assert flow1.get('b', set) == {2}
    assert flow1.get('c', set) == {3}

    assert flow2.get('a', set) == {1, 4}
    assert flow2.get('b', set) == {2, 5}
    assert flow2.get('c', set) == {3, 6}

    assert flow0.get('a', set) == set()
    assert flow0.get('b', set) == set()
    assert flow0.get('c', set) == set()


def test_then_setting_too_soon(builder):
    builder.declare('c')
    flow = builder.build()

    with raises(ValueError):
        flow.then_setting('c', 1)


def test_clearing_cases(preset_flow):
    flow = preset_flow

    assert flow.get('z', set) == {2, 3}
    assert flow.clearing_cases('z').get('z', set) == set()
    assert flow.clearing_cases('z').setting('z', 1).get('z') == 1


def test_all_resource_names(preset_flow):
    assert set(preset_flow.all_resource_names()) == {
        'x', 'y', 'z', 'f', 'g', 'p', 'q'
    }


def test_in_memory_caching(builder):
    builder.assign('x', 2)
    builder.assign('y', 3)

    @builder
    @bn.persist(False)
    @count_calls
    def xy(x, y):
        return x * y

    flow = builder.build()

    assert flow.get('xy') == 6
    assert xy.times_called() == 1

    assert flow.get('xy') == 6
    assert xy.times_called() == 0

    flow = builder.build()

    assert flow.get('xy') == 6
    assert xy.times_called() == 1

    new_flow = flow.setting('y', values=[4, 5])

    assert new_flow.get('xy', set) == {8, 10}
    assert xy.times_called() == 2

    assert new_flow.get('xy', set) == {8, 10}
    assert xy.times_called() == 0

    assert flow.get('xy') == 6
    assert xy.times_called() == 0


def test_to_builder(builder):
    builder.assign('x', 1)
    flow = builder.build()
    assert flow.get('x') == 1

    new_builder = flow.to_builder()
    new_builder.set('x', 2)
    new_flow = new_builder.build()
    assert new_flow.get('x') == 2

    assert flow.get('x') == 1
    assert builder.build().get('x') == 1


def test_shortcuts(builder):
    builder.assign('x', 1)
    flow = builder.build()

    assert flow.get.x() == 1
    assert flow.setting.x(3).get.x() == 3
