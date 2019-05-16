import pytest

import math

from helpers import count_calls

import bionic as bn


@pytest.fixture(scope='function')
def builder(tmp_path):
    builder = bn.FlowBuilder()
    builder.set('core__storage_cache__dir_name', str(tmp_path))
    return builder


# It would be nice to move the builder setup into fixtures, but since we need
# to access the bound functions as well (to check the number of times they were
# called), it's easiest to just have one long test.
def test_caching_and_invalidation(builder, tmp_path):
    # Set up the builder with singleton values.

    builder.assign('x', 2)
    builder.assign('y', 3)
    builder.assign('z', 4)

    @builder
    @bn.persist
    @count_calls
    def xy(x, y):
        return x * y

    @builder
    @count_calls
    def yz(y, z):
        return y * z

    @builder
    @bn.persist
    @count_calls
    def xy_plus_yz(xy, yz):
        return xy + yz

    # Access the downstream values.
    flow = builder.build()

    assert flow.get('xy') == 6
    assert flow.get('xy') == 6
    assert xy.times_called() == 1

    assert flow.get('yz') == 12
    assert flow.get('yz') == 12
    assert yz.times_called() == 1

    assert flow.get('xy_plus_yz') == 18
    assert flow.get('xy_plus_yz') == 18
    assert xy_plus_yz.times_called() == 1

    # Rebuild the flow (resetting the in-memory cache) and confirm that
    # xy and xy_plus_yz are still cached.
    flow = builder.build()

    assert flow.get('xy') == 6
    assert xy.times_called() == 0

    assert flow.get('yz') == 12
    # Note yz is not cached.
    assert yz.times_called() == 1

    assert flow.get('xy_plus_yz') == 18
    assert xy_plus_yz.times_called() == 0

    # Change the value of z, and confirm that yz and xy_plus_yz are recomputed.
    flow = flow.setting('z', -4)

    assert flow.get('xy') == 6
    assert xy.times_called() == 0

    assert flow.get('yz') == -12
    assert yz.times_called() == 1

    assert flow.get('xy_plus_yz') == -6
    assert flow.get('xy_plus_yz') == -6
    assert xy_plus_yz.times_called() == 1

    # Update x and y to have multiple values, and confirm that xy and
    # xy_plus_yz are recomputed.
    flow = builder.build()\
        .setting('x', values=[2, -2])\
        .setting('y', values=[3, 6])

    assert flow.get('xy', set) == {-2*6, -2*3, 2*3, 2*6}  # noqa: E226
    # TODO Note that we actually called xy one more time than necessary,
    # because we already computed the case where x=2, y=3.  Unfortunately the
    # cache system stores it in a different place because we're using case
    # keys, so we don't find the cached version.
    assert xy.times_called() == 4

    assert flow.get('yz', set) == {3*4, 6*4}  # noqa: E226
    assert yz.times_called() == 2

    assert flow.get('xy_plus_yz', set) == {
        -2*3+3*4, -2*6+6*4, 2*3+3*4, 2*6+6*4}  # noqa: E226
    assert xy.times_called() == 0
    assert yz.times_called() == 0
    assert xy_plus_yz.times_called() == 4

    flow = builder.build()\
        .setting('x', values=[2, -2])\
        .setting('y', values=[3, 6])

    assert flow.get('xy', set) == {-12, -6, 6, 12}
    assert xy.times_called() == 0

    assert flow.get('yz', set) == {3*4, 6*4}  # noqa: E226
    assert yz.times_called() == 2

    assert flow.get('xy_plus_yz', set) == {
        -2*3+3*4, -2*6+6*4, 2*3+3*4, 2*6+6*4}  # noqa: E226
    assert xy.times_called() == 0
    assert yz.times_called() == 0
    assert xy_plus_yz.times_called() == 0

    # Update y to have a different, overlapped set of values, and check that
    # the minimal set of recomputations are performed.

    flow = flow.setting('y', values=[6, 9])

    assert flow.get('xy', set) == {-2*6, -2*9, 2*6, 2*9}  # noqa: E226
    assert xy.times_called() == 2

    assert flow.get('yz', set) == {6*4, 9*4}  # noqa: E226
    assert yz.times_called() == 2

    assert flow.get('xy_plus_yz', set) == {
        -2*6+6*4, -2*9+9*4, 2*6+6*4, 2*9+9*4}  # noqa: E226
    assert xy_plus_yz.times_called() == 2

    # This is mainly just to check that the cache wrapper returns a sane set of
    # case keys.
    key_names = flow.get('xy_plus_yz', 'series').index.names
    for name in ['x', 'y']:
        assert name in key_names


def test_versioning(builder):
    builder.assign('x', 2)
    builder.assign('y', 3)

    @builder
    @bn.persist
    @count_calls
    def f(x, y):
        return x + y

    assert builder.build().get('f') == 5
    assert builder.build().get('f') == 5
    assert f.times_called() == 1

    builder.delete('f')

    @builder
    @bn.persist
    @count_calls
    def f(x, y):
        return x * y

    assert builder.build().get('f') == 5
    assert f.times_called() == 0

    builder.delete('f')

    @builder
    @bn.persist
    @bn.version(1)
    @count_calls
    def f(x, y):
        return x * y

    assert builder.build().get('f') == 6
    assert f.times_called() == 1

    builder.delete('f')

    @builder
    @bn.persist
    @bn.version(1)
    @count_calls
    def f(x, y):
        return x ** y

    assert builder.build().get('f') == 6
    assert f.times_called() == 0

    builder.delete('f')

    @builder
    @bn.persist
    @bn.version(2)
    @count_calls
    def f(x, y):
        return x ** y

    assert builder.build().get('f') == 8
    assert f.times_called() == 1


def test_all_returned_results_are_deserialized(builder):
    class RoundingProtocol(bn.protocols.BaseProtocol):
        file_suffix = '.round'

        def write(self, value, file_):
            file_.write(str(round(value)))

        def read(self, file_):
            return float(file_.read())

    @builder
    @bn.persist
    @RoundingProtocol()
    @count_calls
    def pi():
        return math.pi

    assert builder.build().get('pi') == 3
    assert builder.build().get('pi') == 3
    assert builder.build().get('pi') != math.pi
    assert pi.times_called() == 1


def test_gather_cache_invalidation(builder):
    builder.assign('x', values=[1, 2])
    builder.assign('y', values=[2, 3])

    @builder
    @bn.persist
    @bn.gather('x', 'x', 'df')
    @count_calls
    def z(df, y):
        return df['x'].sum() + y

    assert builder.build().get('z', set) == {5, 6}
    assert z.times_called() == 2
    assert builder.build().get('z', set) == {5, 6}
    assert z.times_called() == 0

    builder.set('y', values=[3, 4])

    assert builder.build().get('z', set) == {6, 7}
    assert z.times_called() == 1
