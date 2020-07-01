import pytest

import math
import threading

from ..helpers import RoundingProtocol, count_calls
from bionic.exception import AttributeValidationError, CodeVersioningError
from bionic.protocols import PicklableProtocol

import bionic as bn


# This is detected by pytest and applied to all the tests in this module.
pytestmark = pytest.mark.run_with_all_execution_modes_by_default


class ReadCountingProtocol(bn.protocols.PicklableProtocol):
    def __init__(self):
        self.times_read_called = 0
        super(ReadCountingProtocol, self).__init__()

    def read(self, path):
        self.times_read_called += 1
        return super(ReadCountingProtocol, self).read(path)


# It would be nice to move the builder setup into fixtures, but since we need
# to access the bound functions as well (to check the number of times they were
# called), it's easiest to just have one long test.
def test_caching_and_invalidation(builder, make_counter, parallel_execution_enabled):
    # Set up the builder with singleton values.

    builder.assign("x", 2)
    builder.assign("y", 3)
    builder.assign("z", 4)

    xy_counter = make_counter()

    @builder
    @count_calls(xy_counter)
    def xy(x, y):
        return x * y

    yz_counter = make_counter()

    @builder
    @bn.persist(False)
    @count_calls(yz_counter)
    def yz(y, z):
        return y * z

    xy_plus_yz_counter = make_counter()

    @builder
    @count_calls(xy_plus_yz_counter)
    def xy_plus_yz(xy, yz):
        return xy + yz

    # Access the downstream values.
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.get("xy") == 6
    assert xy_counter.times_called() == 1

    assert flow.get("yz") == 12
    assert flow.get("yz") == 12
    assert xy_counter.times_called() == 0
    assert yz_counter.times_called() == 1

    assert flow.get("xy_plus_yz") == 18
    assert flow.get("xy_plus_yz") == 18
    assert xy_counter.times_called() == 0
    if parallel_execution_enabled:
        # This is different from serial execution because we don't pass
        # in-memory cache to the subprocesses. The subprocess computes
        # non-persisted entities instead.
        assert yz_counter.times_called() == 1
    else:
        assert yz_counter.times_called() == 0
    assert xy_plus_yz_counter.times_called() == 1

    # Rebuild the flow (resetting the in-memory cache) and confirm that
    # xy and xy_plus_yz are still cached.
    flow = builder.build()

    assert flow.get("xy") == 6
    assert xy_counter.times_called() == 0

    assert flow.get("yz") == 12
    assert xy_counter.times_called() == 0
    # Note yz is not cached.
    assert yz_counter.times_called() == 1

    assert flow.get("xy_plus_yz") == 18
    assert xy_counter.times_called() == 0
    assert yz_counter.times_called() == 0
    assert xy_plus_yz_counter.times_called() == 0

    # Change the value of z, and confirm that yz and xy_plus_yz are recomputed.
    flow = flow.setting("z", -4)

    assert flow.get("xy") == 6
    assert xy_counter.times_called() == 0

    assert flow.get("yz") == -12
    assert xy_counter.times_called() == 0
    assert yz_counter.times_called() == 1

    assert flow.get("xy_plus_yz") == -6
    assert flow.get("xy_plus_yz") == -6
    assert xy_counter.times_called() == 0
    if parallel_execution_enabled:
        assert yz_counter.times_called() == 1
    else:
        assert yz_counter.times_called() == 0
    assert xy_plus_yz_counter.times_called() == 1

    # Update x and y to have multiple values, and confirm that xy and
    # xy_plus_yz are recomputed.
    flow = builder.build().setting("x", values=[2, -2]).setting("y", values=[3, 6])

    assert flow.get("xy", set) == {-2 * 6, -2 * 3, 2 * 3, 2 * 6}  # noqa: E226
    # Note that we only call xy 3 times, because one value was already cached.
    assert xy_counter.times_called() == 3

    assert flow.get("yz", set) == {3 * 4, 6 * 4}  # noqa: E226
    assert yz_counter.times_called() == 2

    assert flow.get("xy_plus_yz", set) == {
        -2 * 3 + 3 * 4,
        -2 * 6 + 6 * 4,
        2 * 3 + 3 * 4,
        2 * 6 + 6 * 4,
    }  # noqa: E226
    assert xy_counter.times_called() == 0
    if parallel_execution_enabled:
        assert yz_counter.times_called() == 3
    else:
        assert yz_counter.times_called() == 0
    assert xy_plus_yz_counter.times_called() == 3

    flow = builder.build().setting("x", values=[2, -2]).setting("y", values=[3, 6])

    assert flow.get("xy", set) == {-12, -6, 6, 12}
    assert xy_counter.times_called() == 0

    assert flow.get("yz", set) == {3 * 4, 6 * 4}  # noqa: E226
    assert yz_counter.times_called() == 2

    assert flow.get("xy_plus_yz", set) == {
        -2 * 3 + 3 * 4,
        -2 * 6 + 6 * 4,
        2 * 3 + 3 * 4,
        2 * 6 + 6 * 4,
    }  # noqa: E226
    assert xy_counter.times_called() == 0
    assert yz_counter.times_called() == 0
    assert xy_plus_yz_counter.times_called() == 0

    # Update y to have a different, overlapped set of values, and check that
    # the minimal set of recomputations are performed.

    flow = flow.setting("y", values=[6, 9])

    assert flow.get("xy", set) == {-2 * 6, -2 * 9, 2 * 6, 2 * 9}  # noqa: E226
    assert xy_counter.times_called() == 2

    assert flow.get("yz", set) == {6 * 4, 9 * 4}  # noqa: E226
    assert yz_counter.times_called() == 2

    assert flow.get("xy_plus_yz", set) == {
        -2 * 6 + 6 * 4,
        -2 * 9 + 9 * 4,
        2 * 6 + 6 * 4,
        2 * 9 + 9 * 4,
    }  # noqa: E226
    assert xy_counter.times_called() == 0
    if parallel_execution_enabled:
        assert yz_counter.times_called() == 2
    else:
        assert yz_counter.times_called() == 0
    assert xy_plus_yz_counter.times_called() == 2

    # This is mainly just to check that the cache wrapper returns a sane set of
    # case keys.
    key_names = flow.get("xy_plus_yz", "series").index.names
    for name in ["x", "y"]:
        assert name in key_names


def test_user_values_persistence(builder):
    class WriteTenProtocol(PicklableProtocol):
        """Always writes 10 as the value"""

        def write(self, value, path):
            super(WriteTenProtocol, self).write(10, path)

    # Assign x an unpicklable value. The test should still pass as x will
    # use the protocol for serialization.
    builder.assign("x", threading.Lock(), protocol=WriteTenProtocol())

    @builder
    def y(x):
        return x

    assert builder.build().get("x") == 10
    assert builder.build().get("y") == 10


def test_versioning(builder, make_counter):
    call_counter = make_counter()

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    def f(x, y):
        call_counter.mark()
        return x + y

    assert builder.build().get("f") == 5
    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 1

    builder.delete("f")

    # flake8 behavior changed with python 3.8 version.
    # https://gitlab.com/pycqa/flake8/-/issues/583
    @builder  # noqa: F811
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=2)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 8
    assert call_counter.times_called() == 1


def test_indirect_versioning(builder, make_counter):
    y_call_counter = make_counter()
    f_call_counter = make_counter()

    builder.assign("x", 2)

    @builder
    def y():
        y_call_counter.mark()
        return 3

    @builder
    def f(x, y):
        f_call_counter.mark()
        return x + y

    assert builder.build().get("f") == 5
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 5
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1, minor=1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    builder.set("x", 5)

    assert builder.build().get("f") == 9
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 1

    builder.set("x", 2)

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0


def test_versioning_assist(builder, make_counter):
    call_counter = make_counter()

    builder.set("core__versioning_mode", "assist")

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    def f(x, y):
        call_counter.mark()
        return x + y

    assert builder.build().get("f") == 5
    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=2)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 8
    assert call_counter.times_called() == 1


def test_indirect_versioning_assist(builder, make_counter):
    y_call_counter = make_counter()
    f_call_counter = make_counter()

    builder.set("core__versioning_mode", "assist")

    builder.assign("x", 2)

    @builder
    def y():
        y_call_counter.mark()
        return 3

    @builder
    def f(x, y):
        f_call_counter.mark()
        return x + y

    assert builder.build().get("f") == 5
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version(1, minor=1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    builder.set("x", 5)

    assert builder.build().get("f") == 9
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 1

    builder.set("x", 2)

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0


def test_versioning_auto(builder, make_counter):
    call_counter = make_counter()

    builder.set("core__versioning_mode", "auto")

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    def f(x, y):
        call_counter.mark()
        return x + y

    assert builder.build().get("f") == 5
    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 8
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=2)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 8
    assert call_counter.times_called() == 1


def test_indirect_versioning_auto(builder, make_counter):
    y_call_counter = make_counter()
    f_call_counter = make_counter()

    builder.set("core__versioning_mode", "auto")

    builder.assign("x", 2)

    @builder
    def y():
        y_call_counter.mark()
        return 3

    @builder
    def f(x, y):
        f_call_counter.mark()
        return x + y

    assert builder.build().get("f") == 5
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    # f uses the cached value since final values
    # of x and y are still the same
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    # f uses the cached value since final values
    # of x and y are still the same
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1, minor=1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    builder.set("x", 5)

    assert builder.build().get("f") == 9
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 1

    builder.set("x", 2)

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0


def test_all_returned_results_are_deserialized(builder, make_counter):
    counter = make_counter()

    @builder
    @RoundingProtocol()
    @count_calls(counter)
    def pi():
        return math.pi

    assert builder.build().get("pi") == 3
    assert builder.build().get("pi") == 3
    assert builder.build().get("pi") != math.pi
    assert counter.times_called() == 1


def test_deps_of_cached_values_not_needed(builder):
    y_protocol = ReadCountingProtocol()
    z_protocol = ReadCountingProtocol()

    builder.assign("x", 2)

    @builder
    @y_protocol
    def y(x):
        return x + 1

    @builder
    @z_protocol
    def z(y):
        return y + 1

    flow = builder.build()
    assert flow.get("x") == 2
    assert flow.get("y") == 3
    assert flow.get("z") == 4

    assert flow.get("x") == 2
    assert flow.get("y") == 3
    assert flow.get("z") == 4

    assert y_protocol.times_read_called == 1
    assert z_protocol.times_read_called == 1

    flow = builder.build()
    assert flow.get("z") == 4

    assert y_protocol.times_read_called == 1
    assert z_protocol.times_read_called == 2


def test_deps_not_called_when_values_not_changed(builder, make_counter):
    builder.assign("x", 2)
    builder.assign("y", 3)
    builder.assign("z", 4)

    xy_counter = make_counter()

    @builder
    @count_calls(xy_counter)
    def xy(x, y):
        return x * y

    yz_counter = make_counter()

    @builder
    @count_calls(yz_counter)
    def yz(y, z):
        return y * z

    xy_plus_yz_counter = make_counter()

    @builder
    @count_calls(xy_plus_yz_counter)
    def xy_plus_yz(xy, yz):
        return xy + yz

    flow = builder.build()
    assert flow.get("xy") == 6
    assert flow.get("yz") == 12
    assert flow.get("xy_plus_yz") == 18

    assert xy_counter.times_called() == 1
    assert yz_counter.times_called() == 1
    assert xy_plus_yz_counter.times_called() == 1

    flow = flow.setting("x", 1).setting("y", 6).setting("z", 2)
    assert flow.get("xy") == 6
    assert flow.get("yz") == 12
    assert flow.get("xy_plus_yz") == 18

    # xy_plus_yz should not be called again
    assert xy_counter.times_called() == 1
    assert yz_counter.times_called() == 1
    assert xy_plus_yz_counter.times_called() == 0


def test_gather_cache_invalidation(builder, make_counter):
    builder.assign("x", values=[1, 2])
    builder.assign("y", values=[2, 3])

    counter = make_counter()

    @builder
    @bn.gather("x", "x", "df")
    @count_calls(counter)
    def z(df, y):
        return df["x"].sum() + y

    assert builder.build().get("z", set) == {5, 6}
    assert counter.times_called() == 2
    assert builder.build().get("z", set) == {5, 6}
    assert counter.times_called() == 0

    assert builder.build().setting("x", values=[2, 3]).get("z", set) == {7, 8}
    assert counter.times_called() == 2

    builder.set("y", values=[3, 4])

    assert builder.build().get("z", set) == {6, 7}
    assert counter.times_called() == 1


def test_gather_cache_invalidation_with_over_vars(builder, make_counter):
    builder.assign("x", values=[1, 2])
    builder.assign("y", values=[2, 3])

    counter = make_counter()

    @builder
    @bn.gather("x", "y", "df")
    @count_calls(counter)
    def z(df):
        return df.sum().sum()

    assert builder.build().get("z", set) == {7, 9}
    assert counter.times_called() == 2
    assert builder.build().get("z", set) == {7, 9}
    assert counter.times_called() == 0

    # If we change one of the values of `x`, both values of `z` should change
    # (because each instance depends on both values of `x`).
    assert builder.build().setting("x", values=[2, 3]).get("z", set) == {9, 11}
    assert counter.times_called() == 2

    # If we change one of the values of `y`, only one value of `z` should
    # change.
    assert builder.build().setting("y", values=[3, 4]).get("z", set) == {9, 11}
    assert counter.times_called() == 1


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y


def test_complex_input_type(builder, make_counter):
    builder.assign("point", Point(2, 3))

    @builder
    def x(point):
        return point.x

    @builder
    def y(point):
        return point.y

    counter = make_counter()

    @builder
    @count_calls(counter)
    def x_plus_y(x, y):
        return x + y

    flow = builder.build()

    assert flow.get("x_plus_y") == 5
    assert counter.times_called() == 1
    assert flow.get("x_plus_y") == 5
    assert counter.times_called() == 0

    builder = flow.to_builder()
    builder.set("point", values=(Point(2, 3), Point(4, 5)))
    flow = builder.build()

    assert flow.get("x_plus_y", set) == {5, 9}
    assert counter.times_called() == 1
    assert flow.get("x_plus_y", set) == {5, 9}
    assert counter.times_called() == 0


def test_persisting_none(builder, make_counter):
    counter = make_counter()

    @builder
    @count_calls(counter)
    def none():
        return None

    assert builder.build().get("none") is None
    assert builder.build().get("none") is None
    assert counter.times_called() == 1


def test_disable_memory_caching(builder, make_counter):
    protocol = ReadCountingProtocol()
    counter = make_counter()

    @builder
    @protocol
    @bn.memoize(False)
    @count_calls(counter)
    def x():
        return 1

    flow = builder.build()
    assert flow.get("x") == 1
    assert flow.get("x") == 1
    assert protocol.times_read_called == 2
    assert counter.times_called() == 1

    @builder  # noqa: F811
    @protocol
    @bn.persist(False)
    @bn.memoize(False)
    @count_calls(counter)
    def x():  # noqa: F811
        return 1

    flow = builder.build()
    assert flow.get("x") == 1
    assert flow.get("x") == 1
    assert protocol.times_read_called == 2
    assert counter.times_called() == 2


def test_disable_default_memory_caching(builder, make_counter):
    # Test that memoization by default is True.
    x_protocol = ReadCountingProtocol()
    x_counter = make_counter()

    @builder
    @x_protocol
    @count_calls(x_counter)
    def x():
        return 1

    flow = builder.build()
    assert flow.get("x") == 1
    assert flow.get("x") == 1
    # Flow uses memoized value for the second get call.
    assert x_protocol.times_read_called == 1
    assert x_counter.times_called() == 1

    # Test that disabling memoization works.
    builder.set("core__memoize_by_default", False)

    y_protocol = ReadCountingProtocol()
    y_counter = make_counter()

    @builder
    @y_protocol
    @count_calls(y_counter)
    def y():
        return 2

    flow = builder.build()
    assert flow.get("y") == 2
    assert flow.get("y") == 2
    # Flow reads from disk for each get call.
    assert y_protocol.times_read_called == 2
    assert y_counter.times_called() == 1

    # Test overriding the default memoization value.
    z_protocol = ReadCountingProtocol()
    z_counter = make_counter()

    @builder
    @z_protocol
    @bn.memoize(True)
    @count_calls(z_counter)
    def z():
        return 3

    flow = builder.build()
    assert flow.get("z") == 3
    assert flow.get("z") == 3
    # Flow uses memoized value for the second get call.
    assert z_protocol.times_read_called == 1
    assert z_counter.times_called() == 1


@pytest.mark.parametrize("decorator", [bn.persist, bn.memoize])
@pytest.mark.parametrize("enabled1", [True, False])
@pytest.mark.parametrize("enabled2", [True, False])
def test_redundant_decorators(builder, decorator, enabled1, enabled2):
    with pytest.raises(AttributeValidationError):

        @builder
        @decorator(enabled1)
        @decorator(enabled2)
        def fail():
            pass


def test_unset_and_not_memoized(builder):
    builder.declare("x")

    @builder
    @bn.memoize(False)
    def x_plus_one(x):
        return x + 1

    assert builder.build().get("x_plus_one", list) == []


def test_unset_and_not_persisted(builder):
    builder.declare("x")

    @builder
    @bn.persist(False)
    def x_plus_one(x):
        return x + 1

    assert builder.build().get("x_plus_one", list) == []


def test_changes_per_run_and_not_persist(
    builder, make_counter, parallel_execution_enabled
):
    builder.assign("x", 5)

    x_plus_one_counter = make_counter()

    @builder
    @bn.persist(False)
    @bn.changes_per_run
    @count_calls(x_plus_one_counter)
    def x_plus_one(x):
        return x + 1

    x_plus_two_counter = make_counter()

    @builder
    @bn.persist(False)
    @count_calls(x_plus_two_counter)
    def x_plus_two(x_plus_one):
        return x_plus_one + 1

    x_plus_three_counter = make_counter()

    @builder
    @count_calls(x_plus_three_counter)
    def x_plus_three(x_plus_two):
        return x_plus_two + 1

    x_plus_four_counter = make_counter()

    @builder
    @count_calls(x_plus_four_counter)
    def x_plus_four(x_plus_three):
        return x_plus_three + 1

    flow = builder.build()
    assert flow.get("x_plus_one") == 6
    assert flow.get("x_plus_four") == 9
    if parallel_execution_enabled:
        # This is different from serial execution because we don't pass
        # in-memory cache to the subprocesses. The subprocess computes
        # non-persisted entities instead.
        assert x_plus_one_counter.times_called() == 2
    else:
        assert x_plus_one_counter.times_called() == 1
    assert x_plus_two_counter.times_called() == 1
    assert x_plus_three_counter.times_called() == 1
    assert x_plus_four_counter.times_called() == 1

    # In the same flow, a nondeterministic entity is not recomputed.
    assert flow.get("x_plus_one") == 6
    assert flow.get("x_plus_four") == 9
    assert x_plus_one_counter.times_called() == 0
    assert x_plus_two_counter.times_called() == 0
    assert x_plus_three_counter.times_called() == 0
    assert x_plus_four_counter.times_called() == 0

    flow = builder.build()
    assert flow.get("x_plus_four") == 9
    # x_plus_one changes per run and should be recomputed between runs.
    assert x_plus_one_counter.times_called() == 1
    # x_plus_two is a child of a nondeterministic parent which does not persist
    # and should also be recomputed between runs.
    assert x_plus_two_counter.times_called() == 1
    # x_plus_three is a child of a parent that has a nondeterministic parent
    # and is not persisted. Hence the provenance also contains the
    # noise from nondeterministic ancestor, cannot use the persisted value and
    # should also be recomputed.
    assert x_plus_three_counter.times_called() == 1
    # x_plus_four uses the value hash of its parent which didn't change.
    # Should not be recomputed and should use cached value instead.
    assert x_plus_four_counter.times_called() == 0


def test_changes_per_run_and_persist(builder, make_counter, parallel_execution_enabled):
    builder.assign("x", 5)

    x_plus_one_counter = make_counter()

    @builder
    @bn.changes_per_run
    @count_calls(x_plus_one_counter)
    def x_plus_one(x):
        return x + 1

    x_plus_two_counter = make_counter()

    @builder
    @bn.persist(False)
    @count_calls(x_plus_two_counter)
    def x_plus_two(x_plus_one):
        return x_plus_one + 1

    x_plus_three_counter = make_counter()

    @builder
    @count_calls(x_plus_three_counter)
    def x_plus_three(x_plus_two):
        return x_plus_two + 1

    flow = builder.build()
    assert flow.get("x_plus_one") == 6
    assert flow.get("x_plus_three") == 8
    assert x_plus_one_counter.times_called() == 1
    assert x_plus_two_counter.times_called() == 1
    assert x_plus_three_counter.times_called() == 1
    # In the same flow, a nondeterministic entity is not recomputed.
    assert flow.get("x_plus_one") == 6
    assert flow.get("x_plus_three") == 8
    assert x_plus_one_counter.times_called() == 0
    assert x_plus_two_counter.times_called() == 0
    assert x_plus_three_counter.times_called() == 0

    flow = builder.build()
    assert flow.get("x_plus_three") == 8
    # x_plus_one changes per run and should be recomputed between runs.
    assert x_plus_one_counter.times_called() == 1
    if parallel_execution_enabled:
        # When executed in parallel, we don't compute every entity in
        # the subprocess. Subprocess computes a non-persisted entity
        # only if it is required to compute any other entity.
        assert x_plus_two_counter.times_called() == 0
    else:
        # Since the value does not persist, x_plus_two is recomputed.
        assert x_plus_two_counter.times_called() == 1
    # Since value of x_plus_one didn't change even though it changes per run,
    # x_plus_three is not computed.
    assert x_plus_three_counter.times_called() == 0


def test_changes_per_run_and_not_memoize(builder, make_counter):
    counter = make_counter()
    builder.assign("x", 5)

    @builder
    @bn.memoize(False)
    @bn.persist(False)
    @bn.changes_per_run
    @count_calls(counter)
    def x_plus_one(x):
        return x + 1

    with pytest.warns(
        UserWarning,
        match="aren't configured to be memoized but are decorated with @changes_per_run",
    ):
        flow = builder.build()
        assert flow.get("x_plus_one") == 6
        assert flow.get("x_plus_one") == 6
        # Recompute once as the value would still be memoized.
        assert counter.times_called() == 1

    with pytest.warns(
        UserWarning,
        match="aren't configured to be memoized but are decorated with @changes_per_run",
    ):
        flow = builder.build()
        assert flow.get("x_plus_one") == 6
        # Recomputes between flows since the value is memoized and not persisted.
        assert counter.times_called() == 1


def test_updating_cache_works_only_with_immediate(builder):
    persistent_cache = builder.build().get("core__persistent_cache")

    @builder
    @bn.immediate
    def core__persistent_cache():
        return persistent_cache

    @builder
    def x():
        return 1

    assert builder.build().get("x") == 1

    # This should fail because user is trying to persist an internal
    # entity which is not allowed.
    with pytest.raises(AttributeValidationError):

        @builder  # noqa: F811
        @bn.persist(True)
        def core__persistent_cache():  # noqa: F811
            return persistent_cache

    @builder  # noqa: F811
    @bn.immediate
    def core__persistent_cache(x):  # noqa: F811
        return persistent_cache

    # Even though `x` is used by an internal entity, we will allow this
    # by not persisting x.
    assert builder.build().get("x") == 1


def test_multiple_outputs_all_persisted_at_once(builder, make_counter):
    call_counter = make_counter()

    @builder
    @bn.outputs("x", "y")
    @count_calls(call_counter)
    def x_y():
        return 1, 2

    assert builder.build().get("x") == 1
    assert builder.build().get("y") == 2
    assert call_counter.times_called() == 1
