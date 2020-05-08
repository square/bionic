import pytest

import math

from ..helpers import RoundingProtocol, count_calls
from bionic.exception import AttributeValidationError, CodeVersioningError

import bionic as bn


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
@pytest.mark.no_parallel
def test_caching_and_invalidation(builder, make_counter):
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
    assert yz_counter.times_called() == 1

    assert flow.get("xy_plus_yz") == 18
    assert flow.get("xy_plus_yz") == 18
    assert xy_plus_yz_counter.times_called() == 1

    # Rebuild the flow (resetting the in-memory cache) and confirm that
    # xy and xy_plus_yz are still cached.
    flow = builder.build()

    assert flow.get("xy") == 6
    assert xy_counter.times_called() == 0

    assert flow.get("yz") == 12
    # Note yz is not cached.
    assert yz_counter.times_called() == 1

    assert flow.get("xy_plus_yz") == 18
    assert xy_plus_yz_counter.times_called() == 0

    # Change the value of z, and confirm that yz and xy_plus_yz are recomputed.
    flow = flow.setting("z", -4)

    assert flow.get("xy") == 6
    assert xy_counter.times_called() == 0

    assert flow.get("yz") == -12
    assert yz_counter.times_called() == 1

    assert flow.get("xy_plus_yz") == -6
    assert flow.get("xy_plus_yz") == -6
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
    assert xy_plus_yz_counter.times_called() == 2

    # This is mainly just to check that the cache wrapper returns a sane set of
    # case keys.
    key_names = flow.get("xy_plus_yz", "series").index.names
    for name in ["x", "y"]:
        assert name in key_names


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

    @builder  # noqa: F811
    def f(x, y):
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=2)
    def f(x, y):
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
    def y():
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 5
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1)
    def y():
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version(1)
    def y():
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1, minor=1)
    def y():
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
    def f(x, y):
        call_counter.mark()
        return x * y

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):
        call_counter.mark()
        return y * x

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):
        call_counter.mark()
        return x ** y

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=2)
    def f(x, y):
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
    def y():
        y_call_counter.mark()
        return 4

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version(1)
    def y():
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version(1)
    def y():
        y_call_counter.mark()
        return len("xxxx")

    with pytest.raises(CodeVersioningError):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version(1, minor=1)
    def y():
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
    def f(x, y):
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(1)
    def f(x, y):
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def f(x, y):
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 8
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version(major=2)
    def f(x, y):
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
    def y():
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version(1)
    def y():
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    # f uses the cached value since final values
    # of x and y are still the same
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1)
    def y():
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    # f uses the cached value since final values
    # of x and y are still the same
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(1, minor=1)
    def y():
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


def test_disable_memory_caching(builder):
    x_protocol = ReadCountingProtocol()

    @builder
    @x_protocol
    @bn.memoize(False)
    def x():
        return 1

    flow = builder.build()
    assert flow.get("x") == 1
    assert flow.get("x") == 1
    assert x_protocol.times_read_called == 2

    with pytest.raises(ValueError):

        @builder
        @x_protocol
        @bn.persist(False)
        @bn.memoize(False)
        def y():
            return 1

        flow = builder.build()
        assert flow.get("y") == 1


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


@pytest.mark.no_parallel
def test_changes_per_run_and_not_persist(builder, make_counter):
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
    assert flow.get("x_plus_four") == 9
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


@pytest.mark.no_parallel
def test_changes_per_run_and_persist(builder, make_counter):
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
    # Since the value does not persist, x_plus_two is recomputed.
    assert x_plus_two_counter.times_called() == 1
    # Since value of x_plus_one didn't change even though it changes per run,
    # x_plus_three is not computed.
    assert x_plus_three_counter.times_called() == 0


def test_changes_per_run_and_not_memoize(builder):
    builder.assign("x", 5)

    @builder
    @bn.memoize(False)
    @bn.changes_per_run
    def x_plus_one(x):
        return x + 1

    with pytest.raises(AttributeValidationError):
        builder.build().get("x_plus_one")
