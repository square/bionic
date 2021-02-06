import pytest

import math
from textwrap import dedent
import threading

from ..helpers import RoundingProtocol, import_code
from bionic.exception import AttributeValidationError, CodeVersioningError
from bionic.protocols import PicklableProtocol

import bionic as bn


# This is detected by pytest and applied to all the tests in this module.
pytestmark = pytest.mark.allows_parallel


class ReadCountingProtocol(bn.protocols.PicklableProtocol):
    def __init__(self):
        self.times_read_called = 0
        super(ReadCountingProtocol, self).__init__()

    def read(self, path):
        self.times_read_called += 1
        return super(ReadCountingProtocol, self).read(path)


def raises_versioning_error_for_entity(entity_name):
    return pytest.raises(
        CodeVersioningError,
        match=f".*function that outputs <{entity_name}>.*",
    )


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
    @xy_counter
    def xy(x, y):
        return x * y

    yz_counter = make_counter()

    @builder
    @bn.persist(False)
    @yz_counter
    def yz(y, z):
        return y * z

    xy_plus_yz_counter = make_counter()

    @builder
    @xy_plus_yz_counter
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
    @bn.version_no_warnings(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version_no_warnings(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=2)
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
    @bn.version_no_warnings(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 0
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version_no_warnings(1, minor=1)
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

    with raises_versioning_error_for_entity("f"):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    with raises_versioning_error_for_entity("f"):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version_no_warnings(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    with raises_versioning_error_for_entity("f"):
        builder.build().get("f")

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=2)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 8
    assert call_counter.times_called() == 1


def test_versioning_assist_with_refs(builder, make_counter):
    call_counter = make_counter()

    builder.set("core__versioning_mode", "assist")

    builder.assign("x", 2)
    builder.assign("y", 3)

    def op(x, y):
        return x + y

    @builder
    def f(x, y):
        call_counter.mark()
        return op(x, y)

    assert builder.build().get("f") == 5
    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 1

    def op(x, y):  # noqa: F811
        return x * y

    with raises_versioning_error_for_entity("f"):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return op(x, y)

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    def op(x, y):  # noqa: F811
        return y * x

    with raises_versioning_error_for_entity("f"):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return op(x, y)

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    def op(x, y):  # noqa: F811
        return x ** y

    with raises_versioning_error_for_entity("f"):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=2)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return op(x, y)

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

    with raises_versioning_error_for_entity("y"):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    with raises_versioning_error_for_entity("y"):
        builder.build().get("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(1, minor=1)
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


def test_indirect_nonpersisted_versioning_assist(builder, make_counter):
    y_call_counter = make_counter()
    f_call_counter = make_counter()

    builder.set("core__versioning_mode", "assist")

    builder.assign("x", 2)

    @builder
    @bn.persist(False)
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
    @bn.persist(False)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    with raises_versioning_error_for_entity("y"):
        builder.build().get("f")


def test_indirect_nonpersisted_versioning_auto(builder, make_counter):
    y_call_counter = make_counter()
    f_call_counter = make_counter()

    builder.set("core__versioning_mode", "auto")

    builder.assign("x", 2)

    @builder
    @bn.persist(False)
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
    @bn.persist(False)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    assert f_call_counter.times_called() == 1


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
    @bn.version_no_warnings(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x * y

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version_no_warnings(major=1, minor=1)
    def f(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    assert builder.build().get("f") == 8
    assert call_counter.times_called() == 1

    builder.delete("f")

    @builder  # noqa: F811
    @bn.version_no_warnings(major=2)
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
    @bn.version_no_warnings(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return 4

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    # f uses the cached value since final values
    # of x and y are still the same
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version_no_warnings(1)
    def y():  # noqa: F811
        y_call_counter.mark()
        return len("xxxx")

    assert builder.build().get("f") == 6
    assert y_call_counter.times_called() == 1
    # f uses the cached value since final values
    # of x and y are still the same
    assert f_call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version_no_warnings(1, minor=1)
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


def test_versioning_auto_class_changes(builder, make_counter):
    call_counter = make_counter()

    # PicklableProtocol cannot pickle IntWrapper class because it is
    # defined inside the test function. We instead use this custom
    # protocol that wraps the PicklableProtocol to be able to serialize
    # IntWrapper.
    class IntWrapperProtocol(bn.protocols.PicklableProtocol):
        def write(self, wrapper, path):
            assert isinstance(wrapper, IntWrapper)
            super().write(wrapper.value, path)

        def read(self, path):
            return IntWrapper(super().read(path))

    class IntWrapper:
        def __init__(self, value):
            self.value = value

        def __str__(self):
            return f"IntWrapper: value={self.value}"

    builder.set("core__versioning_mode", "auto")

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    @IntWrapperProtocol()
    def wrapped_f(x, y):
        return IntWrapper(x + y)

    @builder
    def f(wrapped_f):
        call_counter.mark()
        return wrapped_f.value

    assert builder.build().get("f") == 5
    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 1

    builder.delete("wrapped_f")

    @builder  # noqa: F811
    @IntWrapperProtocol()
    def wrapped_f(x, y):  # noqa: F811
        return IntWrapper(x * y)

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1

    builder.delete("wrapped_f")

    # No changes in the wrapper class.
    class IntWrapper:  # noqa: F811
        def __init__(self, value):
            self.value = value

        def __str__(self):
            return f"IntWrapper: value={self.value}"

    @builder  # noqa: F811
    @IntWrapperProtocol()
    def wrapped_f(x, y):  # noqa: F811
        return IntWrapper(x * y)

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 0

    builder.delete("wrapped_f")

    # Make a change in the __str__ method.
    class IntWrapper:  # noqa: F811
        def __init__(self, value):
            self.value = value

        def __str__(self):
            return f"value={self.value}"

    @builder  # noqa: F811
    @IntWrapperProtocol()
    def wrapped_f(x, y):  # noqa: F811
        return IntWrapper(x * y)

    assert builder.build().get("f") == 6
    assert call_counter.times_called() == 1


@pytest.mark.parametrize("versioning_mode", ["manual", "assist"])
def test_versioning_class_changes(builder, make_counter, versioning_mode):
    call_counter = make_counter()

    # PicklableProtocol cannot pickle IntWrapper class because it is
    # defined inside the test function. We instead use this custom
    # protocol that wraps the PicklableProtocol to be able to serialize
    # IntWrapper.
    class IntWrapperProtocol(bn.protocols.PicklableProtocol):
        def write(self, wrapper, path):
            assert isinstance(wrapper, IntWrapper)
            super().write(wrapper.value, path)

        def read(self, path):
            return IntWrapper(super().read(path))

    class IntWrapper:
        def __init__(self, value):
            self.value = value

        def __str__(self):
            return f"IntWrapper: value={self.value}"

    builder.set("core__versioning_mode", versioning_mode)

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    @IntWrapperProtocol()
    @bn.version(1)
    def wrapped_f(x, y):
        return IntWrapper(x + y)

    @builder
    def f(wrapped_f):
        call_counter.mark()
        return wrapped_f.value

    assert builder.build().get("f") == 5
    assert builder.build().get("f") == 5
    assert call_counter.times_called() == 1

    builder.delete("wrapped_f")

    # Make a change in the __str__ method.
    class IntWrapper:  # noqa: F811
        def __init__(self, value):
            self.value = value

        def __str__(self):
            return f"value={self.value}"

    @builder  # noqa: F811
    @IntWrapperProtocol()
    @bn.version(1)
    def wrapped_f(x, y):  # noqa: F811
        return IntWrapper(x + y)

    if versioning_mode == "assist":
        with raises_versioning_error_for_entity("wrapped_f"):
            builder.build().get("f")
    else:
        assert builder.build().get("f") == 5
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @IntWrapperProtocol()
    @bn.version(2)
    def wrapped_f(x, y):  # noqa: F811
        return IntWrapper(x + y)

    assert builder.build().get("f") == 5
    # We don't hash the class of the inputs for manual and assist
    # versioning modes. So even if the class for `wrapped_f` changes,
    # if the hash of the `wrapped_f.value` doesn't changes, `f` won't
    # be recomputed.
    assert call_counter.times_called() == 0


def test_versioning_auto_with_local_refs(builder, make_counter):
    call_counter = make_counter()

    builder.set("core__versioning_mode", "auto")

    ret_val = 1

    def ref_func():
        return ret_val

    @builder
    @call_counter
    def x():
        return ref_func()

    assert builder.build().get("x") == 1
    assert call_counter.times_called() == 1

    # With no changes, x is not called again.
    assert builder.build().get("x") == 1
    assert call_counter.times_called() == 0

    # If we change the ref_func, the bytecode of x changes.
    def ref_func():  # noqa: F811
        return ret_val + 7

    assert builder.build().get("x") == 8
    assert call_counter.times_called() == 1

    # If we change the ret_val, the bytecode of x changes.
    ret_val = 5
    assert builder.build().get("x") == 12
    assert call_counter.times_called() == 1


def test_versioning_auto_with_module_refs(builder, make_counter):
    call_counter = make_counter()

    builder.set("core__versioning_mode", "auto")

    mod_code = """
    ret_val = 1

    def ref_func():
        return ret_val
    """
    m = import_code(dedent(mod_code))

    @builder
    @call_counter
    def x():
        return m.ref_func()

    assert builder.build().get("x") == 1
    assert call_counter.times_called() == 1

    # With no changes, x is not called again.
    m = import_code(dedent(mod_code))
    assert builder.build().get("x") == 1
    assert call_counter.times_called() == 0

    # If we change the ref_func, the bytecode of x changes.
    mod_code = """
    ret_val = 1

    def ref_func():
        return ret_val + 7
    """
    m = import_code(dedent(mod_code))
    assert builder.build().get("x") == 8
    assert call_counter.times_called() == 1

    # If we change the ret_val, the bytecode of x changes.
    mod_code = """
    ret_val = 5

    def ref_func():
        return ret_val + 7
    """
    m = import_code(dedent(mod_code))
    assert builder.build().get("x") == 12
    assert call_counter.times_called() == 1


def test_versioning_auto_version_options(builder):
    builder.set("core__versioning_mode", "auto")

    import pandas as pd

    series = pd.Series([1, 2, 3])

    @builder
    def x():
        assert series is not None
        return 1

    with pytest.warns(None) as warnings:
        assert builder.build().get("x") == 1
    assert len(warnings) == 0

    # With suppress_bytecode_warnings as False, bytecode analysis
    # throws a warning.
    @builder  # noqa: F811
    @bn.version(major=2)
    def x():  # noqa: F811
        assert series is not None
        return 1

    with pytest.warns(UserWarning, match="Found a complex object"):
        assert builder.build().get("x") == 1

    # With ignore_bytecode=True, we won't analyze bytecode and won't
    # throw a warning.
    @builder  # noqa: F811
    @bn.version(major=2, ignore_bytecode=True)
    def x():  # noqa: F811
        assert series is not None
        return 2

    with pytest.warns(None) as warnings:
        # It's a little awkward that we compute the value again even
        # though we ignore the bytecode. But that's because the
        # bytecode is now null which changes the version.
        assert builder.build().get("x") == 2
    assert len(warnings) == 0

    @builder  # noqa: F811
    @bn.version(major=2, ignore_bytecode=True)
    def x():  # noqa: F811
        assert series is not None
        return 3

    with pytest.warns(None) as warnings:
        # Even with the change in bytecode, it uses the cached value.
        assert builder.build().get("x") == 2
    assert len(warnings) == 0


def test_all_returned_results_are_deserialized(builder, make_counter):
    counter = make_counter()

    @builder
    @RoundingProtocol()
    @counter
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
    @xy_counter
    def xy(x, y):
        return x * y

    yz_counter = make_counter()

    @builder
    @yz_counter
    def yz(y, z):
        return y * z

    xy_plus_yz_counter = make_counter()

    @builder
    @xy_plus_yz_counter
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
    @counter
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
    @counter
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
    @counter
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
    @counter
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
    @counter
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
    @counter
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
    @x_counter
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
    @y_counter
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
    @z_counter
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
    @x_plus_one_counter
    def x_plus_one(x):
        return x + 1

    x_plus_two_counter = make_counter()

    @builder
    @bn.persist(False)
    @x_plus_two_counter
    def x_plus_two(x_plus_one):
        return x_plus_one + 1

    x_plus_three_counter = make_counter()

    @builder
    @x_plus_three_counter
    def x_plus_three(x_plus_two):
        return x_plus_two + 1

    x_plus_four_counter = make_counter()

    @builder
    @x_plus_four_counter
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


def test_changes_per_run_and_persist(builder, make_counter):
    builder.assign("x", 5)

    x_plus_one_counter = make_counter()

    @builder
    @bn.changes_per_run
    @x_plus_one_counter
    def x_plus_one(x):
        return x + 1

    x_plus_two_counter = make_counter()

    @builder
    @bn.persist(False)
    @x_plus_two_counter
    def x_plus_two(x_plus_one):
        return x_plus_one + 1

    x_plus_three_counter = make_counter()

    @builder
    @x_plus_three_counter
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
    # x_plus_one changes per run, so it needs to be recomputed for this new flow
    # instance.
    assert x_plus_one_counter.times_called() == 1
    # The other entities haven't changed and shouldn't need to be recomputed.
    assert x_plus_two_counter.times_called() == 0
    assert x_plus_three_counter.times_called() == 0


def test_changes_per_run_and_not_memoize(builder, make_counter):
    counter = make_counter()
    builder.assign("x", 5)

    @builder
    @bn.memoize(False)
    @bn.persist(False)
    @bn.changes_per_run
    @counter
    def x_plus_one(x):
        return x + 1

    with pytest.warns(
        UserWarning,
        match="isn't configured to be memoized but is decorated with @changes_per_run",
    ):
        flow = builder.build()
        assert flow.get("x_plus_one") == 6
        assert flow.get("x_plus_one") == 6
        # Recompute once as the value would still be memoized.
        assert counter.times_called() == 1

    with pytest.warns(
        UserWarning,
        match="isn't configured to be memoized but is decorated with @changes_per_run",
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
    counter = make_counter()

    @builder
    @bn.outputs("x", "y")
    @counter
    def x_y():
        return 1, 2

    assert builder.build().get("x") == 1
    assert builder.build().get("y") == 2
    assert counter.times_called() == 1


def test_complex_tuple_outputs_all_persisted_at_once(builder, make_counter):
    counter = make_counter()

    @builder
    @bn.returns("w, ((x, y), z)")
    @counter
    def w_x_y_z():
        return (1, ((2, 3), 4))

    assert builder.build().get("w") == 1
    assert builder.build().get("x") == 2
    assert builder.build().get("y") == 3
    assert builder.build().get("z") == 4
    assert counter.times_called() == 1


def test_complex_flow_outputs_all_persisted_at_once(builder, make_counter):
    builder.assign("a_minus_one", values=[2, 3, 4])

    a_counter = make_counter()
    ax_counter = make_counter()
    a_something_x_counter = make_counter()

    @builder
    @a_counter
    def a(a_minus_one):
        return a_minus_one + 1

    @builder
    @bn.persist(False)
    @ax_counter
    def x():
        return 2

    @builder
    @bn.returns("(a_plus_x, a_minus_x), a_times_x")
    @a_something_x_counter
    def a_something_x(a, x):
        return ((a + x), (a - x)), (a * x)

    assert builder.build().get("a_plus_x", collection=set) == {5, 6, 7}
    assert builder.build().get("a_minus_x", collection=set) == {1, 2, 3}
    assert builder.build().get("a_times_x", collection=set) == {6, 8, 10}
    assert a_counter.times_called() == 3
    assert a_something_x_counter.times_called() == 3


def test_avoid_recomputing_nonpersisted_dep(builder, make_counter):
    a_counter = make_counter()

    @builder
    @bn.persist(False)
    @a_counter
    def a():
        return 1

    @builder
    def b(a):
        return a + 1

    assert builder.build().get("b") == 2
    assert builder.build().get("b") == 2
    assert a_counter.times_called() == 1


def test_caching_dir_with_whitespaces(builder, make_counter, tmp_path):
    builder.set("core__persistent_cache__flow_dir", str(tmp_path / "BN TEST DATA"))
    counter = make_counter()

    @builder
    @counter
    def one():
        return 1

    assert builder.build().get("one") == 1
    assert builder.build().get("one") == 1
    assert counter.times_called() == 1


@pytest.fixture
def make_tracked_class():
    """
    Creates a "tracked" class which keeps a count of how many of its instances are in
    memory.

    The class also asserts that each instance in memory is has a unique value; this
    guarantees that we don't (for example) read an instance from disk while the
    original instance is still in memory.

    """

    def _make_tracked_class():
        # The Tracked class we're about to make is not picklable (since it's nested in
        # this function), so we'll also need a custom protocol for it.
        class TrackedProtocol(bn.protocols.BaseProtocol):
            def get_fixed_file_extension(self):
                return "tracked"

            def write(self, value, path):
                path.write_text(str(value.value))

            def read(self, path):
                return Tracked(int(path.read_text()))

        class Tracked:
            n_instances_in_memory = 0
            instance_values_in_memory = set()
            protocol = TrackedProtocol()

            def __init__(self, value):
                assert value not in self.__class__.instance_values_in_memory

                self.__class__.instance_values_in_memory.add(value)
                self.__class__.n_instances_in_memory += 1
                self.value = value

            def __del__(self):
                # If our assertion in __init__ failed, we don't have a "value"
                # attribute and don't need to update our trackers.
                if hasattr(self, "value"):
                    self.__class__.n_instances_in_memory -= 1
                    self.__class__.instance_values_in_memory.remove(self.value)

        return Tracked

    return _make_tracked_class


def test_non_memoized_value_is_garbage_collected(builder, make_tracked_class):
    Tracked = make_tracked_class()

    @builder
    @bn.memoize(False)
    @Tracked.protocol
    def tracked_x():
        return Tracked(1)

    @builder
    def x_plus_one(tracked_x):
        assert Tracked.n_instances_in_memory == 1
        return tracked_x.value + 1

    @builder
    def x_plus_two(x_plus_one):
        assert Tracked.n_instances_in_memory == 0
        return x_plus_one + 1

    assert builder.build().get("x_plus_two") == 3


def test_non_memoized_values_are_garbage_collected(builder, make_tracked_class):
    Tracked = make_tracked_class()

    @builder
    @bn.memoize(False)
    @Tracked.protocol
    @bn.outputs("tracked_x", "tracked_y")
    def _():
        return Tracked(1), Tracked(-1)

    @builder
    def x_plus_one(tracked_x):
        assert Tracked.n_instances_in_memory == 1
        return tracked_x.value + 1

    @builder
    def x_plus_two(x_plus_one):
        assert Tracked.n_instances_in_memory == 0
        return x_plus_one + 1

    assert builder.build().get("x_plus_two") == 3


def test_non_memoized_complex_tuples_are_garbage_collected(builder, make_tracked_class):
    Tracked = make_tracked_class()

    @builder
    @bn.memoize(False)
    @Tracked.protocol
    @bn.returns("(tracked_x, tracked_y), tracked_z")
    def _():
        return (Tracked(1), Tracked(-1)), Tracked(0)

    @builder
    def x_plus_one(tracked_x):
        assert Tracked.n_instances_in_memory == 1
        return tracked_x.value + 1

    @builder
    def x_plus_two(x_plus_one):
        assert Tracked.n_instances_in_memory == 0
        return x_plus_one + 1

    assert builder.build().get("x_plus_two") == 3


# When an entity has both persistence and memoization disabled, we memoize it just for
# the duration of the get() call. However, we may want to disable this behavior in the
# future; for the time being it's configurable (but undocumented).
@pytest.mark.parametrize("disable_query_caching", [False, True])
def test_uncached_values_cached_for_query_duration(
    builder, make_counter, disable_query_caching
):
    x_counter = make_counter()

    if disable_query_caching:
        builder.set("core__temp_memoize_if_uncached", False)
        expected_x_calls_per_query = 2
    else:
        expected_x_calls_per_query = 1

    builder.set("core__persist_by_default", False)
    builder.set("core__memoize_by_default", False)

    builder.assign("y", 3)

    @builder
    @x_counter
    def x():
        return 2

    @builder
    def x_squared(x):
        return x * x

    @builder
    def y_x_squared(y, x_squared):
        return y * x_squared

    @builder
    def y_x_cubed(y_x_squared, x):
        return y_x_squared * x

    flow = builder.build()

    assert flow.get("y_x_cubed") == 24
    assert x_counter.times_called() == expected_x_calls_per_query

    assert flow.get("y_x_cubed") == 24
    assert x_counter.times_called() == expected_x_calls_per_query
