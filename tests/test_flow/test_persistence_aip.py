import pytest

import bionic as bn

# This is detected by pytest and applied to all the tests in this module.
from bionic.aip.state import AipError

pytestmark = pytest.mark.needs_aip


def test_aip_jobs(aip_builder):
    builder = aip_builder

    builder.assign("x1", 1)

    # Test various combinations of memoize and persist settings for these
    # function entities.

    @builder
    def x2():
        return 2

    @builder
    @bn.persist(False)
    def x3():
        return 3

    @builder
    @bn.memoize(False)
    def x4():
        return 4

    @builder
    @bn.persist(False)
    @bn.memoize(False)
    def x5():
        return 5

    @builder
    @bn.aip_task_config("n1-standard-4")
    def y1(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 1

    @builder
    @bn.aip_task_config("n1-standard-8")
    def y2(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 2

    @builder
    def y3(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 3

    @builder
    def y4(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 4

    @builder
    def y5(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 5

    @builder
    def total(y1, y2, y3, y4, y5):
        return y1 + y2 + y3 + y4 + y5

    assert builder.build().get("total") == 90


def test_aip_fail(aip_builder):
    builder = aip_builder

    builder.assign("x", 1)

    @builder
    @bn.aip_task_config("n1-standard-4")
    def x_plus_one(x):
        raise Exception()

    with pytest.raises(AipError):
        builder.build().get("x_plus_one")
