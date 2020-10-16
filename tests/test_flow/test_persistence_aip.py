import pytest

import bionic as bn


# This is detected by pytest and applied to all the tests in this module.
pytestmark = [pytest.mark.no_parallel]


@pytest.mark.needs_aip
def test_aip_jobs(aip_builder):
    builder = aip_builder

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    @bn.aip_task_config("n1-standard-4")
    def a(x, y):
        return x * y

    @builder
    @bn.aip_task_config("n1-standard-8")
    def b(x, y):
        return x + y

    @builder
    def c(a, b):
        return a - b  # 6 (2 * 3) - 5 (2 + 3)

    assert builder.build().get("c") == 1


@pytest.mark.needs_aip
def test_aip_not_persisted_dependency(aip_builder):
    builder = aip_builder

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    @bn.persist(False)
    def x_plus_one(x):
        return x + 1

    @builder
    @bn.persist(False)
    @bn.memoize(False)
    def x_plus_two(x_plus_one):
        return x_plus_one + 1

    @builder
    @bn.aip_task_config("n1-standard-4")
    def x_plus_three(x_plus_two):
        return x_plus_two + 1

    @builder
    def total(x_plus_three, y):
        return x_plus_three + y

    assert builder.build().get("total") == 8
