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
