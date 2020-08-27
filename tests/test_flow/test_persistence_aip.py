import pytest

import bionic as bn


# This is detected by pytest and applied to all the tests in this module.
pytestmark = pytest.mark.needs_gcs


@pytest.fixture(params=[pytest.param("serial", marks=pytest.mark.serial)])
def parallel_execution_enabled(request):
    return request.param == "parallel"


@pytest.fixture(scope="function")
def aip_builder(gcs_builder):
    builder = gcs_builder

    builder.set("core__distributed_execution__enabled", "True")

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    @bn.aip_resource(bn.aip.task.Resource("n1-standard-4"))
    def a(x, y):
        return x * y

    @builder
    @bn.aip_resource(bn.aip.task.Resource("n1-standard-8"))
    def b(x, y):
        return x + y

    @builder
    def c(a, b):
        return a - b

    return builder


def test_aip_jobs(aip_builder):
    flow = aip_builder.build()

    assert flow.get("c") == 1
