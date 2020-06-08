import pytest

from ..conftest import ExecutionMode

import bionic.executor as bnexec


@pytest.fixture(params=[ExecutionMode.PARALLEL])
def parallel_processing_enabled(request):
    return request.param == ExecutionMode.PARALLEL


def test_exeuctor_resizes(builder):
    builder.set("core__parallel_processing__enabled", True)
    builder.assign("x", 1)

    @builder
    def y(x):
        return x

    builder.set("core__parallel_processing__worker_count", 2)
    flow1 = builder.build()

    builder.set("core__parallel_processing__worker_count", 3)
    flow2 = builder.build()

    assert flow1.get("y") == 1
    assert bnexec._executor.worker_count == 2

    assert flow2.get("y") == 1
    assert bnexec._executor.worker_count == 3

    assert flow1.get("y") == 1
    assert bnexec._executor.worker_count == 2
