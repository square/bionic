import pytest

import bionic as bn

from bionic.executor import get_singleton_manager, logging_initializer
from bionic.deps.optdep import import_optional_dependency

pytestmark = pytest.mark.needs_parallel


@pytest.fixture
def loky_executor():
    loky = import_optional_dependency("loky", purpose="parallel execution")
    return loky.get_reusable_executor(
        max_workers=None,
        initializer=logging_initializer,
        initargs=(get_singleton_manager().logging_queue,),
    )


def test_executor_resizes(builder, loky_executor):
    builder.assign("a", 1)

    @builder
    def b(a):
        return a

    @builder
    def c(b):
        return b

    @builder
    def d(c):
        return c

    builder.set("core__parallel_execution__worker_count", 2)
    flow1 = builder.build()

    builder.set("core__parallel_execution__worker_count", 3)
    flow2 = builder.build()

    assert flow1.get("b") == 1
    # It's gross to check a private variable of the executor but this is
    # the best way to check that it was resized correctly.
    # TODO: Return PIDs in functions and assert that PIDs are different.
    assert loky_executor._max_workers == 2

    # Call a non-cached entity so that a task is submitted to executor
    # and it resizes.
    assert flow2.get("c") == 1
    assert loky_executor._max_workers == 3

    # Call a non-cached entity so that a task is submitted to executor
    # and it resizes.
    assert flow1.get("d") == 1
    assert loky_executor._max_workers == 2


# Test that when bionic sends a job to a parallel or AIP executor, it does not
# need to wait for the results and can send more jobs to executors.
# Test only runs in fake AIP because it uses SyncManager Barrier.
@pytest.mark.fake_gcp_only
def test_parallel_and_aip(aip_builder, multiprocessing_manager):
    builder = aip_builder

    builder.assign("x", 1)

    # The barrier ensures that all the entity functions do not complete unless
    # all of them are started.
    barrier = multiprocessing_manager.Barrier(4, timeout=120)

    @builder
    def y1(x):
        barrier.wait()
        return x + 1

    @builder
    def y2(x):
        barrier.wait()
        return x + 1

    @builder
    @bn.aip_task_config("n1-standard-4")
    def y3(x):
        barrier.wait()
        return x + 1

    @builder
    @bn.aip_task_config("n1-standard-4")
    def y4(x):
        barrier.wait()
        return x + 1

    @builder
    def total(y1, y2, y3, y4):
        return y1 + y2 + y3 + y4

    assert builder.build().get("total") == 8


# Similar to the test above, but with entity functions that fail to compute.
# When one or more entity functions fail, bionic should wait for all other
# concurrent tasks to complete and log all the exceptions.
@pytest.mark.fake_gcp_only
def test_parallel_fail(aip_builder, make_counter, multiprocessing_manager, log_checker):
    builder = aip_builder

    builder.assign("x", 1)

    y1_counter = make_counter()
    y2_counter = make_counter()
    y3_counter = make_counter()
    y4_counter = make_counter()

    # The barrier ensures that all the entity functions do not complete unless
    # all of them are started.
    barrier = multiprocessing_manager.Barrier(4, timeout=120)

    @builder
    @y1_counter
    def y1(x):
        barrier.wait()
        return x + 1

    @builder
    @y2_counter
    def y2(x):
        barrier.wait()
        raise Exception()

    @builder
    @bn.aip_task_config("n1-standard-4")
    @y3_counter
    def y3(x):
        barrier.wait()
        raise Exception()

    @builder
    @bn.aip_task_config("n1-standard-4")
    @y4_counter
    def y4(x):
        barrier.wait()
        return x + 1

    @builder
    def total(y1, y2, y3, y4):
        return y1 + y2 + y3 + y4

    with pytest.raises(Exception):
        builder.build().get("total")

    # Verify that, when multiple jobs fail, bionic can log all the exceptions.
    log_checker.expect_regex(
        r".*An exception was thrown while computing the value of descriptor '<y2>'.*",
        r".*An exception was thrown while computing the value of descriptor '<y3>'.*",
    )

    assert y1_counter.times_called() == 1
    assert y2_counter.times_called() == 1
    assert y3_counter.times_called() == 1
    assert y4_counter.times_called() == 1

    # flake8: noqa: E811
    @builder
    @y2_counter
    def y2(x):
        return x + 1

    # flake8: noqa: E811
    @builder
    @bn.aip_task_config("n1-standard-4")
    @y3_counter
    def y3(x):
        return x + 1

    assert builder.build().get("total") == 8

    assert y1_counter.times_called() == 0
    assert y2_counter.times_called() == 1
    assert y3_counter.times_called() == 1
    assert y4_counter.times_called() == 0
