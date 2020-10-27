import os
import pytest
import time

from bionic.executor import get_singleton_manager, logging_initializer
from bionic.deps.optdep import import_optional_dependency


@pytest.fixture
def loky_executor():
    loky = import_optional_dependency("loky", purpose="parallel execution")
    return loky.get_reusable_executor(
        max_workers=None,
        initializer=logging_initializer,
        initargs=(get_singleton_manager().logging_queue,),
    )


@pytest.mark.needs_parallel
def test_executor_resizes(builder, loky_executor):
    @builder
    def f1():
        # Sleep for a little while so that it doesn't finish before all jobs are
        # submitted. Otherwise, by the time flow execution code submits another
        # job, this job finishes and the executor reuses the process (same as the)
        # one that ran this job.
        # We can use Barrier objects here, but then, the test would be controlled
        # by the number of parties in the barrier. If we set barrier parties to
        # two, we won't know if the executor used only two processes because the
        # executor size was two. The executor size can be higher but still use
        # less than max size. Hence, we can't be sure that the executor was
        # resized correctly with barrier.
        time.sleep(0.1)
        return os.getpid()

    @builder
    def f2():
        time.sleep(0.1)
        return os.getpid()

    @builder
    def f3():
        return os.getpid()

    @builder
    def all(f1, f2, f3):
        return [f1, f2, f3]

    builder.set("core__flow_name", "flow1")
    builder.set("core__parallel_execution__worker_count", 1)
    flow1 = builder.build()

    builder.set("core__flow_name", "flow2")
    builder.set("core__parallel_execution__worker_count", 2)
    flow2 = builder.build()

    builder.set("core__flow_name", "flow3")
    builder.set("core__parallel_execution__worker_count", 3)
    flow3 = builder.build()

    assert len(set(flow3.get("all"))) == 3

    assert len(set(flow2.get("all"))) == 2

    assert len(set(flow1.get("all"))) == 1
