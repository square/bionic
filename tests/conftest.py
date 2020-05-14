import pytest

import getpass
import logging
import random

from multiprocessing.managers import SyncManager

from .helpers import gsutil_path_exists, gsutil_wipe_path, ResettingCounter

import bionic as bn
from bionic.decorators import persist
from bionic.deriver import TaskKeyLogger
from bionic.logging import LoggingReceiver, WorkerProcessLogHandler
from bionic.optdep import import_optional_dependency


@pytest.fixture(scope="session")
def parallel_processing_enabled(request):
    return request.config.getoption("--parallel")


@pytest.fixture(scope="session")
def process_manager(parallel_processing_enabled, request):
    if not parallel_processing_enabled:
        return None

    class MyManager(SyncManager):
        pass

    MyManager.register("ResettingCounter", ResettingCounter)
    manager = MyManager()
    manager.start()
    request.addfinalizer(manager.shutdown)

    return manager


@pytest.fixture(scope="session")
def logging_queue(parallel_processing_enabled, process_manager):
    if not parallel_processing_enabled:
        return None
    return process_manager.Queue(-1)


@pytest.fixture(scope="session")
def logging_receiver(logging_queue, parallel_processing_enabled, request):
    if not parallel_processing_enabled:
        return None

    logging_receiver = LoggingReceiver(logging_queue)
    logging_receiver.start()
    request.addfinalizer(logging_receiver.stop)
    return logging_receiver


@pytest.fixture(scope="session")
def process_executor(logging_queue, parallel_processing_enabled):
    if not parallel_processing_enabled:
        return None

    # Copied from the original process_executor in flow.py.
    def logging_initializer():
        logger = logging.getLogger()
        orig_handlers = logger.handlers
        for orig_handler in orig_handlers:
            logger.removeHandler(orig_handler)
        logger.addHandler(WorkerProcessLogHandler(logging_queue))
        logger.setLevel(logging.DEBUG)

    loky = import_optional_dependency("loky", purpose="parallel processing")
    return loky.get_reusable_executor(max_workers=2, initializer=logging_initializer)


# We provide this at the top level because we want everyone using FlowBuilder
# to use a temporary directory rather than the default one.
@pytest.fixture(scope="function")
def builder(
    logging_queue,
    logging_receiver,
    parallel_processing_enabled,
    process_executor,
    process_manager,
    request,
    tmp_path,
):
    builder = bn.FlowBuilder("test")
    builder.set("core__persistent_cache__flow_dir", str(tmp_path / "BNTESTDATA"))
    builder.set("core__parallel_processing__enabled", parallel_processing_enabled)

    # We can't use builder.set here because that uses ValueProvider which tries to
    # tokenize the value by writing / pickling it. We go around that issue by making
    # them use FunctionProvider.
    @builder
    @persist(False)
    def core__process_executor():
        return process_executor

    @builder
    @persist(False)
    def core__process_manager():
        return process_manager

    @builder
    @persist(False)
    def core__logging_queue():
        return logging_queue

    @builder
    @persist(False)
    def core__logging_receiver():
        return logging_receiver

    return builder


@pytest.fixture
def make_counter(process_manager):
    def _make_counter():
        if process_manager is None:
            return ResettingCounter()
        else:
            return process_manager.ResettingCounter()

    return _make_counter


@pytest.fixture
def make_list(process_manager):
    def _make_list():
        if process_manager is None:
            return []
        else:
            return process_manager.list([])

    return _make_list


def pytest_addoption(parser):
    parser.addoption(
        "--slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--bucket", action="store", help="URL to GCS bucket to use for tests"
    )
    parser.addoption(
        "--parallel",
        action="store_true",
        default=False,
        help="uses parallel processing",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "needs_gcs: mark test as requiring GCS to run")
    config.addinivalue_line(
        "markers",
        "no_parallel: mark test as not supported by parallel processing to run",
    )
    config.addinivalue_line(
        "markers", "only_parallel: mark test as requiring parallel processing to run"
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--slow"):
        skip_slow = pytest.mark.skip(reason="only runs when --slow is set")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--bucket"):
        skip_gcs = pytest.mark.skip(reason="only runs when --bucket is set")
        for item in items:
            if "needs_gcs" in item.keywords:
                item.add_marker(skip_gcs)

    if config.getoption("--parallel"):
        skip_no_parallel = pytest.mark.skip(
            reason="only runs when --parallel is not set"
        )
        for item in items:
            if "no_parallel" in item.keywords:
                item.add_marker(skip_no_parallel)
    else:
        skip_only_parallel = pytest.mark.skip(reason="only runs when --parallel is set")
        for item in items:
            if "only_parallel" in item.keywords:
                item.add_marker(skip_only_parallel)


@pytest.fixture(scope="session")
def gcs_url_stem(request):
    url = request.config.getoption("--bucket")
    assert url.startswith("gs://")
    return url


@pytest.fixture(scope="session")
def session_tmp_gcs_url_prefix(gcs_url_stem):
    """
    Sets up and tears down a temporary "directory" on GCS to be shared by all
    of our tests.
    """

    random_hex_str = "%016x" % random.randint(0, 2 ** 64)
    path_str = f"{getpass.getuser()}/BNTESTDATA/{random_hex_str}"

    gs_url = gcs_url_stem + "/" + path_str + "/"
    # This emits a stderr warning because the URL doesn't exist.  That's
    # annoying but I wasn't able to find a straightforward way to avoid it.
    assert not gsutil_path_exists(gs_url)

    yield gs_url

    gsutil_wipe_path(gs_url)


@pytest.fixture(scope="function")
def tmp_gcs_url_prefix(session_tmp_gcs_url_prefix, request):
    """A temporary "directory" on GCS for a single test."""

    return session_tmp_gcs_url_prefix + request.node.name + "/"
