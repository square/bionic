import pytest

import getpass
import random

from multiprocessing.managers import SyncManager

from .helpers import gsutil_path_exists, gsutil_wipe_path, ResettingCounter

import bionic as bn
from bionic.decorators import persist
from bionic.deriver import TaskKeyLogger
from bionic.optdep import import_optional_dependency


@pytest.fixture(scope="session")
def executor(uses_parallel_processing):
    if not uses_parallel_processing:
        return None

    loky = import_optional_dependency("loky", purpose="parallel processing")
    return loky.get_reusable_executor()


@pytest.fixture(scope="session")
def manager(uses_parallel_processing):
    if not uses_parallel_processing:
        return None

    class MyManager(SyncManager):
        pass

    MyManager.register("ResettingCounter", ResettingCounter)
    MyManager.register("TaskKeyLogger", TaskKeyLogger)
    manager = MyManager()
    manager.start()
    return manager


@pytest.fixture(scope="session")
def uses_parallel_processing(request):
    return request.config.getoption("--parallel")


# We provide this at the top level because we want everyone using FlowBuilder
# to use a temporary directory rather than the default one.
@pytest.fixture(scope="function")
def builder(executor, manager, tmp_path):
    builder = bn.FlowBuilder("test")
    builder.set("core__persistent_cache__flow_dir", str(tmp_path / "BNTESTDATA"))

    # We can't use builder.set here because that uses ValueProvider which tries to
    # tokenize the value by writing / pickling it. We go around that issue by making
    # them use FunctionProvider.
    @builder
    @persist(False)
    def core__process_executor():
        return executor

    @builder
    @persist(False)
    def core__process_manager():
        return manager

    return builder


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
