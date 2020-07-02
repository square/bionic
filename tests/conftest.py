import pytest

import getpass
import random

from enum import Enum
from multiprocessing.managers import SyncManager

from .helpers import gsutil_path_exists, gsutil_wipe_path, ResettingCounter

import bionic as bn


class ExecutionMode(Enum):
    PARALLEL = "PARALLEL"
    SERIAL = "SERIAL"


# Parameterizing a fixture adds the parameter in the test name at the end,
# like test_name[PARALLEL] and test_name[SERIAL]. This is super helpful while
# debugging and much clearer than parameterizing `parallel_execution_enabled`
# which suffixes [TRUE] / [FALSE].
@pytest.fixture(params=[ExecutionMode.PARALLEL, ExecutionMode.SERIAL])
def parallel_execution_enabled(request):
    return request.param == ExecutionMode.PARALLEL


# We provide this at the top level because we want everyone using FlowBuilder
# to use a temporary directory rather than the default one.
@pytest.fixture
def builder(parallel_execution_enabled, tmp_path):
    builder = bn.FlowBuilder("test")
    builder.set("core__persistent_cache__flow_dir", str(tmp_path / "BNTESTDATA"))
    builder.set("core__parallel_execution__enabled", parallel_execution_enabled)
    return builder


# This is a different multiprocessing manager than the one we use in
# ExternalProcessLoggingManager. This one is responsible for sharing test
# objects between processes. Its' only used in make_counter and make_list
# fixtures. The purpose here is to not pollute the manager that bionic uses.
# I also don't want to replace the manager that bionic creates with a test
# one.
class PytestManager(SyncManager):
    pass


PytestManager.register("ResettingCounter", ResettingCounter)


@pytest.fixture(scope="session")
def multiprocessing_manager(request):
    manager = PytestManager()
    manager.start()
    request.addfinalizer(manager.shutdown)
    return manager


@pytest.fixture
def process_manager(parallel_execution_enabled, multiprocessing_manager):
    if not parallel_execution_enabled:
        return None
    return multiprocessing_manager


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
        "--all-execution-modes",
        action="store_true",
        default=False,
        help="also run all tests with parallel execution mode",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "needs_gcs: mark test as requiring GCS to run")
    config.addinivalue_line(
        "markers",
        "run_with_all_execution_modes_by_default: mark test to always run all execution modes",
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

    if not config.getoption("--all-execution-modes"):
        skip_execution_mode = pytest.mark.skip(
            reason="only runs when --all-execution-modes is set"
        )
        for item in items:
            if (
                any("ExecutionMode.PARALLEL" in keyword for keyword in item.keywords)
                and "run_with_all_execution_modes_by_default" not in item.keywords
            ):
                item.add_marker(skip_execution_mode)


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

    # This will throw an exception if the URL doesn't exist at this point.
    # Currently every test using this fixture does write some objects under this URL,
    # *and* doesn't clean all of them up. If this changes, we may need to start
    # handling this more gracefully.
    gsutil_wipe_path(gs_url)


@pytest.fixture(scope="function")
def tmp_gcs_url_prefix(session_tmp_gcs_url_prefix, request):
    """A temporary "directory" on GCS for a single test."""

    # `gsutil` doesn't support wildcard characters which are `[]` here.
    # This is an open issue with gsutil but till it's fixed, we are going
    # to change the node name to not have any wildcard characters.
    # https://github.com/GoogleCloudPlatform/gsutil/issues/290
    node_name = request.node.name.replace("[", "_").replace("]", "")
    return session_tmp_gcs_url_prefix + node_name + "/"


@pytest.fixture(scope="function")
def gcs_builder(builder, tmp_gcs_url_prefix):
    URL_PREFIX = "gs://"
    assert tmp_gcs_url_prefix.startswith(URL_PREFIX)
    gcs_path = tmp_gcs_url_prefix[len(URL_PREFIX) :]
    bucket_name, object_path = gcs_path.split("/", 1)

    builder = builder.build().to_builder()

    builder.set("core__persistent_cache__gcs__bucket_name", bucket_name)
    builder.set("core__persistent_cache__gcs__object_path", object_path)
    builder.set("core__persistent_cache__gcs__enabled", True)

    return builder
