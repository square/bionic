import getpass
import random
from multiprocessing.managers import SyncManager
from typing import Optional

import logging
import pytest

import bionic as bn
from bionic.gcs import get_gcs_fs_without_warnings
from .fakes import FakeGcsFs, FakeAipClient
from ..helpers import (
    assert_messages_match_regexes,
    SimpleCounter,
    ResettingCallCounter,
)


# Parameterizing a fixture adds the parameter in the test name at the end,
# like test_name[PARALLEL] and test_name[SERIAL]. This is super helpful while
# debugging and much clearer than parameterizing `parallel_execution_enabled`
# which suffixes [TRUE] / [FALSE].
@pytest.fixture(
    params=[
        pytest.param("serial", marks=pytest.mark.serial),
        pytest.param("parallel", marks=pytest.mark.parallel),
    ],
)
def parallel_execution_enabled(request) -> bool:
    return request.param == "parallel"


# This allows tests that depend on GCS and/or AIP to be run locally using fake
# GCP, and again using real GCP if the correct command line parameters are
# provided.
@pytest.fixture(
    params=[
        pytest.param("fake-gcp", marks=pytest.mark.fake_gcp),
        pytest.param("real-gcp", marks=pytest.mark.real_gcp),
    ],
)
def use_fake_gcp(request) -> bool:
    return request.param == "fake-gcp"


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
# objects between processes. It's only used in the make_counter and make_list
# fixtures. The purpose here is to not pollute the manager that bionic uses. I
# also don't want to replace the manager that bionic creates with a test one.
class PytestManager(SyncManager):
    pass


PytestManager.register("SimpleCounter", SimpleCounter)


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
            counter = SimpleCounter()
        else:
            counter = process_manager.SimpleCounter()
        return ResettingCallCounter(counter)

    return _make_counter


@pytest.fixture
def make_list(process_manager):
    def _make_list():
        if process_manager is None:
            return []
        else:
            return process_manager.list([])

    return _make_list


@pytest.fixture
def make_dict(process_manager):
    def _make_dict():
        if process_manager is None:
            return {}
        else:
            return process_manager.dict()

    return _make_dict


@pytest.fixture
def gcs_fs(use_fake_gcp, multiprocessing_manager):
    if use_fake_gcp:
        # When running an AIP job, the GCS filesystem is serialized and then
        # deserialized. Any data written to the deserialized instance needs to
        # be readable by the original instance. Hence, a proxy object is used to
        # store the fake GCS data. This is independent of whether parallel
        # processing has been enabled.
        return FakeGcsFs(multiprocessing_manager.dict())
    else:
        return get_gcs_fs_without_warnings()


@pytest.fixture
def gcs_builder(builder, tmp_gcs_url_prefix, use_fake_gcp, gcs_fs):
    URL_PREFIX = "gs://"
    assert tmp_gcs_url_prefix.startswith(URL_PREFIX)
    gcs_path = tmp_gcs_url_prefix[len(URL_PREFIX) :]
    bucket_name, object_path = gcs_path.split("/", 1)

    builder = builder.build().to_builder()

    builder.set("core__persistent_cache__gcs__bucket_name", bucket_name)
    builder.set("core__persistent_cache__gcs__object_path", object_path)
    builder.set("core__persistent_cache__gcs__enabled", True)

    if use_fake_gcp:
        builder.set("core__persistent_cache__gcs__fs", gcs_fs)
    else:
        # Since gcs is enabled, if core__persistent_cache__gcs__fs is not set,
        # the builder should use get_gcs_fs_without_warnings() by default.
        # The passed in gcs_fs is used in other places, verify that it is
        # not a fake.
        assert gcs_fs == get_gcs_fs_without_warnings()

    return builder


# Unlike gcs_builder, which is parametrized to be either real or fake, this builder is
# always fake.
@pytest.fixture
def fake_gcs_builder(builder, make_dict):
    builder = builder.build().to_builder()

    builder.set("core__persistent_cache__gcs__bucket_name", "some-bucket")
    builder.set("core__persistent_cache__gcs__object_path", "")
    builder.set("core__persistent_cache__gcs__enabled", True)
    builder.set("core__persistent_cache__gcs__fs", FakeGcsFs(make_dict()))

    return builder


@pytest.fixture
def aip_builder(gcs_builder, use_fake_gcp, gcs_fs, tmp_path):
    gcs_builder.set("core__aip_execution__enabled", True)
    gcs_builder.set("core__aip_execution__docker_image_name", "bionic:latest")

    if use_fake_gcp:
        gcs_builder.set("core__aip_execution__gcp_project_id", "fake-project")
        gcs_builder.set("core__aip_execution__poll_period_seconds", 0.1)
        gcs_builder.set("core__aip_client", FakeAipClient(gcs_fs, tmp_path))

    return gcs_builder


@pytest.fixture(scope="session")
def real_gcs_url_stem(request) -> Optional[str]:
    url = request.config.getoption("--bucket")
    if url is not None:
        assert url.startswith("gs://")
    return url


@pytest.fixture(scope="session")
def real_gcs_session_tmp_url_prefix(real_gcs_url_stem) -> Optional[str]:
    """
    Sets up and tears down a temporary "directory" on GCS to be shared by all
    of our tests. Not applicable for fake GCS.
    """

    if real_gcs_url_stem is None:
        yield None
        return

    gcs_fs = get_gcs_fs_without_warnings()

    random_hex_str = "%016x" % random.randint(0, 2 ** 64)
    path_str = f"{getpass.getuser()}/BNTESTDATA/{random_hex_str}"

    gs_url = real_gcs_url_stem + "/" + path_str + "/"
    assert not gcs_fs.exists(gs_url)

    yield gs_url

    # This will throw an exception if the URL doesn't exist at this point.
    # Currently every test using this fixture does write some objects under this URL,
    # *and* doesn't clean all of them up. If this changes, we may need to start
    # handling this more gracefully.
    gcs_fs.rm(gs_url, recursive=True)


@pytest.fixture
def tmp_gcs_url_prefix(use_fake_gcp, real_gcs_session_tmp_url_prefix, request) -> str:
    """A temporary "directory" on GCS for a single test."""
    if use_fake_gcp:
        return "gs://fake-bucket/BNTESTDATA/"

    assert real_gcs_session_tmp_url_prefix is not None

    # `gsutil` doesn't support wildcard characters which are `[]` here.
    # This is an open issue with gsutil but till it's fixed, we are going
    # to change the node name to not have any wildcard characters.
    # https://github.com/GoogleCloudPlatform/gsutil/issues/290
    # gcsfs seems to have the same problem.
    node_name = request.node.name.replace("[", "_").replace("]", "")
    return real_gcs_session_tmp_url_prefix + node_name + "/"


@pytest.fixture
def clear_test_gcs_data(tmp_gcs_url_prefix, gcs_fs):
    """
    Deletes all GCS data specific to a single test.
    """

    assert "BNTESTDATA" in tmp_gcs_url_prefix

    def _clear_test_gcs_data():
        gcs_fs.rm(tmp_gcs_url_prefix, recursive=True)

    return _clear_test_gcs_data


class LogChecker:
    def __init__(self, caplog):
        self._caplog = caplog

    def expect_all(self, *expected_messages):
        actual_messages = self._pop_messages()
        assert set(actual_messages) == set(expected_messages)
        self._caplog.clear()

    def expect_regex(self, *expected_patterns):
        assert_messages_match_regexes(
            self._pop_messages(),
            expected_patterns,
            allow_unmatched_messages=True,
        )

    def _pop_messages(self):
        messages = [self._format_message(record) for record in self._caplog.records]
        self._caplog.clear()
        return messages

    @staticmethod
    def _format_message(record):
        message = record.getMessage()
        if record.exc_text:
            message = message + " " + record.exc_text
        return message


@pytest.fixture
def log_checker(caplog) -> LogChecker:
    caplog.set_level(logging.INFO)
    return LogChecker(caplog)
