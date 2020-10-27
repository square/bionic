"""
This module tests Bionic's GCS caching. In order to run it, you need to set
the `--bucket` command line option with a GCS path (like `gs://BUCKET_NAME`).
Bionic will cache its data to randomly-generated prefix in this bucket, and
then clean it up after the tests finish.

These tests are pretty slow -- they take about 60 seconds for me.
"""

import tempfile
from pathlib import Path

import dask.dataframe as dd
import pytest

import bionic as bn
from bionic.exception import CodeVersioningError
from .fakes import InstrumentedFilesystem
from ..helpers import (
    df_from_csv_str,
    equal_frame_and_index_content,
    local_wipe_path,
)

# This is detected by pytest and applied to all the tests in this module.
pytestmark = pytest.mark.needs_gcs


@pytest.fixture
def instrumented_gcs_fs(gcs_fs, make_list):
    return InstrumentedFilesystem(gcs_fs, make_list)


@pytest.fixture
def preset_gcs_builder(gcs_builder, instrumented_gcs_fs):
    builder = gcs_builder

    builder.set("core__versioning_mode", "assist")
    builder.set("core__persistent_cache__gcs__fs", instrumented_gcs_fs)

    builder.assign("x", 2)
    builder.assign("y", 3)

    return builder


def test_gcs_caching(
    preset_gcs_builder,
    make_counter,
    instrumented_gcs_fs,
    parallel_execution_enabled,
    clear_test_gcs_data,
):
    call_counter = make_counter()
    builder = preset_gcs_builder

    @builder
    @call_counter
    def xy(x, y):
        return x * y

    artifact_regex = "^.*/xy\\.json$"

    def times_artifact_uploaded():
        return instrumented_gcs_fs.matching_urls_uploaded(artifact_regex)

    def times_artifact_downloaded():
        return instrumented_gcs_fs.matching_urls_downloaded(artifact_regex)

    flow = builder.build()
    local_cache_path_str = flow.get("core__persistent_cache__flow_dir")

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 2
    # TODO These assertions fail when parallel execution is enabled. I'm pretty sure
    # this is because our global patching of the GCS filesystem doesn't get properly
    # carried over to the other processes. In order to get the fix associated with
    # these tests merged, I'm going to leave this gross hack in place and then look into
    # injecting the filesystem instead of making it global.
    if not parallel_execution_enabled:
        assert times_artifact_uploaded() == 2
        assert times_artifact_downloaded() == 0

    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0
    if not parallel_execution_enabled:
        assert times_artifact_uploaded() == 0
        assert times_artifact_downloaded() == 0

    clear_test_gcs_data()
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0
    if not parallel_execution_enabled:
        assert times_artifact_uploaded() == 2
        assert times_artifact_downloaded() == 0

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0
    if not parallel_execution_enabled:
        assert times_artifact_uploaded() == 0
        assert times_artifact_downloaded() == 2

    clear_test_gcs_data()
    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 2
    if not parallel_execution_enabled:
        assert times_artifact_uploaded() == 2
        assert times_artifact_downloaded() == 0


def test_versioning(preset_gcs_builder, make_counter):
    call_counter = make_counter()
    builder = preset_gcs_builder

    @builder
    def xy(x, y):
        return x * y

    flow = builder.build()
    local_cache_path_str = flow.get("core__persistent_cache__flow_dir")

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12

    @builder  # noqa: F811
    def xy(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    flow = builder.build()
    with pytest.raises(CodeVersioningError):
        flow.get("xy")

    local_wipe_path(local_cache_path_str)
    flow = builder.build()
    with pytest.raises(CodeVersioningError):
        flow.get("xy")

    @builder  # noqa: F811
    @bn.version(minor=1)
    def xy(x, y):  # noqa: F811
        call_counter.mark()
        return y * x

    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1)
    def xy(x, y):  # noqa: F811
        call_counter.mark()
        return x ** y

    flow = builder.build()

    assert flow.get("xy") == 8
    assert flow.setting("x", 4).get("xy") == 64
    assert call_counter.times_called() == 2

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("xy") == 8
    assert flow.setting("x", 4).get("xy") == 64
    assert call_counter.times_called() == 0


def test_indirect_versioning(preset_gcs_builder, make_counter):
    call_counter = make_counter()
    builder = preset_gcs_builder

    @builder
    @bn.version(major=1)
    def xy(x, y):
        call_counter.mark()
        return x ** y

    flow = builder.build()
    assert flow.get("xy") == 8
    assert call_counter.times_called() == 1

    @builder
    def xy_plus(xy):
        return xy + 1

    flow = builder.build()

    assert flow.get("xy_plus") == 9
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1)
    def xy(x, y):  # noqa: F811
        call_counter.mark()
        return int(float(x)) ** y

    flow = builder.build()
    with pytest.raises(CodeVersioningError):
        flow.get("xy_plus")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def xy(x, y):  # noqa: F811
        call_counter.mark()
        return int(float(y)) ** x

    flow = builder.build()

    assert flow.get("xy_plus") == 9
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=2)
    def xy(x, y):  # noqa: F811
        call_counter.mark()
        return y ** x

    flow = builder.build()

    assert flow.get("xy_plus") == 10
    assert call_counter.times_called() == 1


def test_multifile_serialization(preset_gcs_builder, make_counter):
    call_counter = make_counter()
    builder = preset_gcs_builder

    dask_df = dd.from_pandas(
        df_from_csv_str(
            """
            color,number
            red,1
            blue,2
            green,3
            """
        ),
        npartitions=1,
    )

    @builder
    @bn.protocol.dask
    @call_counter
    def df():
        return dask_df

    flow = builder.build()
    local_cache_path_str = flow.get("core__persistent_cache__flow_dir")

    assert equal_frame_and_index_content(flow.get("df").compute(), dask_df.compute())
    assert equal_frame_and_index_content(flow.get("df").compute(), dask_df.compute())
    assert call_counter.times_called() == 1

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert equal_frame_and_index_content(flow.get("df").compute(), dask_df.compute())
    assert call_counter.times_called() == 0


def test_file_path_copying(preset_gcs_builder, make_counter):
    call_counter = make_counter()
    builder = preset_gcs_builder

    file_contents = "DATA"

    @builder
    @call_counter
    @bn.protocol.path(operation="move")
    def data_path():
        call_counter.mark()
        fd, filename = tempfile.mkstemp()
        with open(fd, "w") as f:
            f.write(file_contents)
        return Path(filename)

    flow = builder.build()
    local_cache_path_str = flow.get("core__persistent_cache__flow_dir")

    assert flow.get("data_path").read_text() == file_contents
    assert call_counter.times_called() == 1

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("data_path").read_text() == file_contents
    assert call_counter.times_called() == 0
