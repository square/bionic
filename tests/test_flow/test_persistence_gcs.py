"""
This module tests Bionic's GCS caching. In order to run it, you need to set
the `--bucket` command line option with a GCS path (like `gs://BUCKET_NAME`).
Bionic will cache its data to randomly-generated prefix in this bucket, and
then clean it up after the tests finish.

These tests are pretty slow -- they take about 60 seconds for me.
"""

import pytest

from pathlib import Path
import tempfile

import dask.dataframe as dd

from ..helpers import (
    ResettingCounter,
    df_from_csv_str,
    equal_frame_and_index_content,
    gsutil_wipe_path,
    local_wipe_path,
)
from bionic.exception import CodeVersioningError

import bionic as bn


# This is detected by pytest and applied to all the tests in this module.
pytestmark = pytest.mark.needs_gcs


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

    builder.set("core__versioning_mode", "assist")

    return builder


# This should really be multiple separate tests, but it's expensive to do the
# setup, teardown, and client initialization, so we'll just do it all in one
# place.
# TODO Now that we have a workspace fixture and cached client initialization,
# this may not be true anymore.
def test_gcs_caching(gcs_builder, make_counter):
    # Setup.

    call_counter = make_counter()

    builder = gcs_builder

    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    def xy(x, y):
        call_counter.mark()
        return x * y

    # Test reading from and writing to GCS cache.

    flow = builder.build()

    local_cache_path_str = flow.get("core__persistent_cache__flow_dir")
    gcs_cache_url = flow.get("core__persistent_cache__gcs__url")

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 2

    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0

    gsutil_wipe_path(gcs_cache_url)
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 0

    gsutil_wipe_path(gcs_cache_url)
    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("xy") == 6
    assert flow.setting("x", 4).get("xy") == 12
    assert call_counter.times_called() == 2

    # Test versioning.
    @builder  # noqa: F811
    def xy(x, y):
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
    def xy(x, y):
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
    def xy(x, y):
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

    # Test indirect versioning.
    @builder
    def xy_plus(xy):
        return xy + 1

    flow = builder.build()

    assert flow.get("xy_plus") == 9
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=1)
    def xy(x, y):
        call_counter.mark()
        return int(float(x)) ** y

    flow = builder.build()
    with pytest.raises(CodeVersioningError):
        flow.get("xy_plus")

    @builder  # noqa: F811
    @bn.version(major=1, minor=1)
    def xy(x, y):
        call_counter.mark()
        return int(float(y)) ** x

    flow = builder.build()

    assert flow.get("xy_plus") == 9
    assert call_counter.times_called() == 0

    @builder  # noqa: F811
    @bn.version(major=2)
    def xy(x, y):
        call_counter.mark()
        return y ** x

    flow = builder.build()

    assert flow.get("xy_plus") == 10
    assert call_counter.times_called() == 1

    # Test multi-file serialization.
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
    def df():
        call_counter.mark()
        return dask_df

    flow = builder.build()

    assert equal_frame_and_index_content(flow.get("df").compute(), dask_df.compute())
    assert equal_frame_and_index_content(flow.get("df").compute(), dask_df.compute())
    assert call_counter.times_called() == 1

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert equal_frame_and_index_content(flow.get("df").compute(), dask_df.compute())
    assert call_counter.times_called() == 0

    # Test file path copying.
    file_contents = "DATA"

    @builder
    @bn.protocol.path(operation="move")
    def data_path():
        call_counter.mark()
        fd, filename = tempfile.mkstemp()
        with open(fd, "w") as f:
            f.write(file_contents)
        return Path(filename)

    flow = builder.build()

    assert flow.get("data_path").read_text() == file_contents
    assert call_counter.times_called() == 1

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get("data_path").read_text() == file_contents
    assert call_counter.times_called() == 0
