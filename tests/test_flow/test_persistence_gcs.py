'''
This module tests Bionic's GCS caching.  In order to run it, you need to set
the ``BIONIC_GCS_TEST_BUCKET`` environmet variable with the name of a GCS
bucket you have access to.  Bionic will cache its data to randomly-generated
prefix in this bucket, and then clean it up after the tests finish.

These tests are pretty slow -- they take about 40 seconds for me.
'''

import pytest
import random
import subprocess
import getpass
import shutil

from ..helpers import count_calls, skip_unless_gcs, GCS_TEST_BUCKET


# This is detected by pytest and applied to all the tests in this module.
pytestmark = skip_unless_gcs


def gsutil_wipe_path(url):
    assert 'BNTESTDATA' in url
    subprocess.check_call(['gsutil', 'rm', '-rf', url])


def gsutil_path_exists(url):
    return subprocess.call(['gsutil', 'ls', url]) == 0


def local_wipe_path(path_str):
    assert 'BNTESTDATA' in path_str
    shutil.rmtree(path_str)


@pytest.fixture(scope='module')
def bucket_name():
    return GCS_TEST_BUCKET


@pytest.fixture(scope='function')
def tmp_object_path(bucket_name):
    random_hex_str = '%016x' % random.randint(0, 2 ** 64)
    path_str = '%s/BNTESTDATA/%s' % (getpass.getuser(), random_hex_str)

    gs_url = 'gs://%s/%s' % (bucket_name, path_str)
    assert not gsutil_path_exists(gs_url)

    yield path_str

    gsutil_wipe_path(gs_url)


@pytest.fixture(scope='function')
def gcs_builder(builder, bucket_name, tmp_object_path):
    builder = builder.build().to_builder()

    builder.set('core__persistent_cache__gcs__bucket_name', bucket_name)
    builder.set('core__persistent_cache__gcs__object_path', tmp_object_path)
    builder.set('core__persistent_cache__gcs__enabled', True)

    return builder


def test_gcs_caching(gcs_builder):
    builder = gcs_builder

    builder.assign('x', 2)
    builder.assign('y', 3)

    @builder
    @count_calls
    def xy(x, y):
        return x * y

    flow = builder.build()

    local_cache_path_str = flow.get('core__persistent_cache__flow_dir')
    gcs_cache_url = flow.get('core__persistent_cache__gcs__url')

    assert flow.get('xy') == 6
    assert xy.times_called() == 1

    flow = builder.build()

    assert flow.get('xy') == 6
    assert xy.times_called() == 0

    gsutil_wipe_path(gcs_cache_url)
    flow = builder.build()

    assert flow.get('xy') == 6
    assert xy.times_called() == 0

    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get('xy') == 6
    assert xy.times_called() == 0

    gsutil_wipe_path(gcs_cache_url)
    local_wipe_path(local_cache_path_str)
    flow = builder.build()

    assert flow.get('xy') == 6
    assert xy.times_called() == 1
