import pytest
import shutil

from .generate_test_compatibility_cache import Harness, CACHE_TEST_DIR


@pytest.fixture
def older_serialized_cache_harness(tmp_path):
    # shutil.copytree dest should not exist
    tmp_cache_path = tmp_path.joinpath('test_cache')
    shutil.copytree(CACHE_TEST_DIR, tmp_cache_path)
    harness = Harness(tmp_cache_path)
    return harness


# Tests caching backward compatibility by loading and deserializaing
# an old snapshot of the cache. Test failure indicates that the changes
# made to the caching layer are backward incompatible.
# In case of a failure, either
# a) fix the caching logic so it's backward compatible or
# b) update the cache schema version and generate a new cache snapshot.
#
# To update cache schema version, change `CACHE_SCHEMA_VERSION` in cache.py.
#
# To renegerate cache, run the following command from bionic/ dir
#   `python -m tests.test_flow.generate_test_compatibility_cache`
def test_caching_compatibility(older_serialized_cache_harness):
    flow = older_serialized_cache_harness.flow
    assert flow.get('xy') == 6
    assert flow.get('yz') == 12
    assert flow.get('xy_plus_yz') == 18

    # assert that no methods were called
    older_serialized_cache_harness.xy.times_called() == 0
    older_serialized_cache_harness.yz.times_called() == 0
    older_serialized_cache_harness.xy_plus_yz.times_called() == 0
