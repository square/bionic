# This script generates cache for a flow represented in Harness class
# inside test_dir (tests/test_flow/test_persistence_compatibility).
# The generated cache is used by test_persistence_compatibility.py tests
# to validate that the cache can be deserialized by current Bionic.
# In case the caching has changed, this file is used to replace the
# test cache.
#
# To renegerate cache, run the following command from bionic/ dir
#   `python -m tests.test_flow.generate_test_compatibility_cache`
#
# Note that the repo ignores *.pkl datafiles which is bypassed using
# "Test data" section in .gitignore.

import os

import bionic as bn

from ..conftest import ResettingCounter
from ..helpers import count_calls


CACHE_TEST_DIR = os.path.join(
    os.path.dirname(__file__), "test_persistence_compatibility"
)


class Harness:
    """
    Holds a simple Bionic flow with counters to all the functions in it.
    """

    def __init__(self, cache_dir, make_counter):
        builder = bn.FlowBuilder("test")

        builder.set("core__persistent_cache__flow_dir", cache_dir)
        builder.assign("x", 2)
        builder.assign("y", 3)
        builder.assign("z", 4)

        xy_counter = make_counter()

        @builder
        @count_calls(xy_counter)
        def xy(x, y):
            return x * y

        yz_counter = make_counter()

        @builder
        @count_calls(yz_counter)
        def yz(y, z):
            return y * z

        xy_plus_yz_counter = make_counter()

        @builder
        @count_calls(xy_plus_yz_counter)
        def xy_plus_yz(xy, yz):
            return xy + yz

        self.flow = builder.build()
        self.xy_counter = xy_counter
        self.yz_counter = yz_counter
        self.xy_plus_yz_counter = xy_plus_yz_counter


if __name__ == "__main__":

    def make_counter():
        return ResettingCounter()

    flow = Harness(CACHE_TEST_DIR, make_counter).flow

    # call methods to write to cache
    flow.get("xy")
    flow.get("yz")
    flow.get("xy_plus_yz")
