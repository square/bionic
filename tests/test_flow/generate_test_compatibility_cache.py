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

from ..helpers import count_calls

import bionic as bn


CACHE_TEST_DIR = os.path.join(os.path.dirname(__file__), 'test_persistence_compatibility')


class Harness:
    """
    Holds a simple Bionic flow alongside the different functions used in it.
    The functions are exposed separately since the tests that use the flow
    asserts the number of times the methods were invoked in the flow using
    the @count_calls annotation which is not exposed by Bionic.
    """

    def __init__(self, cache_dir):
        builder = bn.FlowBuilder('test')

        builder.set('core__persistent_cache__flow_dir', cache_dir)
        builder.assign('x', 2)
        builder.assign('y', 3)
        builder.assign('z', 4)

        @builder
        @count_calls
        def xy(x, y):
            return x * y

        @builder
        @count_calls
        def yz(y, z):
            return y * z

        @builder
        @count_calls
        def xy_plus_yz(xy, yz):
            return xy + yz

        self.flow = builder.build()
        self.xy = xy
        self.yz = yz
        self.xy_plus_yz = xy_plus_yz


if __name__ == "__main__":
    flow = Harness(CACHE_TEST_DIR).flow

    # call methods to write to cache
    flow.get('xy')
    flow.get('yz')
    flow.get('xy_plus_yz')
