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
import shutil

import bionic as bn

from ..helpers import ResettingCallCounter


CACHE_TEST_DIR = os.path.join(
    os.path.dirname(__file__), "test_persistence_compatibility"
)


class Harness:
    """
    Holds a simple Bionic flow with counters to all the functions in it.
    """

    def __init__(self, cache_dir, make_counter):
        self.lowercase_sum_counter = make_counter()
        self.uppercase_sum_counter = make_counter()
        self.total_sum_counter = make_counter()

        builder = bn.FlowBuilder("test")

        builder.set("core__persistent_cache__flow_dir", cache_dir)

        # It's important that this test uses sets, because we want to check that sets
        # are hashed deterministically. (Set iteration is non-deterministic, but it's
        # always the same within one Python process, so a simpler test where we just
        # run a flow multiple times won't work for this.)
        builder.assign("lowercase_chars", set("abcdef"))
        builder.assign("uppercase_chars", frozenset("ABCDEF"))

        @builder
        @self.lowercase_sum_counter
        def lowercase_sum(lowercase_chars):
            return sum(ord(char) for char in lowercase_chars)

        @builder
        @self.uppercase_sum_counter
        def uppercase_sum(uppercase_chars):
            return sum(ord(char) for char in uppercase_chars)

        @builder
        @self.total_sum_counter
        def total_sum(lowercase_sum, uppercase_sum):
            return lowercase_sum + uppercase_sum

        self.flow = builder.build()


if __name__ == "__main__":

    def make_counter():
        return ResettingCallCounter()

    flow = Harness(CACHE_TEST_DIR, make_counter).flow

    shutil.rmtree(CACHE_TEST_DIR)

    # Make sure everything is written to the cache.
    flow.get("total_sum")
