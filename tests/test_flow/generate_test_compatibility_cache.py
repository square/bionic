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

    EXPECTED_TOTAL_SUM = 1002

    def __init__(self, cache_dir, make_counter):
        lowercase_sum_counter = make_counter()
        uppercase_sum_counter = make_counter()
        total_sum_counter = make_counter()

        builder = bn.FlowBuilder("test")

        builder.set("core__persistent_cache__flow_dir", cache_dir)

        # It's important that this test uses sets, because we want to check that sets
        # are hashed deterministically. (Set iteration is non-deterministic, but it's
        # always the same within one Python process, so a simpler test where we just
        # run a flow multiple times won't work for this.)
        builder.assign("lowercase_chars", set("abcdef"))
        builder.assign("uppercase_chars", frozenset("ABCDEF"))

        @builder
        def lowercase_sum(lowercase_chars):
            lowercase_sum_counter.mark()
            return sum(ord(char) for char in lowercase_chars)

        @builder
        def uppercase_sum(uppercase_chars):
            uppercase_sum_counter.mark()
            return sum(ord(char) for char in uppercase_chars)

        @builder
        def total_sum(lowercase_sum, uppercase_sum):
            total_sum_counter.mark()
            return lowercase_sum + uppercase_sum

        self.lowercase_sum_counter = lowercase_sum_counter
        self.uppercase_sum_counter = uppercase_sum_counter
        self.total_sum_counter = total_sum_counter

        self.manual_flow = builder.build()
        builder.set("core__versioning_mode", "auto")
        self.auto_flow = builder.build()

    @property
    def flows(self):
        return [self.manual_flow, self.auto_flow]


if __name__ == "__main__":

    def make_counter():
        return ResettingCallCounter()

    harness = Harness(CACHE_TEST_DIR, make_counter)

    shutil.rmtree(CACHE_TEST_DIR)

    for flow in harness.flows:
        # Make sure everything is written to the cache.
        flow.get("total_sum")
