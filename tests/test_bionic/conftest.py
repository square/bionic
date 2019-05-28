import sys
import os

import pytest

import bionic as bn

# This allows us to import helper functions from any subdirectory of our tests.
# It's based on this answer: https://stackoverflow.com/questions/33508060/
# The more straightforward approach of creating packages in the test/ dirs
# seems to be discouraged in pytest.
sys.path.append(os.path.join(os.path.dirname(__file__), '_helpers'))


# We provide this at the top level because we want everyone using FlowBuilder
# to use a temporary directory rather than the default one.
@pytest.fixture(scope='function')
def builder(tmp_path):
    builder = bn.FlowBuilder('test')
    builder.set('core__persistent_cache__flow_dir', str(tmp_path))
    return builder
