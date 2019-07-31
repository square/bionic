import pytest

import bionic as bn


# We provide this at the top level because we want everyone using FlowBuilder
# to use a temporary directory rather than the default one.
@pytest.fixture(scope='function')
def builder(tmp_path):
    builder = bn.FlowBuilder('test')
    builder.set(
        'core__persistent_cache__flow_dir', str(tmp_path / 'BNTESTDATA'))
    return builder
