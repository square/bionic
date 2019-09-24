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


# These three functions add a --slow command line option to enable slow tests.
# Seems involved, but it's the approach recommended in the pytest docs.
def pytest_addoption(parser):
    parser.addoption(
        '--slow', action='store_true', default=False, help='run slow tests'
    )


def pytest_configure(config):
    config.addinivalue_line('markers', 'slow: mark test as slow to run')


def pytest_collection_modifyitems(config, items):
    if config.getoption('--slow'):
        # If the option is present, don't skip slow tests.
        return
    skip_slow = pytest.mark.skip(reason='only runs when --slow is set')
    for item in items:
        if 'slow' in item.keywords:
            item.add_marker(skip_slow)
