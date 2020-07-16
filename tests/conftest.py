import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--bucket", action="store", help="URL to GCS bucket to use for tests"
    )
    parser.addoption(
        "--all-execution-modes",
        action="store_true",
        default=False,
        help="also run all tests with parallel execution mode",
    )


def pytest_configure(config):
    config.addinivalue_line("markers", "slow: mark test as slow to run")
    config.addinivalue_line("markers", "needs_gcs: mark test as requiring GCS to run")
    config.addinivalue_line(
        "markers",
        "run_with_all_execution_modes_by_default: mark test to always run all execution modes",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--slow"):
        skip_slow = pytest.mark.skip(reason="only runs when --slow is set")
        for item in items:
            if "slow" in item.keywords:
                item.add_marker(skip_slow)

    if not config.getoption("--bucket"):
        skip_gcs = pytest.mark.skip(reason="only runs when --bucket is set")
        for item in items:
            if "needs_gcs" in item.keywords:
                item.add_marker(skip_gcs)

    if not config.getoption("--all-execution-modes"):
        skip_execution_mode = pytest.mark.skip(
            reason="only runs when --all-execution-modes is set"
        )
        for item in items:
            if (
                any("ExecutionMode.PARALLEL" in keyword for keyword in item.keywords)
                and "run_with_all_execution_modes_by_default" not in item.keywords
            ):
                item.add_marker(skip_execution_mode)
