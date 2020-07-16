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
    def add_mark(name, description):
        config.addinivalue_line("markers", f"{name}: given test {description}")

    # These markers are added manually.
    add_mark("slow", "runs slowly")
    add_mark("needs_gcs", "requires GCS to run")
    add_mark("needs_parallel", "requires parallel execution to run")
    add_mark(
        "allows_parallel",
        "can run with parallel execution even when that's not explicitly enabled",
    )

    # These markers are added automatically based on parametric fixtures.
    add_mark("serial", "will run using serial execution")
    add_mark("parallel", "will run using parallel execution")


def pytest_collection_modifyitems(config, items):
    also_run_slow = config.getoption("--slow")
    skip_slow = pytest.mark.skip(reason="only runs when --slow is set")

    has_gcs = config.getoption("--bucket")
    skip_needs_gcs = pytest.mark.skip(reason="only runs when --bucket is set")

    also_run_parallel = config.getoption("--all-execution-modes")

    items_to_keep = []
    for item in items:
        if "slow" in item.keywords and not also_run_slow:
            item.add_marker(skip_slow)

        if "needs_gcs" in item.keywords and not has_gcs:
            item.add_marker(skip_needs_gcs)

        if "parallel" in item.keywords:
            if not (also_run_parallel or "allows_parallel" in item.keywords):
                continue
        elif "needs_parallel" in item.keywords:
            continue

        items_to_keep.append(item)

    items.clear()
    items.extend(items_to_keep)
