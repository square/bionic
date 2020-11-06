import pytest


@pytest.fixture(autouse=True)
def set_env_variables(monkeypatch):
    # We don't want to set up Stackdriver logging for local tests.
    monkeypatch.setenv("BIONIC_NO_STACKDRIVER", "True")
    yield
    monkeypatch.delenv("BIONIC_NO_STACKDRIVER")


def pytest_addoption(parser):
    parser.addoption(
        "--slow", action="store_true", default=False, help="run slow tests"
    )
    parser.addoption(
        "--bucket", action="store", help="URL to GCS bucket to use for tests"
    )
    parser.addoption(
        "--project", action="store", help="GCP project to use for AIP tests"
    )
    parser.addoption(
        "--parallel",
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
    add_mark("needs_aip", "requires AIP execution to run")
    add_mark("needs_parallel", "requires parallel execution to run")
    add_mark("no_parallel", "does not run with parallel execution")
    add_mark(
        "allows_parallel",
        "can run with parallel execution even when that's not explicitly enabled",
    )

    # These markers are added automatically based on parametric fixtures.
    add_mark("serial", "will run using serial execution")
    add_mark("parallel", "will run using parallel execution")
    add_mark("real_gcp", "use real gcp")
    add_mark("fake_gcp", "use fake gcp")

    # This marker is added automatically based on other markers.
    add_mark("baseline", "runs by default when no options are passed to pytest")


def pytest_collection_modifyitems(config, items):
    also_run_slow = config.getoption("--slow")
    skip_slow = pytest.mark.skip(reason="only runs when --slow is set")

    has_gcs = config.getoption("--bucket")
    skip_needs_gcs = pytest.mark.skip(reason="only runs when --bucket is set")

    has_aip = config.getoption("--project") and has_gcs
    skip_needs_aip = pytest.mark.skip(
        reason="only runs when --bucket and --project are set"
    )

    also_run_parallel = config.getoption("--parallel")

    items_to_keep = []
    for item in items:
        item_is_baseline = True

        if "slow" in item.keywords:
            item_is_baseline = False
            if not also_run_slow:
                item.add_marker(skip_slow)

        if "real_gcp" in item.keywords:
            if "needs_gcs" in item.keywords:
                item_is_baseline = False
                if not has_gcs:
                    item.add_marker(skip_needs_gcs)

            if "needs_aip" in item.keywords:
                item_is_baseline = False
                if not has_aip:
                    item.add_marker(skip_needs_aip)

        if "parallel" in item.keywords:
            if "allows_parallel" not in item.keywords:
                item_is_baseline = False

                if "no_parallel" in item.keywords or not also_run_parallel:
                    continue

        elif "needs_parallel" in item.keywords:
            continue

        if item_is_baseline:
            item.add_marker(pytest.mark.baseline)

        items_to_keep.append(item)

    items.clear()
    items.extend(items_to_keep)
