import pytest

import logging


class LogChecker:
    def __init__(self, caplog):
        self._caplog = caplog

    def expect(self, *expected_messages):
        actual_messages = [record.getMessage() for record in self._caplog.records]
        assert set(actual_messages) == set(expected_messages)
        self._caplog.clear()


@pytest.fixture(scope="function")
def log_checker(caplog):
    caplog.set_level(logging.INFO)
    return LogChecker(caplog)


@pytest.mark.no_parallel
def test_logging_details(builder, log_checker):
    """
    Test the details of the log messages we emit. Since these messages are currently the
    best way to get visibility into what Bionic is doing, we have much more detailed
    tests than we'd normally want for logging. This means we'll have to tweak these
    tests as we update the format or implementation details of our logging.

    At some point we should introduce a separate system for user-facing
    progress reporting instead of using logs.
    """

    builder.assign("x", 1)

    @builder
    def x_plus_one(x):
        return x + 1

    @builder
    def x_plus_two(x_plus_one):
        return x_plus_one + 1

    flow = builder.build()
    assert flow.get("x_plus_one") == 2

    log_checker.expect(
        "Accessed   x(x=1) from definition",
        "Computing  x_plus_one(x=1) ...",
        "Computed   x_plus_one(x=1)",
    )

    assert flow.get("x_plus_two") == 3
    log_checker.expect(
        "Accessed   x_plus_one(x=1) from in-memory cache",
        "Computing  x_plus_two(x=1) ...",
        "Computed   x_plus_two(x=1)",
    )

    flow = builder.build()
    assert flow.get("x_plus_one") == 2
    log_checker.expect(
        # We need to access x in order to determine whether x_plus_one can be
        # loaded from disk.
        "Accessed   x(x=1) from definition",
        "Loaded     x_plus_one(x=1) from disk cache",
    )

    flow = builder.build()
    assert flow.get("x_plus_two") == 3
    log_checker.expect(
        # We need to access x in order to determine whether x_plus_two can be
        # loaded from disk.
        "Accessed   x(x=1) from definition",
        # However, we don't log anything for x_plus_one, since we only load its
        # hash, not its actual value. A little weird, but probably not worth
        # worrying too much about.
        "Loaded     x_plus_two(x=1) from disk cache",
    )

    flow = flow.setting("x_plus_one", 3)
    assert flow.get("x_plus_two") == 4
    log_checker.expect(
        "Accessed   x_plus_one(x_plus_one=3) from definition",
        "Computing  x_plus_two(x_plus_one=3) ...",
        "Computed   x_plus_two(x_plus_one=3)",
    )
