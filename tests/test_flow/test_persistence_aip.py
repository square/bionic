import logging
import pytest

import bionic as bn

from bionic.aip.client import get_aip_client

pytestmark = [pytest.mark.no_parallel]


# Running a job in AIP can take a few minutes. To minimize testing time, avoid
# having too many AIP jobs.
@pytest.mark.needs_aip
def test_aip_jobs(aip_builder, log_checker, caplog):
    # For simplicity, ignore checking for the log message "Initializing AIP client ..."
    # which may or may not happen depending on whether the AIP client has been
    # loaded.
    with caplog.at_level(logging.CRITICAL):
        get_aip_client()

    builder = aip_builder

    # Test various combinations of persistence and memoization settings.

    builder.assign("a", 1)

    @builder
    def b():
        return 2

    @builder
    @bn.persist(False)
    def c():
        return 3

    @builder
    @bn.memoize(False)
    def d():
        return 4

    @builder
    @bn.persist(False)
    @bn.memoize(False)
    def e():
        return 5

    @builder
    @bn.aip_task_config("n1-standard-4")
    def x(a, b, c, d, e):
        return a + b + c + d + e + 1

    @builder
    @bn.aip_task_config("n1-standard-8")
    def y(a, b, c, d, e):
        return a + b + c + d + e + 1

    @builder
    def total(x, y):
        return x + y

    assert builder.build().get("x") == 16

    # Entities 'c' and 'e' are required by entity 'x' but are not persisted,
    # hence they are computed together in the same AIP job as entity 'x'.
    # Since currently only one entity is logged after each AIP computation,
    # there are no logs for entities 'c' and 'e'.
    # TODO: fix AIP logging so that non-persisted dependencies are logged.

    log_checker.expect_regex(
        r"Accessed   a\(a=1\) from definition",
        r"Uploading a\(a=1\) to GCS \.\.\.",
        r"Computing  b\(\) \.\.\.",
        r"Computed   b\(\)",
        r"Uploading b\(\) to GCS \.\.\.",
        r"Computing  d\(\) \.\.\.",
        r"Computed   d\(\)",
        r"Uploading d\(\) to GCS \.\.\.",
        r"Staging task .* at gs://.*",
        r"Submitting .*TaskKey\(dnode=EntityNode\(name='x'\), case_key=CaseKey\(a=1\)\).*",
        r"Started task on AI Platform: https://console.cloud.google.com/ai-platform/jobs/.*",
        r"Computed   x\(a=1\) using AIP",
        r"Downloading x\(a=1\) from GCS \.\.\.",
    )

    assert builder.build().get("total") == 32

    log_checker.expect_regex(
        r"Loaded     x\(a=1\) from disk cache",
        r"Staging task .* at gs://.*",
        r"Submitting .*TaskKey\(dnode=EntityNode\(name='y'\), case_key=CaseKey\(a=1\)\).*",
        r"Started task on AI Platform: https://console.cloud.google.com/ai-platform/jobs/.*",
        r"Computed   y\(a=1\) using AIP",
        r"Downloading y\(a=1\) from GCS \.\.\.",
        r"Computing  total\(a=1\) \.\.\.",
        r"Computed   total\(a=1\)",
        r"Uploading total\(a=1\) to GCS \.\.\.",
    )
