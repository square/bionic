import pytest

import bionic as bn

# This is detected by pytest and applied to all the tests in this module.
from bionic.aip.state import AipError

pytestmark = pytest.mark.needs_aip


def test_aip_jobs(aip_builder, log_checker):
    builder = aip_builder

    builder.assign("x1", 1)

    # Test various combinations of memoize and persist settings for these
    # function entities.

    @builder
    def x2():
        return 2

    @builder
    @bn.persist(False)
    def x3():
        return 3

    @builder
    @bn.memoize(False)
    def x4():
        return 4

    @builder
    @bn.persist(False)
    @bn.memoize(False)
    def x5():
        return 5

    @builder
    @bn.aip_task_config("n1-standard-4")
    def y1(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 1

    @builder
    @bn.aip_task_config("n1-standard-8")
    def y2(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 2

    @builder
    def y3(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 3

    @builder
    def y4(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 4

    @builder
    def y5(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 5

    @builder
    def total(y1, y2, y3, y4, y5):
        return y1 + y2 + y3 + y4 + y5

    assert builder.build().get("y1") == 16

    log_checker.expect_regex(
        r"Staging AI Platform task .* at gs://.*bionic_y1.*",
        r"Started AI Platform task: https://console.cloud.google.com/ai-platform/jobs/.*bionic_y1.*",
        r"Submitting AI Platform task .*TaskKey\(dnode=EntityNode\(name='y1'\), case_key=CaseKey\(x1=1\)\).*",
        r"Computed   y1\(x1=1\) using AI Platform",
        r"Downloading y1\(x1=1\) from GCS \.\.\.",
    )

    assert builder.build().get("total") == 90

    log_checker.expect_regex(
        r"Loaded     y1\(x1=1\) from disk cache",
        r"Staging AI Platform task .* at gs://.*bionic_y2.*",
        r"Started AI Platform task: https://console.cloud.google.com/ai-platform/jobs/.*bionic_y2.*",
        r"Submitting AI Platform task .*TaskKey\(dnode=EntityNode\(name='y2'\), case_key=CaseKey\(x1=1\)\).*",
        r"Computed   y2\(x1=1\) using AI Platform",
        r"Downloading y2\(x1=1\) from GCS \.\.\.",
        r"Computed   y3\(x1=1\)",
        r"Computed   y4\(x1=1\)",
        r"Computed   y5\(x1=1\)",
        r"Computed   total\(x1=1\)",
    )


@pytest.mark.no_parallel
def test_aip_fail(aip_builder, log_checker):
    builder = aip_builder

    builder.assign("x", 1)

    @builder
    @bn.aip_task_config("n1-standard-4")
    def x_plus_one(x):
        raise Exception()

    with pytest.raises(AipError):
        builder.build().get("x_plus_one")

    log_checker.expect_regex(
        r"Staging AI Platform task .* at gs://.*bionic_x_plus_one.*",
        r"Submitting AI Platform task on .*x_plus_one.*x=1.*",
        r"Started AI Platform task: https://console.cloud.google.com/ai-platform/jobs/.*bionic_x_plus_one.*",
        r"Submitting AI Platform task .*TaskKey\(dnode=EntityNode\(name='x_plus_one'\), case_key=CaseKey\(x=1\)\).*",
        r".*error while doing remote computation.*",
    )
