import os
import subprocess
from textwrap import dedent

import pytest
from git import Repo

import bionic as bn

# This is detected by pytest and applied to all the tests in this module.
from bionic.aip.docker_image_builder import fix_pip_requirements
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
    @bn.run_in_aip("n1-standard-4")
    def y1(x1, x2, x3, x4, x5):
        return x1 + x2 + x3 + x4 + x5 + 1

    @builder
    @bn.run_in_aip("n1-standard-8")
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
        r"Submitting AI Platform task .*\(name='y1'\).*CaseKey\(x1=1\).*",
        r"Computed   y1\(x1=1\) using AI Platform",
        r"Downloading y1\(x1=1\) from GCS \.\.\.",
    )

    assert builder.build().get("total") == 90

    log_checker.expect_regex(
        r"Loaded     y1\(x1=1\) from disk cache",
        r"Staging AI Platform task .* at gs://.*bionic_y2.*",
        r"Started AI Platform task: https://console.cloud.google.com/ai-platform/jobs/.*bionic_y2.*",
        r"Submitting AI Platform task .*\(name='y2'\).*CaseKey\(x1=1\).*",
        r"Computed   y2\(x1=1\) using AI Platform",
        r"Downloading y2\(x1=1\) from GCS \.\.\.",
        r"Computed   y3\(x1=1\)",
        r"Computed   y4\(x1=1\)",
        r"Computed   y5\(x1=1\)",
        r"Computed   total\(x1=1\)",
    )


def test_aip_fail(aip_builder, log_checker):
    builder = aip_builder

    builder.assign("x", 1)

    @builder
    @bn.run_in_aip("n1-standard-4")
    def x_plus_one(x):
        raise Exception()

    with pytest.raises(AipError):
        builder.build().get("x_plus_one")

    log_checker.expect_regex(
        r"Staging AI Platform task .* at gs://.*bionic_x_plus_one.*",
        r"Started AI Platform task: https://console.cloud.google.com/ai-platform/jobs/.*bionic_x_plus_one.*",
        r"Submitting AI Platform task .*\(name='x_plus_one'\).*CaseKey\(x=1\).*",
        r".*error while doing remote computation for x_plus_one\(x=1\).*AipError.*",
    )


def test_fix_pip_requirements():
    pip_requirements = dedent(
        """
        Package1==1.2.3
        Package2==2
        -e git+git@github.com:square/bionic.git@f13f5405e928d92b553d2cbee41084eecccf7de3#egg=bionic
        -e git+https://github.com/square/bionic.git@88fec3d6921ed13b7c7575cca4c292b4f7003b9c#egg=bionic
    """
    )

    fixed_pip_requirements = dedent(
        """
        Package1==1.2.3
        Package2==2
        -e git+https://github.com/square/bionic.git@f13f5405e928d92b553d2cbee41084eecccf7de3#egg=bionic
        -e git+https://github.com/square/bionic.git@88fec3d6921ed13b7c7575cca4c292b4f7003b9c#egg=bionic
    """
    )

    assert fix_pip_requirements(pip_requirements) == fixed_pip_requirements


def git_repo_is_clean_and_pushed():
    repo = Repo(os.getcwd(), search_parent_directories=True)
    return (
        not repo.is_dirty()
        and len(repo.git.branch("-r", "--contains", repo.head.ref.object.hexsha)) > 0
    )


@pytest.mark.skipif(
    not git_repo_is_clean_and_pushed(),
    reason="current commit needs to be available in remote repo for docker access",
)
@pytest.mark.real_gcp_only
@pytest.mark.no_parallel
def test_aip_with_docker_build(aip_builder):
    builder = aip_builder
    builder.set("core__aip_execution__docker_image_name", None)

    def get_pip_freeze_exclude_editable() -> str:
        # pip freeze may not work properly for editable installs when running in
        # AIP since AIP does not have access to remote git repositories. Hence
        # editable installs are excluded.
        return subprocess.run(
            ["pip", "freeze", "--exclude-editable"],
            capture_output=True,
            check=True,
            encoding="utf-8",
        ).stdout

    @builder
    @bn.run_in_aip("n1-standard-4")
    def x():
        return get_pip_freeze_exclude_editable()

    flow = builder.build()

    assert flow.get("x") == get_pip_freeze_exclude_editable()
