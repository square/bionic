from functools import partial
import logging
from uuid import uuid4

from bionic.aip.future import Future as AipFuture
from bionic.aip.task import Task as AipTask


class FakeAipFuture(AipFuture):
    """
    A mock implementation of Future that finished successfully.
    """

    def __init__(self, project_name: str, job_id: str, output: str):
        self.project_name = project_name
        self.job_id = job_id
        self.output = output

    def _get_state_and_error(self):
        from bionic.aip.future import State

        return State.SUCCEEDED, ""


class FakeAipTask(AipTask):
    """
    A mock implementation of Task that runs the job locally using a
    subprocess instead of running it on AIP.
    """

    def submit(self) -> AipFuture:
        self._stage()
        spec = self._ai_platform_job_spec()

        logging.info(f"Submitting test {self.config.project_name}: {self}")
        import subprocess

        subprocess.check_call(spec["trainingInput"]["args"])
        return FakeAipFuture(self.config.project_name, self.job_id(), self.output_uri())


class FakeAipExecutor:
    """
    A mock version of AipExecutor to submit FakeTasks that uses
    subprocess to execute the tasks instead of using AIP. Useful for
    running tests locally without using AIP.
    """

    def __init__(self, aip_config):
        self._aip_config = aip_config

    def submit(self, task_config, fn, *args, **kwargs):
        return FakeAipTask(
            name="a" + str(uuid4()).replace("-", ""),
            config=self._aip_config,
            task_config=task_config,
            function=partial(fn, *args, **kwargs),
        ).submit()
