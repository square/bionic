""" Data model for task running on AI platform
"""
import logging
import os
import sys
from dataclasses import dataclass
from typing import Callable, Optional

import blocks
import cloudpickle
import googleapiclient.discovery
from bionic.aip.future import Future
from bionic.aip.tune import Tune


@dataclass(frozen=True)
class Resource:
    """ Description of a computing resource, which can be a single machine or a cluster of machines

    In the future could be extended to support GPUs and similar
    """

    machine: str
    worker_count: Optional[int] = None
    worker_machine: Optional[str] = None


@dataclass(frozen=True)
class Job:
    """ A job groups together individual tasks, all of which share these properties

    #TODO some square specific defaults in here, couldn't leave those in bionic

    Attributes
    ----------
    uuid: str
        The globally unique job name on AI platform
    image: str
        The name of the docker image for the tasks
    project: str
        The GCP project where the jobs will be run

    """

    uuid: str
    image: str
    project: str
    account: str = None
    network: str = None

    def __post_init__(self):
        if self.account is None:
            # NOTE we must set an account or job submission will fail
            object.__setattr__(
                self,
                "account",
                f"ai-platform-agent@{self.project}.iam.gserviceaccount.com",
            )
        if "gcr.io" not in self.image:
            # NOTE this is just for convenient image shorthand
            object.__setattr__(self, "image", f"gcr.io/{self.project}/{self.image}")


@dataclass(frozen=True)
class Task:
    name: str
    job: Job
    function: Callable
    resource: Resource
    tune: Optional[Tune] = None

    def job_id(self):
        return f"{self.job.uuid}_{self.name}"

    def path_inputs(self):
        # TODO bionic will need to customize
        return f"gs://{self.job.project}/bionic/{self.job.uuid}/{self.name}-inputs.pkl"

    def path_output(self):
        # TODO bionic will need to customize
        return f"gs://{self.job.project}/bionic/{self.job.uuid}/{self.name}-output.pkl"

    def _stage(self):
        path = self.path_inputs()
        logging.info(f"Staging task {self.name} at {path}")
        with blocks.filesystem.GCSNativeFileSystem().open(path, "wb") as f:
            cloudpickle.dump(self, f)

    def _ai_platform_job_spec(self):
        """ Conversion from our task data model to a job request on ai platform
        """
        output = {
            "jobId": f"{self.job.uuid}_{self.name}",
            "labels": {"job": self.job.uuid},
            "trainingInput": {
                "serviceAccount": self.job.account,
                "masterType": self.resource.machine,
                "masterConfig": {"imageUri": self.job.image},
                "args": ["python", "-m", "bionic.aip.task", self.path_inputs()],
                "packageUris": [],
                "region": "us-west1",
                "pythonModule": "",
                "scaleTier": "CUSTOM",
            },
        }
        if self.job.network is not None:
            output["trainingInput"]["network"] = self.job.network

        if self.resource.worker_count is not None and self.resource.worker_count > 0:
            output["trainingInput"]["workerCount"] = self.resource.worker_count
            output["trainingInput"]["workerType"] = self.resource.worker_machine
            output["trainingInput"]["workerConfig"] = {"imageUri": self.job.image}

        if self.tune is not None:
            output["trainingInput"]["hyperparameters"] = self.tune.spec()

        return output

    def submit(self) -> Future:
        # TODO tune doesn't really work as intended yet, need to alter the future to read the tune
        self._stage()
        spec = self._ai_platform_job_spec()

        if os.environ.get("TEST", "").lower() == "true":
            logging.info(f"Submitting test {self.job.project}: {self}")
            import subprocess
            from bionic.aip.future import _TestFuture

            env = os.environ.copy()
            env["CLOUD_ML_JOB_ID"] = spec["jobId"]
            subprocess.check_call(spec["trainingInput"]["args"], env=env)
            return _TestFuture(self.job.project, self.job_id(), self.path_output())

        aip = googleapiclient.discovery.build("ml", "v1", cache_discovery=False)
        logging.info(f"Submitting {self.job.project}: {self}")

        request = (
            aip.projects()
            .jobs()
            .create(body=spec, parent=f"projects/{self.job.project}")
        )
        request.execute()
        url = f'https://console.cloud.google.com/ai-platform/jobs/{spec["jobId"]}'
        logging.info(f"Started task on AI Platform: f{url}")
        return Future(self.job.project, self.job_id(), self.path_output())


def run():
    # TODO stackdriver logging
    ipath = sys.argv[-1]
    logging.info(f"Reading task from {ipath}")
    fs = blocks.filesystem.GCSNativeFileSystem()
    with fs.open(ipath, "rb") as f:
        task = cloudpickle.load(f)

    logging.info("Starting execution")
    result = task.function()

    opath = task.path_output()
    logging.info(f"Uploading result to {opath}")
    blocks.pickle(result, opath)
    return


if __name__ == "__main__":
    # This module is run as main in order to execute a task on a worker
    run()
