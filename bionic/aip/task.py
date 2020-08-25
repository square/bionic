"""
Data model for task running on AI platform
"""
import logging
import sys
import attr
from typing import Callable, Optional

from bionic.deps.optdep import import_optional_dependency
from bionic.aip.future import Future


@attr.s(auto_attribs=True)
class Resource:
    """Description of a computing resource, which can be a single machine or a cluster of machines

    In the future could be extended to support GPUs and similar
    """

    machine: str
    worker_count: Optional[int] = None
    worker_machine: Optional[str] = None


@attr.s(auto_attribs=True)
class Job:
    """A job groups together individual tasks, all of which share these properties

    Attributes
    ----------
    uuid: str
        The globally unique job name on AI platform
    image_uri: str
        The full address on gcr of the docker image for the tasks
    project: str
        The GCP project where the jobs will be run

    """

    uuid: str
    image_uri: str
    project: str
    account: Optional[str] = None
    network: Optional[str] = None


@attr.s(auto_attribs=True)
class Task:
    name: str
    job: Job
    function: Callable
    resource: Resource

    def job_id(self):
        return f"{self.job.uuid}_{self.name}"

    def inputs_uri(self):
        # In a future version it might be better to make this path specified by the flow,
        # so that it can be inside a GCS cache location and different file types
        return f"gs://{self.job.project}/bionic/{self.job.uuid}/{self.name}-inputs.cloudpickle"

    def output_uri(self):
        # In a future version it might be better to make this path specified by the flow,
        # so that it can be inside a GCS cache location and different file types
        return f"gs://{self.job.project}/bionic/{self.job.uuid}/{self.name}-output.cloudpickle"

    def _ai_platform_job_spec(self):
        """Conversion from our task data model to a job request on ai platform"""
        output = {
            "jobId": f"{self.job.uuid}_{self.name}",
            "labels": {"job": self.job.uuid},
            "trainingInput": {
                "serviceAccount": self.job.account,
                "masterType": self.resource.machine,
                "masterConfig": {"imageUri": self.job.image_uri},
                "args": ["python", "-m", "bionic.aip.task", self.inputs_uri()],
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
            output["trainingInput"]["workerConfig"] = {"imageUri": self.job.image_uri}

        return output


def _stage(task):
    # Scope the blocks import to this function to avoid raising for anyone not using it.
    blocks = import_optional_dependency("blocks", raise_on_missing=True)
    cloudpickle = import_optional_dependency("cloudpickle", raise_on_missing=True)

    path = task.inputs_uri()
    logging.info(f"Staging task {task.name} at {path}")
    with blocks.filesystem.GCSNativeFileSystem().open(path, "wb") as f:
        cloudpickle.dump(task, f)


def submit(task) -> Future:
    # Scope the import to this function to avoid raising for anyone not using it.
    discovery = import_optional_dependency(
        "googleapiclient.discovery", raise_on_missing=True
    )

    task._stage()
    spec = task._ai_platform_job_spec()

    aip = discovery.build("ml", "v1", cache_discovery=False)
    logging.info(f"Submitting {task.job.project}: {task}")

    request = (
        aip.projects().jobs().create(body=spec, parent=f"projects/{task.job.project}")
    )
    request.execute()
    url = f'https://console.cloud.google.com/ai-platform/jobs/{spec["jobId"]}'
    logging.info(f"Started task on AI Platform: {url}")
    return Future(task.job.project, task.job_id(), task.output_uri())


def run():
    # Scope the import to this function to avoid raising for anyone not using it.
    blocks = import_optional_dependency("blocks", raise_on_missing=True)
    cloudpickle = import_optional_dependency("cloudpickle", raise_on_missing=True)

    # We should add stackdriver logging to this in a future version.
    ipath = sys.argv[-1]
    logging.info(f"Reading task from {ipath}")
    fs = blocks.filesystem.GCSNativeFileSystem()
    with fs.open(ipath, "rb") as f:
        task = cloudpickle.load(f)

    logging.info("Starting execution")
    result = task.function()

    opath = task.output_uri()
    logging.info(f"Uploading result to {opath}")
    blocks.pickle(result, opath)
    return


if __name__ == "__main__":
    # This module is run as main in order to execute a task on a worker
    run()
