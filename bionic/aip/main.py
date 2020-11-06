"""
This module is run as main in order to execute a task on a worker.
"""

import logging
import pickle
import os
import sys

from bionic.deps.optdep import import_optional_dependency
from bionic.gcs import get_gcs_fs_without_warnings


def _run(ipath, gcs_fs):
    cloudpickle = import_optional_dependency("cloudpickle")

    with gcs_fs.open(ipath, "rb") as f:
        task = cloudpickle.load(f)

    # Now that we have the task, set up logging.
    _set_up_logging(task.job_id(), task.config.project_name)
    logging.info(f"Read task from {ipath}")

    result = task.function()

    opath = task.output_uri()
    logging.info(f"Uploading result to {opath}")
    with gcs_fs.open(opath, "wb") as f:
        pickle.dump(result, f)


# Main entry point for AIP
def run():
    """
    This method is a proxy to _run which does the actual work. The proxy exists
    so that _run can be replaced for testing.
    """
    _run(sys.argv[-1], get_gcs_fs_without_warnings())


def _set_up_logging(job_id, project_id):
    if os.environ.get("BIONIC_NO_STACKDRIVER", False):
        return

    # TODO This is the ID of the hyperparameter tuning trial currently
    # running on this VM. This field is only set if the current
    # training job is a hyperparameter tuning job. Conductor uses this
    # environment variable but AIP documentation suggests us to use
    # TF_CONFIG. Check whether we need to update this env variable.
    # Find more details on TF_CONFIG at this link:
    # https://cloud.google.com/ai-platform/training/docs/distributed-training-details
    trial_id = os.environ.get("CLOUD_ML_TRIAL_ID", None)

    glogging = import_optional_dependency("google.cloud.logging")

    client = glogging.Client(project=project_id)
    resource = glogging.resource.Resource(
        type="ml_job",
        # AIP expects a default task_name for the master cluster. We
        # use a placeholder value till we start using clusters. Once we
        # do, it should be configured based on the cluster.
        labels=dict(job_id=job_id, project_id=project_id, task_name="master-replica-0"),
    )
    labels = None
    if trial_id is not None:
        # Enable grouping by trial when present.
        labels = {"ml.googleapis.com/trial_id": trial_id}

    # Enable only the cloud logger to avoid duplicate messages.
    handler = glogging.handlers.handlers.CloudLoggingHandler(
        client, resource=resource, labels=labels
    )
    root_logger = logging.getLogger()
    # Remote the StreamHandler. Any logs logged by it shows up as error
    # logs in Stackdriver.
    root_logger.handlers = []
    # We should ideally make this configurable, but till then, let's
    # set the level to DEBUG to write all the logs. It's not hard to
    # filter using log level on Stackdriver so it doesn't create too
    # much noise anyway.
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(handler)
    for logger_name in glogging.handlers.handlers.EXCLUDED_LOGGER_DEFAULTS:
        logging.getLogger(logger_name).propagate = False


if __name__ == "__main__":
    run()
