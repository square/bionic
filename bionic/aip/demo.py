""" # TODO this is for short term testing, will be removed before merge
"""
import logging
import os
import time
from bionic.aip.task import Job, Resource, Task
from datetime import datetime
from functools import partial


def x():
    return 3


def y(v):
    return v * 2


def main():
    logging.basicConfig(level=logging.INFO)
    job = Job(
        uuid=f"demo_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
        project=os.environ["GCP_PROJECT"],
        image="bionic:latest",
    )

    futures = []
    futures.append(
        Task(
            name="x", job=job, resource=Resource("n1-standard-4"), function=x,
        ).submit()
    )
    futures.append(
        Task(
            name="y",
            job=job,
            resource=Resource("n1-standard-4"),
            function=partial(y, 2),
        ).submit()
    )

    while any(future.running() for future in futures):
        time.sleep(10)

    print(sum(future.result() for future in futures))


if __name__ == "__main__":
    main()
