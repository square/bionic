import blocks
import logging
import time
import googleapiclient.discovery
from concurrent.futures import Future as _Future, TimeoutError, CancelledError
from enum import Enum, auto


class AIPError(Exception):
    pass


class State(Enum):
    STATE_UNSPECIFIED = auto()
    QUEUED = auto()
    PREPARING = auto()
    RUNNING = auto()
    SUCCEEDED = auto()
    FAILED = auto()
    CANCELLING = auto()
    CANCELLED = auto()


EXECUTE = {State.STATE_UNSPECIFIED, State.QUEUED, State.PREPARING, State.RUNNING}
CANCEL = {State.CANCELLING, State.CANCELLED}
FAIL = {State.FAILED}


class Future(_Future):
    def __init__(self, project: str, job_id: str, output: str):
        self.project = project
        self.job_id = job_id
        self.output = output
        self.aip = googleapiclient.discovery.build("ml", "v1", cache_discovery=False)

    def name(self):
        return f"projects/{self.project}/jobs/{self.job_id}"

    def cancel(self):
        request = self.aip.projects().jobs().cancel(name=self.name())
        request.execute()
        return True

    def query(self):
        request = self.aip.projects().jobs().get(name=self.name())
        response = request.execute()
        return State[response["state"]], response.get("errorMessage", "")

    def cancelled(self):
        # TODO do we ever want to distinguish between canceling and cancelled on GCP?
        # at the moment we just trust that GCP will succesfully cancel once requested
        state, _ = self.query()
        return state in CANCEL

    def running(self):
        state, _ = self.query()
        return state in EXECUTE

    def done(self):
        state, _ = self.query()
        return state is State.SUCCEEDED or state in CANCEL

    def result(self, timeout: int = None):
        # TODO support other serializers
        exc = self.exception(timeout)
        if exc is not None:
            raise exc

        try:
            return blocks.unpickle(self.output)
        except ValueError:
            logging.warning(f"Failed to load output from succesful job at {self.path}")
            raise

    def exception(self, timeout: int = None):
        start = time.time()
        state, error = self.query()
        while state in EXECUTE:
            if timeout is not None and (time.time() - start) > timeout:
                raise TimeoutError(
                    f"{self.job_id} did not finish running before timeout of {timeout}s"
                )

            time.sleep(10)
            state, error = self.query()
            logging.info(f"Future for {self.job_id} has state {state}")

        if state in CANCEL:
            raise CancelledError(f"{self.job_id}: " + error)
        if state in FAIL:
            return AIPError(f"{self.job_id}: " + error)


class _TestFuture(Future):
    def query(self):
        return State.SUCCEEDED, ""
