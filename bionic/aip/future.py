import logging
import pickle
import time
from concurrent.futures import Future as ConcurrentFuture, TimeoutError, CancelledError
from enum import Enum, auto

from bionic.aip.client import get_aip_client


class AipError(Exception):
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

    def is_executing(self):
        return self in {
            State.STATE_UNSPECIFIED,
            State.QUEUED,
            State.PREPARING,
            State.RUNNING,
        }

    def is_cancelled(self):
        return self in {State.CANCELLING, State.CANCELLED}

    def is_finished(self):
        return self in {
            State.SUCCEEDED,
            State.FAILED,
            State.CANCELLING,
            State.CANCELLED,
        }


class Future(ConcurrentFuture):
    """This future represents a job running on AI platform

    The result of the running job will be pickled, and this future can load that pickle.
    You can find more information about the job details and states at
    https://cloud.google.com/ai-platform/training/docs/reference/rest/v1/projects.jobs

    """

    def __init__(self, gcs_fs, project_name: str, job_id: str, output: str):
        self.gcs_fs = gcs_fs
        self.project_name = project_name
        self.job_id = job_id
        self.output = output
        self.aip = get_aip_client()
        self.done_callbacks = []

        # These are used for caching of the results when the remote job is
        # finished.
        self.state = None
        self.error = None

    @property
    def name(self):
        return f"projects/{self.project_name}/jobs/{self.job_id}"

    def cancel(self):
        request = self.aip.projects().jobs().cancel(name=self.name)
        request.execute()
        return True

    def _get_state_and_error(self):
        if self.state is not None:
            assert self.state.is_finished()
            return self.state, self.error

        request = self.aip.projects().jobs().get(name=self.name)
        response = request.execute()
        state, error = State[response["state"]], response.get("errorMessage", "")

        if state.is_finished():
            self.state = state
            self.error = error
            for fn in self.done_callbacks:
                fn(self)
            self.done_callbacks.clear()

        return state, error

    def cancelled(self):
        # We may want to distinguish between canceling and cancelled on GCP.
        # At the moment we trust that GCP will always succesfully cancel once requested.
        state, _ = self._get_state_and_error()
        return state.is_cancelled()

    def running(self):
        state, _ = self._get_state_and_error()
        return state.is_executing()

    def done(self):
        state, _ = self._get_state_and_error()
        return state.is_finished()

    def result(self, timeout: int = None):
        # This will need an update to support other serializers.
        exc = self.exception(timeout)
        if exc is not None:
            raise exc

        try:
            with self.gcs_fs.open(self.output, "rb") as f:
                return pickle.load(f)
        except:  # NOQA
            logging.warning(
                f"Failed to load output from succesful job at {self.output}"
            )
            raise

    def exception(self, timeout: int = None):
        start = time.time()
        state, error = self._get_state_and_error()
        while state.is_executing():
            if timeout is not None and (time.time() - start) > timeout:
                raise TimeoutError(
                    f"{self.job_id} did not finish running before timeout of {timeout}s"
                )

            time.sleep(10)
            state, error = self._get_state_and_error()
            logging.info(f"Future for {self.job_id} has state {state}")

        if state.is_cancelled():
            raise CancelledError(f"{self.job_id}: " + error)
        if state is State.FAILED:
            return AipError(f"{self.job_id}: " + error)

    def add_done_callback(self, fn):
        if self.done():
            fn(self)
        else:
            self.done_callbacks.append(fn)
