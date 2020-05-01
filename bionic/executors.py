"""
FIXME
"""

from concurrent.futures import Future, wait, FIRST_COMPLETED

from .optdep import import_optional_dependency

class BaseTaskCompletionExecutor:
    def resolve_any_future(self, futures):
        return futures

    def submit(self, task_state, task_key_logger):
        """
        FIXME: Submits.
        """
        return task_state.complete(task_key_logger)

    def sync_state_after_completion(self, task_state):
        pass


class ParallelTaskCompletionExecutor(BaseTaskCompletionExecutor):
    def __init__(self, max_workers):
        # Loky uses cloudpickle by default. We should investigate further what would
        # it take to make is use pickle and wrap the functions that are non-picklable
        # using `loky.wrap_non_picklable_objects`.
        loky = import_optional_dependency("loky", purpose="parallel processing")
        self._exec = loky.get_reusable_executor(max_workers=max_workers)

    def resolve_any_future(self, futures):
        successful_results = [future for future in futures if not isinstance(future, Future)]
        if successful_results:
            return successful_results

        finished_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
        return [future.result() for future in finished_futures]

    def submit(self, task_state, task_key_logger):
        # NOTE 1: This makes non-persisted entities compute here as well
        # as in executor pool. We need to keep track of what needs to be
        # computed in main process vs subprocess when entity is not persisted.
        # NOTE 2: Right now, non-persisted entities include simple lookup values
        # which we should not be really sending using IPC. We should read/write
        # a tmp file for this instead to use protocol for serialization instead of
        # using cloudpickle.
        if not task_state.provider.attrs.should_persist():
            return task_state.complete(task_key_logger)

        # NOTE 1: Logging support for multiple processes not done yet.
        new_state_for_subprocess = task_state.strip_state_for_subprocess()
        return self._exec.submit(new_state_for_subprocess.complete, task_key_logger)

    def sync_state_after_completion(self, task_state):
        if task_state.provider.attrs.should_persist():
            task_state.sync_after_subprocess_completion()
            
