"""
This module contains the logic to execute tasks and their dependencies
to completion.
"""

from collections import defaultdict
from concurrent.futures import wait, FIRST_COMPLETED

import logging

from .task_execution import EntryBlockage, EntryStage, TaskRunnerEntry
from ..utils.misc import SynchronizedSet


# TODO At some point it might be good to have the option of Bionic handling its
# own logging.  Probably it would manage its own logger instances and inject
# them into tasks, while providing the option of either handling the output
# itself or routing it back to the global logging system.
logger = logging.getLogger(__name__)


def run_in_subprocess(task_completion_runner, state):
    task_completion_runner.run([state])
    return state.task.keys[0]


class TaskCompletionRunner:
    """
    Computes a DAG of `TaskState` objects.

    Given a collection of TaskStates, this class identifies and performs the necessary
    work to compute their final values. With a simple directed acyclic graph of tasks,
    this would be a simple as doing a topological sort and then computing the tasks
    in order. However, there are several complicating factors:

    - Some tasks should be run asynchronously in a separate process to allow
      parallelism.
    - Some task outputs can't be serialized, which means those tasks need to be
      re-computed in each process that needs them.
    - If a task's value is cached, we want to load it as quickly as possible, without
      computing any unnecessary dependencies. (However, sometimes we do need to compute
      dependencies in order to determine if the cached value is up-to-date or not.)

    To deal with these, for each `TaskState` we maintain a `TaskRunnerEntry` which
    tracks what is required for that particular task. Each entry has an `EntryStage`
    enum, indicating whether we need to and/or can do more work on it.
    """

    def __init__(self, bootstrap, flow_instance_uuid, task_key_logger):
        # These are needed to complete entries.
        self._bootstrap = bootstrap
        self._flow_instance_uuid = flow_instance_uuid
        self.task_key_logger = task_key_logger

        # These are used for caching and tracking.
        self._entries_by_task_key = {}
        self._pending_entries = []
        self._in_progress_entries = {}
        self._blockage_lists_by_blocking_tk = defaultdict(list)

    @property
    def _parallel_execution_enabled(self):
        return self._bootstrap is not None and self._bootstrap.executor is not None

    def run(self, states):
        try:
            if self._parallel_execution_enabled:
                self._bootstrap.executor.start_logging()

            for state in states:
                state.set_up_caching_flags(self._bootstrap)
                entry = self._get_or_create_entry_for_state(state)
                self._request_entry_be_cached(entry)

            while self._has_pending_entries():
                entry = self._activate_next_pending_entry()
                self._process_active_entry(entry)
                assert entry.stage != EntryStage.ACTIVE

            assert len(self._pending_entries) == 0
            assert len(self._in_progress_entries) == 0
            assert len(self._get_all_blocked_entries()) == 0

            results = {}
            for state in states:
                task_key = state.task_keys[0]
                entry = self._entries_by_task_key[task_key]
                results[task_key] = entry.get_cached_results(self.task_key_logger)
            return results

        finally:
            if self._parallel_execution_enabled:
                self._bootstrap.executor.stop_logging()

    def _process_active_entry(self, entry):
        """
        Takes the current active entry and does the minimal amount of work to move it
        to the next stage. When this function returns, the entry will no longer be in
        the ACTIVE stage: it will be COMPLETED, IN_PROGRESS, or BLOCKED, and we will
        be able to move on to the next entry.
        """

        assert entry.stage == EntryStage.ACTIVE

        # In theory we could have an entry that doesn't require priming, but in practice
        # we always require it when we create one.
        assert entry.is_requested_to_be_primed

        # First, if we already have a cached value, we can stop immediately without
        # needing to process any dependencies.
        if entry.is_cached:
            self._mark_entry_completed(entry)
            return

        # Otherwise, we'll need all of our dependencies to be primed so that their
        # digests are available.
        self._set_up_entry_dependencies(entry)
        for dep_entry in entry.dep_entries:
            self._request_entry_be_primed(dep_entry)
        if not entry.all_dependencies_satisfy_requests:
            self._mark_entry_blocked(entry)
            return

        entry.state.initialize(self._bootstrap, self._flow_instance_uuid)

        # If this entry is persistable, let's see if we can load it from the cache ...
        if entry.state.should_persist:
            entry.state.attempt_to_access_persistent_cached_values()
            if entry.is_cached:
                self._mark_entry_completed(entry)
                return
        else:
            # ... otherwise, if the digest was all we needed, calling initialize() was
            # sufficient.
            if not entry.is_requested_to_be_cached:
                self._mark_entry_completed(entry)
                return

        # If possible, we prefer to compute entries remotely.
        can_compute_remotely = (
            self._parallel_execution_enabled
            and entry.state.should_persist
            and not entry.state.task.is_simple_lookup
        )
        if can_compute_remotely:
            new_state_for_subprocess = entry.state.strip_state_for_subprocess()
            # We want serial execution in the subprocesses.
            new_bootstrap = self._bootstrap.evolve(executor=None)
            new_task_completion_runner = TaskCompletionRunner(
                bootstrap=new_bootstrap,
                flow_instance_uuid=self._flow_instance_uuid,
                task_key_logger=self.task_key_logger,
            )
            future = self._bootstrap.executor.submit(
                run_in_subprocess, new_task_completion_runner, new_state_for_subprocess,
            )
            self._mark_entry_in_progress(entry, future)
            return

        # Otherwise we'll compute this entry locally. In that case, it's not enough
        # for our dependencies to be primed; they also need to have cached values.
        for dep_entry in entry.dep_entries:
            self._request_entry_be_cached(dep_entry)
        if not entry.all_dependencies_satisfy_requests:
            self._mark_entry_blocked(entry)
            return
        entry.compute(self.task_key_logger)
        self._mark_entry_completed(entry)

    def _set_up_entry_dependencies(self, entry):
        if entry.dep_entries is not None:
            return

        dep_entries = []
        for dep_state in entry.state.dep_states:
            dep_entry = self._get_or_create_entry_for_state(dep_state)
            dep_entries.append(dep_entry)
        entry.dep_entries = dep_entries

    def _request_entry_be_primed(self, entry):
        entry.is_requested_to_be_primed = True
        if entry.stage == EntryStage.COMPLETED and not entry.is_primed:
            self._mark_entry_pending(entry)

    def _request_entry_be_cached(self, entry):
        self._request_entry_be_primed(entry)

        entry.is_requested_to_be_cached = True
        if entry.stage == EntryStage.COMPLETED and not entry.is_cached:
            self._mark_entry_pending(entry)

    def _get_or_create_entry_for_state(self, state):
        task_key = state.task_keys[0]
        if task_key in self._entries_by_task_key:
            return self._entries_by_task_key[task_key]
        # Before doing anything with this task state, we should make sure its
        # cache state is up to date.
        state.refresh_all_persistent_cache_state(self._bootstrap)
        entry = TaskRunnerEntry(state)
        self._entries_by_task_key[task_key] = entry
        return entry

    def _has_pending_entries(self):
        # While there are no entries in the to-process stack but have any in-progress ones,
        # let's wait for in-progress entries to finish till we find an entry to process or
        # exhaust all in-progress entries.
        while len(self._pending_entries) == 0 and len(self._in_progress_entries) != 0:
            self._wait_on_in_progress_entries()
        return len(self._pending_entries) != 0

    def _activate_next_pending_entry(self):
        assert len(self._pending_entries) != 0
        next_entry = self._pending_entries.pop()
        assert next_entry.stage == EntryStage.PENDING

        next_entry.stage = EntryStage.ACTIVE
        return next_entry

    def _wait_on_in_progress_entries(self):
        "Waits on any in-progress entry to finish."
        futures = [entry.future for entry in self._in_progress_entries.values()]
        finished_futures, _ = wait(futures, return_when=FIRST_COMPLETED)
        for finished_future in finished_futures:
            task_key = finished_future.result()
            entry = self._entries_by_task_key[task_key]
            entry.state.sync_after_subprocess_computation()
            self._mark_entry_completed(entry)

    def _mark_entry_pending(self, pending_entry):
        assert pending_entry.stage in (EntryStage.BLOCKED, EntryStage.COMPLETED)

        pending_entry.stage = EntryStage.PENDING
        self._pending_entries.append(pending_entry)

    def _mark_entry_blocked(self, blocked_entry):
        assert blocked_entry.stage == EntryStage.ACTIVE

        blocked_entry.stage = EntryStage.BLOCKED
        blocking_tks = blocked_entry.blocking_dep_task_keys()
        blockage = EntryBlockage(blocked_entry, blocking_tks)
        for blocking_tk in blocking_tks:
            self._blockage_lists_by_blocking_tk[blocking_tk].append(blockage)

        for blocking_entry in blocked_entry.blocking_dep_entries():
            assert blocking_entry.stage != EntryStage.COMPLETED

    def _mark_entry_in_progress(self, in_progress_entry, future):
        assert in_progress_entry.stage == EntryStage.ACTIVE
        assert in_progress_entry.future is None
        assert in_progress_entry not in self._in_progress_entries

        in_progress_entry.stage = EntryStage.IN_PROGRESS
        in_progress_entry.future = future
        self._in_progress_entries[
            in_progress_entry.state.task.keys[0]
        ] = in_progress_entry

    def _mark_entry_completed(self, completed_entry):
        assert completed_entry.stage in [EntryStage.ACTIVE, EntryStage.IN_PROGRESS]

        if completed_entry.stage == EntryStage.IN_PROGRESS:
            completed_entry.future = None
            del self._in_progress_entries[completed_entry.state.task.keys[0]]

        assert completed_entry.satisfies_requests
        completed_entry.stage = EntryStage.COMPLETED
        self._unblock_entries(completed_entry)

    def _unblock_entries(self, completed_entry):
        for completed_tk in completed_entry.state.task_keys:
            affected_blockages = self._blockage_lists_by_blocking_tk[completed_tk]
            for blockage in affected_blockages:
                blockage.mark_task_key_complete(completed_tk)
                if (
                    blockage.is_resolved()
                    and blockage.blocked_entry.stage == EntryStage.BLOCKED
                ):
                    self._mark_entry_pending(blockage.blocked_entry)

    def _get_all_blocked_entries(self):
        return {
            blockage.blocked_entry
            for blockages in self._blockage_lists_by_blocking_tk.values()
            for blockage in blockages
            if not blockage.is_resolved()
        }


class TaskKeyLogger:
    """
    Logs how we derived each task key. The purpose of this class is to make sure that
    each task key used in a derivation (i.e., a call to `Flow.get()`) is logged exactly
    once. (One exception: a task key can be logged twice to indicate the start and end
    of a computation.)
    """

    def __init__(self, bootstrap):
        self._level = logging.INFO if bootstrap is not None else logging.DEBUG

        executor = bootstrap.executor if bootstrap is not None else None
        if executor is not None:
            self._already_logged_task_key_set = executor.create_synchronized_set()
        else:
            self._already_logged_task_key_set = SynchronizedSet()

    def _log(self, template, task_key, is_resolved=True):
        if not is_resolved:
            should_log = not self._already_logged_task_key_set.contains(task_key)
        else:
            should_log = self._already_logged_task_key_set.add(task_key)

        if should_log:
            logger.log(self._level, template, task_key)

    def log_accessed_from_memory(self, task_key):
        self._log("Accessed   %s from in-memory cache", task_key)

    def log_accessed_from_definition(self, task_key):
        self._log("Accessed   %s from definition", task_key)

    def log_loaded_from_disk(self, task_key):
        self._log("Loaded     %s from disk cache", task_key)

    def log_computing(self, task_key):
        self._log("Computing  %s ...", task_key, is_resolved=False)

    def log_computed(self, task_key):
        self._log("Computed   %s", task_key)
