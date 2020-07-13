"""
This module contains the logic to execute tasks and their dependencies
to completion.
"""

from collections import defaultdict
from concurrent.futures import wait, FIRST_COMPLETED

import logging

from .task_execution import EntryBlockage, EntryStage, TaskRunnerEntry
from ..util import SynchronizedSet


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
    Runs `TaskState` to completion.

    Using a `Bootstrap` object, this class completes given task states
    using a stack-based state tracking approach.
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
                entry = self._get_or_create_entry_for_state(
                    state, is_needed_in_memory=not state.should_persist
                )
                self._mark_entry_pending(entry)

            while self._has_pending_entries():
                entry = self._activate_next_pending_entry()

                # Before we decide if the current entry is blocked, we need to create
                # an entry for each of its dependencies. This has the side effect of
                # refreshing each of their cache states, so if any of them have had
                # their cache entries deleted, their status will be correctly updated
                # to "not complete".
                dep_entries = []
                for dep_state in entry.state.dep_states:
                    dep_state.set_up_caching_flags(self._bootstrap)
                    # If the entry is needed in memory, any non-serializable entries are
                    # also needed in memory.
                    dep_entry_needed_in_memory = (
                        entry.is_needed_in_memory and not dep_state.should_persist
                    )
                    dep_entry = self._get_or_create_entry_for_state(
                        dep_state, dep_entry_needed_in_memory
                    )
                    dep_entries.append(dep_entry)
                if not entry.are_dep_entries_set():
                    entry.set_dep_entries(dep_entries)

                if entry.state.is_cached:
                    self._mark_entry_completed(entry)
                    continue

                if len(entry.blocking_dep_entries()) > 0:
                    self._mark_entry_blocked(entry)
                    continue

                self._process_entry(entry)

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

    def _process_entry(self, entry):
        assert entry.stage == EntryStage.ACTIVE

        state = entry.state
        # Initialize the task state before attempting to complete it.
        state.initialize(self._bootstrap, self._flow_instance_uuid)

        # Attempt to complete the task state from persistence cache.
        state.attempt_to_complete_from_cache()
        if state.is_cached:
            self._mark_entry_completed(entry)

        elif (
            not self._parallel_execution_enabled
            or entry.is_needed_in_memory
            # This is a simple lookup task that looks up a value in a dictionary.
            # We can't run this in a separate process because the value may not be
            # cloudpicklable.
            or state.task.is_simple_lookup
        ):
            entry.compute(self.task_key_logger)
            self._mark_entry_completed(entry)

        # Compute the results for serializable entity in parallel.
        elif state.should_persist:
            new_state_for_subprocess = state.strip_state_for_subprocess()
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

        # Do not compute non-serializable entity in parallel. Any entity that
        # depends on this entity will compute it in the subprocess.
        else:
            self._mark_entry_deferred(entry)

    def _get_or_create_entry_for_state(self, state, is_needed_in_memory):
        task_key = state.task_keys[0]
        if task_key not in self._entries_by_task_key:
            # Before doing anything with this task state, we should make sure its
            # cache state is up to date.
            state.refresh_all_persistent_cache_state(self._bootstrap)
            self._entries_by_task_key[task_key] = TaskRunnerEntry(
                state=state, is_needed_in_memory=is_needed_in_memory,
            )
        entry = self._entries_by_task_key[task_key]

        if is_needed_in_memory:
            entry.is_needed_in_memory = is_needed_in_memory
            if entry.stage == EntryStage.DEFERRED:
                self._mark_entry_pending(entry)

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
            entry.state.sync_after_subprocess_completion()
            self._mark_entry_completed(entry)

    def _mark_entry_pending(self, pending_entry):
        assert pending_entry.stage in (EntryStage.NEW, EntryStage.BLOCKED)

        pending_entry.stage = EntryStage.PENDING
        self._pending_entries.append(pending_entry)

    def _mark_entry_blocked(self, blocked_entry):
        assert blocked_entry.stage == EntryStage.ACTIVE

        blocked_entry.stage = EntryStage.BLOCKED
        blocking_tks = blocked_entry.blocking_dep_task_keys()
        blockage = EntryBlockage(blocked_entry, blocking_tks)
        for blocking_tk in blocking_tks:
            self._blockage_lists_by_blocking_tk[blocking_tk].append(blockage)

        self._mark_blocking_entries_pending(blocked_entry)

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

        assert completed_entry.is_cached
        completed_entry.stage = EntryStage.COMPLETED
        self._unblock_entries(completed_entry)

    def _mark_entry_deferred(self, deferred_entry):
        assert deferred_entry.stage == EntryStage.ACTIVE
        assert not deferred_entry.is_needed_in_memory

        deferred_entry.stage = EntryStage.DEFERRED
        self._unblock_entries(deferred_entry)

    def _mark_blocking_entries_pending(self, blocked_entry):
        blocking_entries = blocked_entry.blocking_dep_entries()
        for blocking_entry in blocking_entries:
            if blocking_entry.stage == EntryStage.NEW:
                self._mark_entry_pending(blocking_entry)

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
