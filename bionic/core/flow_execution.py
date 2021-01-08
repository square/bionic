"""
This module contains the logic to execute tasks and their dependencies
to completion.
"""

import logging
from concurrent.futures import wait, FIRST_COMPLETED, ALL_COMPLETED

import attr

from .task_execution import (
    EntryLevel,
    EntryPriority,
    EntryStage,
    EntryRequirement,
    RemoteSubgraph,
    TaskRunnerEntry,
)
from ..exception import AttributeValidationError
from ..utils.keyed_priority_stack import KeyedPriorityStack
from ..utils.misc import oneline, SynchronizedSet


# TODO At some point it might be good to have the option of Bionic handling its
# own logging.  Probably it would manage its own logger instances and inject
# them into tasks, while providing the option of either handling the output
# itself or routing it back to the global logging system.
logger = logging.getLogger(__name__)


def run_in_subprocess(task_completion_runner, states):
    task_completion_runner.run(states)
    return [state.task_key for state in states]


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

    def __init__(self, context):
        # These are needed to complete entries.
        self._context = context

        # These are used for caching and tracking.
        self._entries_by_task_key = {}
        self._pending_entries_kps = KeyedPriorityStack()
        self._in_progress_entries = set()
        self._blocked_entries = set()

    @property
    def _parallel_execution_enabled(self):
        return self._context.core.process_executor is not None

    @property
    def _aip_execution_enabled(self):
        return self._context.core.aip_executor is not None

    def run(self, states):
        try:
            if self._parallel_execution_enabled:
                self._context.core.process_executor.start_logging()

            for state in states:
                entry = self._get_or_create_entry_for_state(state)
                self._add_requirement(
                    dst_entry=entry,
                    level=EntryLevel.CACHED,
                    expiration=EntryRequirement.Expiration.NEVER,
                )

            while self._has_pending_entries():
                entry = self._activate_next_pending_entry()
                self._process_active_entry(entry)
                assert entry.stage != EntryStage.ACTIVE, entry

            assert len(self._pending_entries_kps) == 0
            assert len(self._in_progress_entries) == 0
            assert len(self._blocked_entries) == 0

            results = []
            for state in states:
                task_key = state.task_key
                entry = self._entries_by_task_key[task_key]
                result = entry.get_cached_result(self._context)
                results.append(result)
            return results

        finally:
            if self._parallel_execution_enabled:
                self._context.core.process_executor.stop_logging()

    def _process_active_entry(self, entry):
        """
        Takes the current active entry and does the minimal amount of work to move it
        to the next stage. When this function returns, the entry will no longer be in
        the ACTIVE stage: it will be COMPLETED, IN_PROGRESS, or BLOCKED, and we will
        be able to move on to the next entry.
        """

        assert entry.stage == EntryStage.ACTIVE

        # In theory we could have an entry that doesn't require initializing, but in
        # practice we always require at least that much when we create one.
        assert entry.required_level >= EntryLevel.INITIALIZED

        # First, if we've already met our requirements, we can stop immediately without
        # needing to process any dependencies.
        if self._mark_entry_completed_if_possible(entry):
            return

        # Otherwise, we can't do anything more without the digests of our dependencies,
        # so we'll need them to be primed.
        self._set_up_entry_dependencies(entry)
        for dep_entry in entry.dep_entries:
            self._add_requirement(
                src_entry=entry,
                dst_entry=dep_entry,
                level=EntryLevel.PRIMED,
                expiration=EntryRequirement.Expiration.WHEN_SRC_CACHED,
            )
        if self._mark_entry_blocked_if_necessary(entry):
            return

        # Now that we have the dependency digests, we can initialize our task state.
        entry.state.initialize(self._context)

        # If that was all we needed, we're done.
        if self._mark_entry_completed_if_possible(entry):
            return

        # If this entry produces an artifact, we may be able to load it from the
        # persistent cache, which would immediately get us to the CACHED level.
        if entry.state.yields_artifact:
            entry.state.attempt_to_access_cached_artifact()

        # Otherwise, if it's not an artifact, then initializing it should have gotten it
        # to the PRIMED level.
        else:
            assert entry.level >= EntryLevel.PRIMED

        # If that was all we needed, we're done.
        if self._mark_entry_completed_if_possible(entry):
            return

        # At this point we'll need to actually compute the entry. If possible, we
        # prefer to compute it remotely. This requires checking several prerequisites,
        # then analyzing the entry's non-artifact dependencies to see if they can
        # run outside the this process and/or need to be run on AIP.
        entry_may_be_computable_remotely = (
            self._aip_execution_enabled or self._parallel_execution_enabled
        ) and entry.state.yields_artifact
        if entry_may_be_computable_remotely:
            remote_subgraph = RemoteSubgraph(entry.state, self._context)
            if self._aip_execution_enabled:
                aip_task_configs = remote_subgraph.distinct_aip_task_configs
            else:
                aip_task_configs = []

            # TODO We should add more tests to handle these edge cases, or add validation
            # to make sure they can't happen.
            if len(aip_task_configs) > 1:
                descriptions_str = "; ".join(
                    f"function outputting {state.task_key.dnode.to_descriptor()!r} "
                    f"requires {state.func_attrs.aip_task_config}"
                    for state in remote_subgraph.stripped_states_with_aip_task_configs
                )
                message = f"""
                Multiple functions need to be run together (since some are not
                persistable) but have conflicting AIP configs:
                {descriptions_str}
                """
                raise AttributeValidationError(oneline(message))

            elif len(aip_task_configs) == 1:
                if not remote_subgraph.all_states_can_be_serialized:
                    # This should never happen: only fixed-value nodes should be
                    # non-persistable, and they should never appear in the same subgraph
                    # as an AIP-decorated derived node.
                    non_serializable_descriptors = [
                        state.task_key.dnode.to_descriptor()
                        for state in remote_subgraph.non_serializable_stripped_states
                    ]
                    aip_descriptors = [
                        state.task_key.dnode.to_descriptor()
                        for state in remote_subgraph.stripped_states_with_aip_task_configs
                    ]
                    message = f"""
                    Found impossible configuration:
                    functions outputting {non_serializable_descriptors!r} are not
                    serializable,
                    but functions outputting {aip_descriptors!r} require AIP
                    """
                    postscript = "\nThis is probably a bug in Bionic."
                    raise AttributeValidationError(oneline(message) + postscript)
                (aip_task_config,) = aip_task_configs
                entry_is_computable_remotely = True

            else:
                aip_task_config = None
                entry_is_computable_remotely = (
                    self._parallel_execution_enabled
                    and remote_subgraph.all_states_can_be_serialized
                )

        else:
            entry_is_computable_remotely = False

        if entry_is_computable_remotely:
            # When we run an entry remotely, we also need to run all of its immediate
            # non-persistable (i.e., non-artifact) ancestors, since their values can't
            # be shared between processes. Those ancestors may have follow-up tasks, so
            # we'll need to run those too; and if those follow-ups are persistable (but
            # have not actually been persisted yet), their values will be usable
            # everywhere, so we'll want to track the fact that they're running. So
            # really we're going to run a collection of persistable entries (including
            # the original entry), plus their immediate non-persistable ancestors.
            target_entries = [
                self._get_or_create_entry_for_state(persistable_subgraph_state)
                for persistable_subgraph_state in remote_subgraph.persistable_but_not_persisted_states
            ]
            assert entry in target_entries

            # We want all of our entries to be initialized before we try running
            # them remotely. We'll just require this of the persistable ones, since
            # all the other ones are ancestors of them.
            for target_entry in target_entries:
                if target_entry == entry:
                    assert target_entry.level >= EntryLevel.INITIALIZED
                else:
                    self._add_requirement(
                        src_entry=entry,
                        dst_entry=target_entry,
                        level=EntryLevel.INITIALIZED,
                        expiration=EntryRequirement.Expiration.WHEN_SRC_CACHED,
                    )
            if self._mark_entry_blocked_if_necessary(entry):
                return

            for target_entry in target_entries:
                # Now we'll also require that each entry be CACHED, which guarantees
                # that it won't be COMPLETED. (If we didn't do this, the entry's
                # required level might only be INITIALIZED, in which case it would
                # already be COMPLETED. However, we know it's not already CACHED,
                # because it's `persistable_but_not_persisted`.)
                self._add_requirement(
                    dst_entry=target_entry,
                    level=EntryLevel.CACHED,
                    expiration=EntryRequirement.Expiration.WHEN_MET,
                )
                if target_entry.stage == EntryStage.PENDING:
                    self._mark_entry_active(target_entry)
                # The entry should now be ACTIVE, because:
                # - It can't be COMPLETED because of the requirement we added above.
                # - It can't be PENDING because we would have just activated it above.
                # - It shouldn't be BLOCKED, because:
                #   - we already required the entry to be at least INITIALIZED, and
                #   - it should also be eligible for remote computation and part of the
                #     same subgraph as this one, so it shouldn't have any additional
                #     requirements compared to the original entry.
                # - It shouldn't be IN_PROGRESS, because if it's a target entry for our
                #    our original entry, then the reverse should also be true (it's a
                #    symmetric relationship), so our original entry would already be
                #    IN_PROGRESS or COMPLETED too.
                assert target_entry.stage == EntryStage.ACTIVE

            stripped_target_states = [
                remote_subgraph.get_stripped_state(target_entry.state)
                for target_entry in target_entries
            ]

            # Remove the executors when sending the jobs over so that the remote
            # executor does not attempt to create its own subprocess or AIP
            # jobs.
            new_core = self._context.core.evolve(
                aip_executor=None,
                process_executor=None,
            )
            new_context = self._context.evolve(
                core=new_core,
                temp_result_cache=MemoryResultCache(),
            )
            if aip_task_config is not None:
                new_context = new_context.evolve(
                    task_key_logger=TaskKeyLogger(new_core),
                )
                new_task_completion_runner = TaskCompletionRunner(new_context)
                future = self._context.core.aip_executor.submit(
                    entry.state.task_key,
                    aip_task_config,
                    run_in_subprocess,
                    new_task_completion_runner,
                    stripped_target_states,
                )

                def done_callback(callback_future):
                    if (
                        not callback_future.cancelled()
                        and callback_future.exception() is None
                    ):
                        for target_entry in target_entries:
                            self._context.task_key_logger.log_computed_aip(
                                target_entry.state.task_key
                            )

                future.add_done_callback(done_callback)
            else:
                new_task_completion_runner = TaskCompletionRunner(new_context)
                future = self._context.core.process_executor.submit(
                    run_in_subprocess,
                    new_task_completion_runner,
                    stripped_target_states,
                )

            for target_entry in target_entries:
                self._mark_entry_in_progress(target_entry, future)
            return

        # Otherwise we'll compute this entry locally. In that case, it's not enough
        # for our dependencies to be primed; they also need to have cached values.
        for dep_entry in entry.dep_entries:
            self._add_requirement(
                src_entry=entry,
                dst_entry=dep_entry,
                level=EntryLevel.CACHED,
                expiration=EntryRequirement.Expiration.WHEN_SRC_CACHED,
            )
        if self._mark_entry_blocked_if_necessary(entry):
            return

        entry.compute(self._context)
        assert self._mark_entry_completed_if_possible(entry)

    def _set_up_entry_dependencies(self, entry):
        if entry.dep_entries is not None:
            return

        entry.dep_entries = [
            self._get_or_create_entry_for_state(dep_state)
            for dep_state in entry.state.dep_states
        ]

    def _add_requirement(
        self,
        dst_entry,
        level,
        expiration,
        src_entry=None,
    ):
        if expiration == EntryRequirement.Expiration.WHEN_SRC_CACHED:
            assert src_entry is not None

        req = EntryRequirement(
            dst_entry=dst_entry,
            level=level,
            expiration=expiration,
            src_entry=src_entry,
        )
        if req.src_entry is not None:
            req.src_entry.outgoing_reqs.add(req)
        req.dst_entry.incoming_reqs.add(req)
        if not req.is_met:
            if req.src_entry is not None:
                self._raise_entry_priority(req.dst_entry, req.src_entry.priority)
            if req.dst_entry.stage == EntryStage.COMPLETED:
                self._mark_entry_pending(req.dst_entry)

    def _raise_entry_priority(self, entry, new_priority):
        if entry.priority >= new_priority:
            return

        entry.priority = new_priority

        if entry.stage == EntryStage.PENDING:
            task_key = entry.state.task_key
            self._pending_entries_kps.pop(task_key)
            self._pending_entries_kps.push(
                key=task_key, value=entry, priority=new_priority
            )

        for req in entry.outgoing_reqs:
            if not req.is_met:
                self._raise_entry_priority(req.dst_entry, new_priority)

    def _get_or_create_entry_for_state(self, state):
        task_key = state.task_key
        if task_key in self._entries_by_task_key:
            return self._entries_by_task_key[task_key]
        # Before doing anything with this task state, we should make sure its
        # cache state is up to date.
        state.refresh_all_persistent_cache_state(self._context)
        entry = TaskRunnerEntry(self._context, state)
        self._entries_by_task_key[task_key] = entry
        return entry

    def _has_pending_entries(self):
        # While there are no entries in the to-process stack but have any in-progress ones,
        # let's wait for in-progress entries to finish till we find an entry to process or
        # exhaust all in-progress entries.
        while (
            len(self._pending_entries_kps) == 0 and len(self._in_progress_entries) != 0
        ):
            self._wait_on_in_progress_entries()
        return len(self._pending_entries_kps) != 0

    def _activate_next_pending_entry(self):
        assert len(self._pending_entries_kps) != 0
        next_entry = self._pending_entries_kps.pop()
        assert next_entry.stage == EntryStage.PENDING

        next_entry.stage = EntryStage.ACTIVE
        return next_entry

    def _mark_entry_active(self, entry):
        assert entry.stage == EntryStage.PENDING

        task_key = entry.state.task_key
        self._pending_entries_kps.pop(task_key)

        entry.stage = EntryStage.ACTIVE

    def _wait_on_in_progress_entries(self):
        "Waits on any in-progress entry to finish."
        entries_by_future = {entry.future: entry for entry in self._in_progress_entries}
        finished_futures, _ = wait(
            entries_by_future.keys(), return_when=FIRST_COMPLETED
        )
        for finished_future in finished_futures:
            try:
                self._sync_and_complete_remotely_computed_task_keys(
                    finished_future.result()
                )
            except Exception as exception:
                # If there is an error, wait until all futures are done. With
                # AIP execution, we can have one task fail while others are
                # running. If we don't wait till those tasks are done, the user
                # might perform another `get` operation that can spawn the same
                # Task again.

                logger.exception(
                    oneline(
                        f"""
                        Encountered an error while doing remote computation for
                        {entries_by_future[finished_future].state.task_key}.
                        Waiting for all other remote computation(s) to
                        complete...
                        """
                    )
                )
                finished_futures, _ = wait(
                    entries_by_future.keys(), return_when=ALL_COMPLETED
                )

                # There may be errors in the other tasks. Ensure that we log all
                # of the other errors as well.
                for future in finished_futures:
                    try:
                        self._sync_and_complete_remotely_computed_task_keys(
                            future.result()
                        )
                    except Exception as e:
                        if e != exception and e is not None:
                            logger.exception(
                                oneline(
                                    f"""
                                    Encountered another error while doing remote
                                    computation for
                                    {entries_by_future[future].state.task_key}.
                                    """
                                )
                            )

                raise exception

    def _sync_and_complete_remotely_computed_task_keys(self, task_keys):
        for task_key in task_keys:
            entry = self._entries_by_task_key[task_key]
            entry.state.sync_after_remote_computation()
            assert self._mark_entry_completed_if_possible(entry)

    def _mark_entry_completed_if_possible(self, entry):
        assert entry.stage in [EntryStage.ACTIVE, EntryStage.IN_PROGRESS]

        for req in entry.incoming_reqs:
            if req.is_met:
                if (
                    req.src_entry is not None
                    and req.src_entry.stage == EntryStage.BLOCKED
                    and req.src_entry.all_outgoing_reqs_are_met
                ):
                    self._mark_entry_pending(req.src_entry)
        if not entry.all_incoming_reqs_are_met:
            return False

        if entry.stage == EntryStage.IN_PROGRESS:
            entry.future = None
            self._in_progress_entries.remove(entry)

        entry.stage = EntryStage.COMPLETED

        if entry.level >= EntryLevel.CACHED:
            # If we have any followup tasks, we want to run them immediately.
            for followup_state in entry.state.followup_states:
                followup_entry = self._get_or_create_entry_for_state(followup_state)
                # If this entry's value is a collection of values, then we want to
                # run its followups as soon as possible in order to get the
                # (potentially large) object out of memory.
                if entry.state.desc_metadata.is_composite:
                    priority = EntryPriority.TOP
                # Otherwise, we still want to make sure followups run before any
                # non-followups.
                else:
                    priority = EntryPriority.HIGH
                self._raise_entry_priority(followup_entry, priority)
                self._add_requirement(
                    dst_entry=followup_entry,
                    level=EntryLevel.CACHED,
                    expiration=EntryRequirement.Expiration.WHEN_MET,
                )
                # We also add a requirement from the followup back to its parent.
                # This requirement will get generated anyway as soon as we start
                # processing the followup, but we want to add it early so the
                # requirement is visible when we start pruning requirements below;
                # otherwise the current entry might think it has no more requirements
                # and evict its value from memory.
                #
                # (Normally every entry we process was requested by one of its children,
                # so when it completes it always has at least one active requirement.
                # However, followups are requested by their parents and get processed
                # before their children, so it's possible for a followup to complete
                # with no active. That's why we need to add this requirement early.)
                assert entry.state in followup_entry.state.dep_states
                self._add_requirement(
                    src_entry=followup_entry,
                    dst_entry=entry,
                    level=EntryLevel.CACHED,
                    expiration=EntryRequirement.Expiration.WHEN_SRC_CACHED,
                )

            # Now this this entry has reached the final level of progress, some
            # requirements may be ready to expire.
            self._prune_requirements_for_cached_entry(entry)

            # And now that some requirements have been removed, we may be able to
            # evict some memoized values from memory.
            for dep_entry in entry.dep_entries:
                # For each temporarily-memoized input value, we can try to evict it
                # *unless* it has other high-priority (i.e., followup) entries waiting
                # for it. (In general, we want these values to be recomputed each time
                # they're needed, *except* for composite values like tuples, which need
                # to stick around long enough to be broken up by their followup tassks.)
                self._evict_temp_memoized_entry_value_if_possible(
                    dep_entry, ignore_normal_priority_reqs=True
                )
            # We can also try to evict our own value if it has no active requirements
            # at all. (This will generally happen if this entry was created as a
            # followup but has no followups of its own. Otherwise, if we do have any
            # requirements, we want the value to stick around long enough to actually
            # get used by one of them!)
            self._evict_temp_memoized_entry_value_if_possible(entry)

        return True

    def _prune_requirements_for_cached_entry(self, entry):
        assert entry.level >= EntryLevel.CACHED

        # This entry is cached, so all incoming requirements must be already be met. If
        # that matches their expiration criterion, we can discard them.
        incoming_reqs_to_discard = [
            req
            for req in entry.incoming_reqs
            if req.expiration == EntryRequirement.Expiration.WHEN_MET
        ]
        # Similary, all outgoing requirements have this entry as a source, so we know
        # their source is cached.
        outgoing_reqs_to_discard = [
            req
            for req in entry.outgoing_reqs
            if req.expiration == EntryRequirement.Expiration.WHEN_SRC_CACHED
        ]

        for req in incoming_reqs_to_discard + outgoing_reqs_to_discard:
            req.dst_entry.incoming_reqs.discard(req)
            if req.src_entry is not None:
                req.src_entry.outgoing_reqs.discard(req)

    def _evict_temp_memoized_entry_value_if_possible(
        self, entry, ignore_normal_priority_reqs=False
    ):
        if not self._context.temp_result_cache.contains(entry.state.task_key):
            return

        # If this is enabled, the value should be saved until this query completes.
        if entry.state.should_memoize_for_query:
            return

        remaining_reqs = [
            req for req in entry.incoming_reqs if req.level >= EntryLevel.CACHED
        ]
        if ignore_normal_priority_reqs:
            remaining_reqs = [
                req
                for req in remaining_reqs
                if req.src_entry is not None
                and req.src_entry.priority > EntryPriority.NORMAL
            ]
        if len(remaining_reqs) > 0:
            return

        self._context.temp_result_cache.delete(entry.state.task_key)

        if entry.stage == EntryStage.COMPLETED and not entry.all_incoming_reqs_are_met:
            self._mark_entry_pending(entry)

    def _mark_entry_blocked_if_necessary(self, entry):
        assert entry.stage == EntryStage.ACTIVE

        if entry.all_outgoing_reqs_are_met:
            return False

        entry.stage = EntryStage.BLOCKED
        self._blocked_entries.add(entry)

        return True

    def _mark_entry_pending(self, pending_entry):
        assert pending_entry.stage in (EntryStage.BLOCKED, EntryStage.COMPLETED)

        if pending_entry.stage == EntryStage.BLOCKED:
            self._blocked_entries.remove(pending_entry)

        pending_entry.stage = EntryStage.PENDING
        self._pending_entries_kps.push(
            key=pending_entry.state.task_key,
            value=pending_entry,
            priority=pending_entry.priority,
        )

    def _mark_entry_in_progress(self, in_progress_entry, future):
        assert in_progress_entry.stage == EntryStage.ACTIVE
        assert in_progress_entry.future is None
        assert in_progress_entry not in self._in_progress_entries

        in_progress_entry.stage = EntryStage.IN_PROGRESS
        in_progress_entry.future = future
        self._in_progress_entries.add(in_progress_entry)


class TaskKeyLogger:
    """
    Logs how we derived each task key. The purpose of this class is to make sure that
    each task key used in a derivation (i.e., a call to `Flow.get()`) is logged exactly
    once. (One exception: a task key can be logged twice to indicate the start and end
    of a computation.)
    """

    def __init__(self, core):
        self._level = core.task_key_logging_level

        if core.process_executor is not None:
            self._already_logged_entity_case_key_pairs = (
                core.process_executor.create_synchronized_set()
            )
        else:
            # In AIP execution, any logs and updates to this set are not
            # returned back to the main bionic process.
            self._already_logged_entity_case_key_pairs = SynchronizedSet()

    def _log(self, template, task_key, is_resolved=True):
        # We only want resolved log each (entity, case key) pair once.
        entity_names = task_key.dnode.all_entity_names()
        case_key = task_key.case_key
        pairs = [(entity_name, case_key) for entity_name in entity_names]
        if is_resolved:
            # The pairs here are fully resolved (there's no more work to do on them) so
            # we'll add them to the logged set. If any of them were not already present,
            # we should log them now.
            should_log = any(
                self._already_logged_entity_case_key_pairs.add(pair) for pair in pairs
            )
        else:
            # This pair isn't resolved, so we'll check if it's already logged, but
            # without adding it to the set.
            should_log = not all(
                self._already_logged_entity_case_key_pairs.contains(pair)
                for pair in pairs
            )

        if not should_log:
            return

        # To make the log output look more consistent, we'll clean up the descriptor
        # before logging it. (Otherwise a given entity X will sometimes be logged as
        # "X" and sometimes as "<X>", which will look weird to users.)
        clean_task_key = task_key.evolve(
            dnode=dnode_without_drafts_or_generics(task_key.dnode)
        )
        logger.log(self._level, template, clean_task_key)

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

    def log_computed_aip(self, task_key):
        self._log("Computed   %s using AI Platform", task_key)


# TODO Consider introducing the term "query" to refer to the scope of one get() call.
@attr.s(frozen=True)
class ExecutionContext:
    """
    Holds objects common to a specific "execution" -- i.e., a single ``flow.get()``
    call. This is a convenience class to save the trouble of passing these objects
    around individually.
    """

    flow_instance_uuid = attr.ib()
    core = attr.ib()
    task_key_logger = attr.ib()
    # This is used for temporarily saving results for the duration of one execution.
    # Currently we only do this for entities where both persistence and memoization are
    # disabled.
    temp_result_cache = attr.ib()

    def evolve(self, **kwargs):
        return attr.evolve(self, **kwargs)


class MemoryResultCache:
    """
    A simple cache for storing computed Results by their TaskKeys.
    """

    def __init__(self):
        self._results_by_task_key = {}

    def contains(self, task_key):
        return task_key in self._results_by_task_key

    def save(self, result):
        self._results_by_task_key[result.task_key] = result

    def load(self, task_key):
        return self._results_by_task_key.get(task_key)

    def delete(self, task_key):
        del self._results_by_task_key[task_key]


def dnode_without_drafts_or_generics(dnode):
    return dnode.edit(lambda d: d.child if d.is_draft() or d.is_generic() else d)
