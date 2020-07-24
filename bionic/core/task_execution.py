"""
This module contains the core logic that executes individual tasks.

There are two classes here: TaskRunnerEntry and TaskState. Both of these are
correspond to a Task object, but with additional information for particular contexts.

Task (included here for completeness): An immutable representation of a unit of
computation.

TaskState: Represents a Task in the context of a Flow instance. It has the same
lifetime as its Flow instance, so it's appropriate for data that we want to keep around
across multiple `get` calls. This data is generally related to the various stages an
individual Task goes through as we get ready to compute it.

TaskRunnerEntry: Represents a Task in the context of a single `Flow.get` call. This
class is managed by the TaskCompletionRunner, whose primary job is to run all the tasks
in the correct order; thus, the TaskRunnerEntry mostly contains data pertaining to the
task's relationship to other tasks.
"""

import copy
import logging
import warnings

from enum import Enum, auto

from ..datatypes import ProvenanceDigest, Query, Result
from ..exception import CodeVersioningError
from ..persistence import Provenance
from ..utils.misc import oneline, single_unique_element

logger = logging.getLogger(__name__)


class TaskRunnerEntry:
    """
    Represents a task to be completed by the `TaskCompletionRunner`.

    Wraps a `TaskState`, and holds additional information tracking its relationship to
    other entries. Each entry has "requests", which are imposed on it by other task
    entries. The possible requests correspond to the two main milestones an entry can
    reach, which are easiest to understand in reverse order:

    "Cached": This means the task has been computed and its output value is stored
    somewhere -- in the persistent cache, in memory on the TaskState, and/or in
    memory on this entry (depending on the cache settings). This is the final
    milestone: after this, there is no more work to do on this task.

    "Primed": This is a more abstract condition. It guarantees two things:

    a) This task's provenance digest is available, which means any downstream tasks can
    have their values loaded from the persisted cache. For a task with non-persistable
    output, its provenance digest depends only on its provenance; it doesn't actually
    require the task to be computed. However, for a task with persistable output, the
    provenance digest depends on its actual value, so the task must be computed and its
    output cached.

    b) There is no additional *persistable* work to do on this task. In other words,
    if we have any dependent tasks that we plan to run in a separate process, we can
    go ahead and start them; there may be more work to do on this task, but it will
    have to be done in that separate process, because its results can't be serialized
    and transmitted. (On the other hand, if we have a dependent task to run *in this
    same process*, we'll want to bring this task to the "cached" state instead.) As
    with (a), for a task with non-persistable output, this milestone is reached as
    soon as we compute its provenance; for a task with persistable output, it's
    reached only when the task is computed and its output is cached.

    Thus, the operational definition of "primed" depends on whether the task's output is
    persistable or not. If so, "primed" is equivalent to "cached"; if not, "primed"
    represents a preliminary stage of work which happens to be sufficient for many
    applications.
    """

    def __init__(self, state):
        self.state = state
        self.future = None
        self.results_by_dnode = None
        self.stage = EntryStage.COMPLETED

        # This is initially set to None to avoid eagerly recursing through the entire
        # DAG. We set this once we start processing the entry.
        self.dep_entries = None

        # These are initially set to False, but can be updated to True when we discover
        # a requirement from another entry. However, they should never go from True to
        # False!
        self.is_requested_to_be_primed = False
        self.is_requested_to_be_cached = False

    @property
    def stage(self):
        return self._stage

    @stage.setter
    def stage(self, stage):
        logger.debug("Updated %s to %s", self, stage.name)
        self._stage = stage

    @property
    def is_cached(self):
        return self.results_by_dnode is not None or self.state.is_cached

    @property
    def is_primed(self):
        if self.state.should_persist is None:
            return False
        if self.state.should_persist:
            return self.state.is_cached
        else:
            return self.state.is_initialized

    # TODO Later, once we're keeping track of exactly which requests are imposed by
    # which entries, we should avoid using a dependency's EntryStage and instead just
    # check whether it satisfies our requests.
    def blocking_dep_entries(self):
        assert self.dep_entries is not None
        return [
            dep_entry
            for dep_entry in self.dep_entries
            if dep_entry.stage != EntryStage.COMPLETED
        ]

    def blocking_dep_task_keys(self):
        blocking_dep_entries = self.blocking_dep_entries()
        return set(
            blocking_dep_entry_tk
            for blocking_dep_entry in blocking_dep_entries
            for blocking_dep_entry_tk in blocking_dep_entry.state.task_keys
        )

    @property
    def satisfies_requests(self):
        if self.is_requested_to_be_primed and not self.is_primed:
            return False
        if self.is_requested_to_be_cached and not self.is_cached:
            return False
        return True

    @property
    def all_dependencies_satisfy_requests(self):
        return all(dep_entry.satisfies_requests for dep_entry in self.dep_entries)

    def compute(self, task_key_logger):
        """
        Computes the values of an entry by running its task. Requires that all
        the task's dependencies are already computed.
        """

        state = self.state
        task = self.state.task

        assert state.is_initialized
        assert not state.is_cached

        dep_results = []
        for dep_entry, dep_key in zip(self.dep_entries, task.dep_keys):
            assert dep_entry.is_cached
            dep_results_by_dnode = dep_entry.get_cached_results(task_key_logger)
            dep_results.append(dep_results_by_dnode[dep_key.dnode])

        if not task.is_simple_lookup:
            for task_key in state.task_keys:
                task_key_logger.log_computing(task_key)

        dep_values = [dep_result.value for dep_result in dep_results]

        # If we have any missing outputs, exit early with a missing result.
        if state.output_would_be_missing():
            results_by_dnode = {}
            result_value_hashes_by_dnode = {}
            for query in state._queries:
                result = Result(query=query, value=None, value_is_missing=True)
                results_by_dnode[query.dnode] = result
                result_value_hashes_by_dnode[query.dnode] = ""
            state._results_by_dnode = results_by_dnode
            if state.should_persist:
                state._result_value_hashes_by_dnode = result_value_hashes_by_dnode
            return state._results_by_dnode

        else:
            # If we have no missing outputs, we should not be consuming any missing
            # inputs either.
            assert not any(
                dep_key.case_key.has_missing_values for dep_key in task.dep_keys
            )

        values = task.compute(dep_values)
        assert len(values) == len(state.task_keys)

        for query in state._queries:
            if task.is_simple_lookup:
                task_key_logger.log_accessed_from_definition(query.task_key)
            else:
                task_key_logger.log_computed(query.task_key)

        results_by_dnode = {}
        result_value_hashes_by_dnode = {}
        for ix, (query, value) in enumerate(zip(state._queries, values)):
            query.protocol.validate(value)

            result = Result(query=query, value=value)

            if state.should_persist:
                accessor = state._cache_accessors[ix]
                accessor.save_result(result)

                value_hash = accessor.load_result_value_hash()
                result_value_hashes_by_dnode[query.dnode] = value_hash

            results_by_dnode[query.dnode] = result

        # We cache the hashed values eagerly since they are cheap to load.
        if state.should_persist:
            state._result_value_hashes_by_dnode = result_value_hashes_by_dnode
        # Memoize results at this point only if results should not persist.
        # Otherwise, load it lazily later so that if the serialized/deserialized
        # value is not exactly the same as the original, we still
        # always return the same value.
        elif state.should_memoize:
            state._results_by_dnode = results_by_dnode
        else:
            self.results_by_dnode = results_by_dnode

    def get_cached_results(self, task_key_logger):
        "Returns the results of an already-computed entry."

        assert self.is_cached

        if self.results_by_dnode:
            return self.results_by_dnode
        return self.state.get_cached_results(task_key_logger)

    def __str__(self):
        return f"TaskRunnerEntry({', '.join(str(key) for key in self.state.task_keys)})"


class EntryStage(Enum):
    """
    Represents the stage of completion reached by a `TaskRunnerEntry`.
    """

    """
    The entry is completed; we don't have any more work to do with it. All entries start
    here (before any requests have been placed on them) and end here (once all the
    requests have been satisfied).

    Valid next stages: [PENDING]
    """
    COMPLETED = auto()

    """
    The entry is waiting to be processed. We know there's work to be done on it, but
    we haven't gotten to it yet.

    Valid next stages: [ACTIVE]
    """
    PENDING = auto()

    """
    The entry is being actively processed. There should only be one such entry at a
    time.

    Valid next stages: [BLOCKED, IN_PROGRESS, COMPLETED]
    """
    ACTIVE = auto()

    """
    The entry is blocked: we can't continue processing it until some other entries are
    completed.

    Valid next stages: [PENDING]
    """
    BLOCKED = auto()

    """
    The entry is currently being computed in another process.

    Valid next stages: [COMPLETED]
    """
    IN_PROGRESS = auto()


class EntryBlockage:
    """
    Represents a blocking relationship between a task state and a collection of
    not-yet-completed task keys it depends on.
    """

    def __init__(self, blocked_entry, blocking_tks):
        self.blocked_entry = blocked_entry
        self._blocking_tks = set(blocking_tks)

    def mark_task_key_complete(self, blocking_tk):
        self._blocking_tks.discard(blocking_tk)

    def is_resolved(self):
        return not self._blocking_tks


# TODO Let's reorder the methods here with this order:
# 1. First public, then private.
# 2. Rough chronological order.
class TaskState:
    """
    Represents the state of a task computation.  Keeps track of its position in
    the task graph, whether its values have been computed yet, additional
    intermediate state and the deriving logic.
    """

    def __init__(self, task, dep_states, case_key, provider, entity_defs_by_dnode):
        assert len(entity_defs_by_dnode) == len(task.keys)

        self.task = task
        self.dep_states = dep_states
        self.case_key = case_key
        self.provider = provider
        self.entity_defs_by_dnode = entity_defs_by_dnode

        # Cached values.
        self.task_keys = task.keys

        # These are set by set_up_caching_flags()
        self._are_caching_flags_set_up = False
        self.should_memoize = None
        self.should_persist = None

        # These are set by initialize().
        self.is_initialized = False
        self._provenance = None
        self._queries = None
        self._cache_accessors = None

        # This can be set by compute(), _load_value_hashes(), or
        # attempt_to_access_persistent_cached_values().
        # This will be present only if should_persist is True.
        self._result_value_hashes_by_dnode = None

        # This can be set by get_cached_results() or compute().
        self._results_by_dnode = None

    @property
    def should_cache(self):
        return self.should_memoize or self.should_persist

    @property
    def is_cached(self):
        """
        Indicates whether the task state's results are cached.
        """
        return (
            self._result_value_hashes_by_dnode is not None
            or self._results_by_dnode is not None
        )

    def output_would_be_missing(self):
        return single_unique_element(
            task_key.case_key.has_missing_values for task_key in self.task.keys
        )

    def __repr__(self):
        return f"TaskState({self.task!r})"

    def get_cached_results(self, task_key_logger):
        "Returns the results of an already-computed task state."

        assert self.is_cached

        if self._results_by_dnode:
            for task_key in self.task_keys:
                task_key_logger.log_accessed_from_memory(task_key)
            return self._results_by_dnode

        results_by_dnode = dict()
        for accessor in self._cache_accessors:
            result = accessor.load_result()
            task_key_logger.log_loaded_from_disk(result.query.task_key)

            # Make sure the result is saved in all caches under this exact
            # query.
            accessor.save_result(result)

            results_by_dnode[result.query.dnode] = result

        if self.should_memoize:
            self._results_by_dnode = results_by_dnode

        return results_by_dnode

    def attempt_to_access_persistent_cached_values(self):
        """
        Loads the hashes of the persisted values for this task, if they exist.

        If persisted values are available in the cache, this object's `is_cached`
        property will become true. Otherwise, nothing will happen.
        """
        assert self.is_initialized
        assert not self.is_cached

        if not self.should_persist:
            return
        if not all(axr.can_load() for axr in self._cache_accessors):
            return

        self._load_value_hashes()

    def refresh_all_persistent_cache_state(self, bootstrap):
        """
        Refreshes all state that depends on the persistent cache.

        This is useful if the external cache state might have changed since we last
        worked with this task.
        """

        # If this task state is not initialized or not persisted, there's nothing to
        # refresh.
        if not self.is_initialized or not self.should_persist:
            return

        self.refresh_cache_accessors(bootstrap)

        # If we haven't loaded anything from the cache, we can stop here.
        if self._result_value_hashes_by_dnode is None:
            return

        # Otherwise, let's update our value hashes from the cache.
        if all(axr.can_load() for axr in self._cache_accessors):
            self._load_value_hashes()
        else:
            self._result_value_hashes_by_dnode = None

    def sync_after_subprocess_computation(self):
        """
        Syncs the task state by populating and reloading data in the current process
        after completing the task state in a subprocess.

        This is necessary because values populated in the task state are not
        communicated back from the subprocess.
        """

        assert self.should_persist

        # First, let's flush the stored entries in cache accessors. Since we just
        # computed this entry in a subprocess, there should be a new cache entry that
        # isn't reflected yet in our local accessors.
        # (We don't just call self.refresh_cache_accessors() because we don't
        # particularly want to do the cache versioning check -- it's a little late to
        # do anything if it fails now.)
        for accessor in self._cache_accessors:
            accessor.flush_stored_entries()

        # Then, populate the value hashes.
        if self._result_value_hashes_by_dnode is None:
            self._load_value_hashes()

    def initialize(self, bootstrap, flow_instance_uuid):
        "Initializes the task state to get it ready for completion."

        if self.is_initialized:
            return

        # First, set up caching flags.
        self.set_up_caching_flags(bootstrap)

        # Then set up provenance.
        if bootstrap is None:
            # If we're still in the bootstrap resolution phase, we don't have
            # any versioning policy, so we don't attempt anything fancy.
            treat_bytecode_as_functional = False
        else:
            treat_bytecode_as_functional = (
                bootstrap.versioning_policy.treat_bytecode_as_functional
            )

        dep_provenance_digests_by_task_key = {
            dep_key: dep_state._get_digests_by_dnode()[dep_key.dnode]
            for dep_key, dep_state in zip(self.task.dep_keys, self.dep_states)
        }

        self._provenance = Provenance.from_computation(
            code_fingerprint=self.provider.get_code_fingerprint(self.case_key),
            case_key=self.case_key,
            dep_provenance_digests_by_task_key=dep_provenance_digests_by_task_key,
            treat_bytecode_as_functional=treat_bytecode_as_functional,
            can_functionally_change_per_run=self.provider.attrs.changes_per_run,
            flow_instance_uuid=flow_instance_uuid,
        )

        # Then set up queries.
        self._queries = [
            Query(
                task_key=task_key,
                protocol=self.entity_defs_by_dnode[task_key.dnode].protocol,
                provenance=self._provenance,
            )
            for task_key in self.task_keys
        ]

        # Lastly, set up cache accessors.
        if self.should_persist:
            self.refresh_cache_accessors(bootstrap)

        self.is_initialized = True

    def set_up_caching_flags(self, bootstrap):
        # Setting up the flags is cheap, but it can result in warnings that we don't
        # need to emit multiple times.
        if self._are_caching_flags_set_up:
            return

        # In theory different entities for a single task could have different cache
        # settings, but I'm not sure it can happen in practice (given the way
        # grouped entities are created). At any rate, once we have tuple
        # descriptors, each task state will only be responsible for a single entity
        # and this won't be an issue.
        optional_should_memoize, optional_should_persist = single_unique_element(
            (entity_def.optional_should_memoize, entity_def.optional_should_persist)
            for entity_def in self.entity_defs_by_dnode.values()
        )

        if optional_should_memoize is not None:
            should_memoize = optional_should_memoize
        elif bootstrap is not None:
            should_memoize = bootstrap.should_memoize_default
        else:
            should_memoize = True
        if self.provider.attrs.changes_per_run and not should_memoize:
            descriptors = [
                task_key.dnode.to_descriptor() for task_key in self.task_keys
            ]
            if bootstrap is None or bootstrap.should_memoize_default:
                fix_message = (
                    "removing `memoize(False)` from the corresponding function"
                )
            else:
                fix_message = "applying `@memoize(True)` to the corresponding function"
            message = f"""
            Descriptors {descriptors!r} aren't configured to be memoized but
            are decorated with @changes_per_run. We will memoize it anyway:
            since @changes_per_run implies that this value can have a different
            value each time it’s computed, we need to memoize its value to make
            sure it’s consistent across the entire flow. To avoid this warning,
            enable memoization for the descriptor by {fix_message!r}."""
            warnings.warn(oneline(message))
            should_memoize = True
        self.should_memoize = should_memoize

        if self.output_would_be_missing():
            should_persist = False
        elif optional_should_persist is not None:
            should_persist = optional_should_persist
        elif bootstrap is not None:
            should_persist = bootstrap.should_persist_default
        else:
            should_persist = False
        if should_persist and bootstrap is None:
            descriptors = [
                task_key.dnode.to_descriptor() for task_key in self.task_keys
            ]
            if self.task.is_simple_lookup:
                disable_message = """
                applying `@persist(False)` or `@immediate` to the corresponding
                function"""
            else:
                disable_message = """
                passing `persist=False` when you `declare` / `assign` the entity
                values"""
            message = f"""
            Descriptors {descriptors!r} are set to be persisted but they can't be
            because core bootstrap entities depend on them.
            The corresponding values will not be serialized and deserialized,
            which may cause the values to be subtly different.
            To avoid this warning, disable persistence for the decorators
            by {disable_message}."""
            # TODO We should choose between `logger.warn` and `warnings.warn` and use
            # one consistently.
            logger.warn(message)
            should_persist = False
        self.should_persist = should_persist

        self._are_caching_flags_set_up = True

    def refresh_cache_accessors(self, bootstrap):
        """
        Initializes the cache acessors for this task state.

        This sets up state that allows us to read and write cache entries for this
        task's value. This includes some in-memory representations of exernal persistent
        resources (files or cloud blobs); calling this multiple times can be necessary
        in order to wipe this state and allow it get back in sync with the real world.
        """

        self._cache_accessors = [
            bootstrap.persistent_cache.get_accessor(query) for query in self._queries
        ]

        if bootstrap.versioning_policy.check_for_bytecode_errors:
            self._check_accessors_for_version_problems()

    def _check_accessors_for_version_problems(self):
        """
        Checks for any versioning errors -- i.e., any cases where a task's
        function code was updated but its version annotation was not.
        """

        accessors_needing_saving = []
        for accessor in self._cache_accessors:
            old_prov = accessor.load_provenance()

            if old_prov is None:
                continue

            new_prov = accessor.query.provenance

            if old_prov.exactly_matches(new_prov):
                continue
            accessors_needing_saving.append(accessor)

            if old_prov.code_version_minor == new_prov.code_version_minor:
                if old_prov.bytecode_hash != new_prov.bytecode_hash:
                    raise CodeVersioningError(
                        oneline(
                            f"""
                        Found a cached artifact with the same
                        descriptor ({accessor.query.dnode.to_descriptor()!r}) and
                        version (major={old_prov.code_version_major!r},
                        minor={old_prov.code_version_minor!r}),
                        But created by different code
                        (old hash {old_prov.bytecode_hash!r},
                        new hash {new_prov.bytecode_hash!r}).
                        Did you change your code but not update the
                        version number?
                        Change @version(major=) to indicate that your
                        function's behavior has changed, or @version(minor=)
                        to indicate that it has *not* changed."""
                        )
                    )

        for accessor in accessors_needing_saving:
            accessor.update_provenance()

    def _load_value_hashes(self):
        """
        Reads (from disk) and saves (in memory) this task's value hashes.
        """

        result_value_hashes_by_dnode = {}
        for accessor in self._cache_accessors:
            value_hash = accessor.load_result_value_hash()
            if value_hash is None:
                raise AssertionError(
                    oneline(
                        f"""
                    Failed to load cached value (hash) for descriptor
                    {accessor.query.dnode.to_descriptor()!r}.
                    This suggests we did not successfully compute the task
                    in a subprocess, or the entity wasn't cached;
                    this should be impossible!"""
                    )
                )
            result_value_hashes_by_dnode[accessor.query.dnode] = value_hash
        self._result_value_hashes_by_dnode = result_value_hashes_by_dnode

    def _get_digests_by_dnode(self):
        if self.should_persist:
            assert self._result_value_hashes_by_dnode is not None
            return {
                dnode: ProvenanceDigest.from_value_hash(value_hash)
                for dnode, value_hash in self._result_value_hashes_by_dnode.items()
            }
        else:
            assert self._provenance is not None
            return {
                task_key.dnode: ProvenanceDigest.from_provenance(self._provenance)
                for task_key in self.task.keys
            }

    def strip_state_for_subprocess(self, new_task_states_by_key=None):
        """
        Returns a copy of task state after keeping only the necessary data
        required for to compute it. In addition, this also removes all the memoized
        results since they can be expensive to serialize.

        Mainly used
        - because some results are impossible to serialize and
        - to reduce IPC overhead when sending the state over to another subprocess.

        Parameters
        ----------

        new_task_states_by_key: Dict from key to stripped states, optional, default is ``{}``
            The cache for tracking stripped task states.
        """

        if new_task_states_by_key is None:
            new_task_states_by_key = {}

        # All task keys should point to the same task state.
        if self.task_keys[0] in new_task_states_by_key:
            return new_task_states_by_key[self.task_keys[0]]

        # Let's make a copy of the task state.
        # This is not a deep copy so we'll avoid mutating any of the member variables.
        task_state = copy.copy(self)

        # Clear up memoized cache to avoid sending it through IPC.
        task_state._results_by_dnode = None
        # Clear up fields not needed in subprocess, for computing or for cache lookup.
        task_state.provider = None
        task_state.entity_defs_by_dnode = None
        task_state.case_key = None
        task_state._provenance = None

        if task_state.is_cached:
            task_state.dep_states = []
            task_state.task = None
        else:
            task_state.dep_states = [
                dep_state.strip_state_for_subprocess(new_task_states_by_key)
                for dep_state in task_state.dep_states
            ]

        new_task_states_by_key[task_state.task_keys[0]] = task_state
        return task_state
