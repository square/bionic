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

import attr
import copy
import logging
import warnings

from enum import auto, Enum, IntEnum

from ..datatypes import ProvenanceDigest, Query, Result
from ..exception import CodeVersioningError
from ..persistence import Provenance
from ..utils.misc import oneline, single_unique_element

logger = logging.getLogger(__name__)


class TaskRunnerEntry:
    """
    Represents a task to be completed by the `TaskCompletionRunner`.

    Wraps a `TaskState`, and holds additional information tracking its relationship
    to other entries. These relationships mostly take the form of `EntryRequirement`
    objects, which indicate that one entry can't do any work until another entry
    reaches a certain level of progress.

    Main attributes
    ---------------
    level: EntryLevel
        The level of progress reached by this entry's TaskState.
    incoming_reqs: list of EntryRequirement
        All requirements placed on this entry by other entries.
    outgoing_reqs: list of EntryRequirement
        All requirements placed on other entries by this one.
    stage: EntryStage
        The position of this entry within the bookkeeping system of its owning
        TaskCompletionRunner.
    """

    def __init__(self, state):
        self.state = state
        self.future = None
        self.results_by_dnode = None
        self._stage = None
        self.stage = EntryStage.COMPLETED

        # This is initially set to None to avoid eagerly recursing through the entire
        # DAG. We set this once we start processing the entry.
        self.dep_entries = None

        self.incoming_reqs = set()
        self.outgoing_reqs = set()
        self.priority = EntryPriority.DEFAULT

    @property
    def stage(self):
        return self._stage

    @stage.setter
    def stage(self, stage):
        if self._stage is None:
            fmt_str = "Created %s as %s"
        else:
            fmt_str = "Updated %s to %s"
        logger.debug(fmt_str, self, stage.name)
        self._stage = stage

    @property
    def level(self):
        if self._is_cached:
            return EntryLevel.CACHED
        elif self._is_primed:
            return EntryLevel.PRIMED
        elif self._is_initialized:
            return EntryLevel.INITIALIZED
        else:
            return EntryLevel.CREATED

    @property
    def required_level(self):
        return max(
            (req.level for req in self.incoming_reqs), default=EntryLevel.CREATED
        )

    @property
    def all_incoming_reqs_are_met(self):
        return self.level >= self.required_level

    @property
    def all_outgoing_reqs_are_met(self):
        return all(req.is_met for req in self.outgoing_reqs)

    @property
    def _is_cached(self):
        return self.results_by_dnode is not None or self.state.is_cached

    @property
    def _is_primed(self):
        if self.state.should_persist is None:
            return False
        if self.state.should_persist:
            return self.state.is_cached
        else:
            return self.state.is_initialized

    @property
    def _is_initialized(self):
        return self.state.is_initialized

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
            assert dep_entry._is_cached
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

        assert self._is_cached

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
    here (before any requirements have been placed on them) and end here (once all the
    requirements have been met).

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
    The entry is being actively processed. Typically there's only one such entry at a
    time, although we sometimes activate multiple entries in order to process them as
    a group.

    Valid next stages: [BLOCKED, IN_PROGRESS, COMPLETED]
    """
    ACTIVE = auto()

    """
    The entry is blocked: it has requirements on other entries that haven't been met
    yet, so we can't do any more work on it.

    Valid next stages: [PENDING]
    """
    BLOCKED = auto()

    """
    The entry is currently being computed in another process.

    Valid next stages: [COMPLETED]
    """
    IN_PROGRESS = auto()


@attr.s(frozen=True)
class EntryRequirement:
    """
    Represents a requirement from one entry to another.

    A requirement indicates that one entry (`src_entry`) can't make any further progress
    until another entry (`dst_entry`) has reached a certain level of progress (`level`).

    Note: `src_entry` can also be `None`, indicating some sort of external or a priori
    requirement. On the other hand, `dst_entry` cannot be `None`.
    """

    src_entry = attr.ib()
    dst_entry = attr.ib()
    level = attr.ib()

    @property
    def is_met(self):
        return self.dst_entry.level >= self.level


class EntryLevel(IntEnum):
    """
    Represents the level of progress we've made on a TaskRunnerEntry's TaskState.

    There are four levels of progress (in order):

    1. CREATED: The TaskState exists but not much work has been done on it.

    2. INITIALIZED: The TaskState's initialize() method has been called. At this point
        all of the task's dependencies are guaranteed to have provenance digests
        available, which means we can attempt to load its value from the persistent
        cache.

    3. PRIMED: If the TaskState's value is persistable, this is equivalent to CACHED;
        otherwise it's equivalent to INITIALIZED. This abstract definition is useful
        because it guarantees two things:

        a.  The task's provenance digest is available, which means any downstream tasks
            can have their values loaded from the persisted cache. For a task with
            non-persistable output, its provenance digest depends only on its
            provenance; it doesn't actually require the task to be computed. However,
            for a task with persistable output, the provenance digest depends on its
            actual value, so the task must be computed and its output cached.

        b. There is no additional *persistable* work to do on this task. In other words,
            if we have any dependent tasks that we plan to run in a separate process,
            we can go ahead and start them; there may be more work to do on this
            task, but it will have to be done in that separate process, because its
            results can't be serialized and transmitted. (On the other hand, if we
            have a dependent task to run *in this same process*, we'll want to bring
            this task to the CACHED level instead.) As with (a), for a task with
            non-persistable output, this milestone is reached as soon as we compute
            its provenance; for a task with persistable output, it's reached only
            when the task is computed and its output is cached.

    4. CACHED: The task has been computed and its output value is stored somewhere --
        in the persistent cache, in memory on the TaskState, and/or in memory on this
        entry (depending on the cache settings). This is the final level: after this,
        there is no more work to do on this task.
    """

    CREATED = auto()
    INITIALIZED = auto()
    PRIMED = auto()
    CACHED = auto()


class EntryPriority(IntEnum):
    """
    Indicates a level of priority for a TaskRunnerEntry.

    When multiple entries are in the PENDING stage, an entry with higher priority will
    always be activated before one with lower priority. There are currently only two
    priorities: DEFAULT and HIGH.

    Currently, the only reason to prioritize one entry over another is because we may
    want one entry's output to be saved to a persistent cache before another entry has
    a chance to run (and possibly crash).
    """

    DEFAULT = auto()
    HIGH = auto()


# TODO Let's reorder the methods here with this order:
# 1. First public, then private.
# 2. Rough chronological order.
class TaskState:
    """
    Represents the state of a task computation.  Keeps track of its position in
    the task graph, whether its values have been computed yet, additional
    intermediate state and the deriving logic.

    Parameters
    ----------
    task: Task
        The task whose state we're tracking.
    dep_states: list of TaskStates
        TaskStates that we depend on; these correspond to `task.dep_keys`.
    followup_states: list of TaskStates
        Other TaskStates that should run immediately after this one.
    case_key: CaseKey
        The case key shared by all the task's TaskKeys.
        TODO There is no particular reason we need to pass this in instead of computing
        it in the constructor. Once we restrict each task to having a single key,
        this will be trivial to compute and we can remove this parameter.
    func_attrs: FunctionAttributes
        Additional details about the task's `compute_func` function.
        TODO This should probably be on the Task object itself.
    entity_defs_by_dnode: dict from DescriptorNode to EntityDefinition
        Definitions of each entity (or other descriptor) produced by this task; has one
        entry for each `task_key.dnode` in `task.keys`.
    """

    def __init__(
        self,
        task,
        dep_states,
        followup_states,
        case_key,
        func_attrs,
        entity_defs_by_dnode,
    ):
        assert len(entity_defs_by_dnode) == len(task.keys)

        self.task = task
        self.dep_states = dep_states
        self.followup_states = followup_states
        self.case_key = case_key
        self.func_attrs = func_attrs
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

    @property
    def is_cached_persistently(self):
        """
        Indicates whether the task state's results are cached persistently.
        """
        return self._result_value_hashes_by_dnode is not None

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

    def sync_after_remote_computation(self):
        """
        Syncs the task state by populating and reloading data in the current process
        after completing the task state in a subprocess.

        This is necessary because values populated in the task state are not
        communicated back from the subprocess.
        """

        # If this state was never initialized, it doesn't have any out-of-date
        # information, so there's no need to update anything.
        if not self.is_initialized:
            return

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
            code_fingerprint=self.func_attrs.code_fingerprint,
            case_key=self.case_key,
            dep_provenance_digests_by_task_key=dep_provenance_digests_by_task_key,
            treat_bytecode_as_functional=treat_bytecode_as_functional,
            can_functionally_change_per_run=self.func_attrs.changes_per_run,
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
        if self.func_attrs.changes_per_run and not should_memoize:
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

        if self.should_persist:
            assert len(self.followup_states) == 0

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


class RemoteSubgraph:
    """
    Represents a subset of a task graph to be computed remotely (i.e., in another
    process).

    Given a target TaskState, this class identifies the minimal set of TaskStates that
    should be run along with it; this includes all of its immediate non-persistable
    ancestors (which can't be serialized and transmitted to the other process) and any
    of their follow-up tasks (which need to be computed immediately after their
    parents). It also maintains a copy of this subgraph with unnecessary data pruned;
    these "stripped" TaskStates can be safely serialized with cloudpickle and sent to
    the other process.
    """

    def __init__(self, target_state, bootstrap):
        self._bootstrap = bootstrap

        self._stripped_states_by_task_key = {}
        self.persistable_but_not_persisted_states = set()

        self._strip_state(target_state)

    def _strip_state(self, original_state):
        task_key = original_state.task.keys[0]
        if task_key in self._stripped_states_by_task_key:
            return self._stripped_states_by_task_key[task_key]
        original_state.set_up_caching_flags(self._bootstrap)

        # Make a copy of the TaskState, which we'll strip down to make it easier to
        # serialize.
        # (This is a shallow copy, so we'll make sure to avoid mutating any of its
        # member variables.)
        stripped_state = copy.copy(original_state)
        self._stripped_states_by_task_key[task_key] = stripped_state

        # Strip out data cached in memory -- we can't necessarily pickle it, so
        # we need to get rid of it before trying to transmit this state to another
        # process.
        stripped_state._results_by_dnode = None

        # These fields are picklable, but only needed for cache setup and
        # initialization.
        if stripped_state.is_initialized:
            stripped_state.func_attrs = None
            stripped_state.entity_defs_by_dnode = None
            stripped_state.case_key = None

        if stripped_state.should_persist:
            assert len(stripped_state.followup_states) == 0

            if stripped_state.is_cached_persistently:
                stripped_state.task = None
                stripped_state.dep_states = []

            else:
                self.persistable_but_not_persisted_states.add(original_state)

        stripped_state.dep_states = [
            self._strip_state(dep_state) for dep_state in stripped_state.dep_states
        ]
        stripped_state.followup_states = [
            self._strip_state(followup_state)
            for followup_state in stripped_state.followup_states
        ]

        return stripped_state

    def get_stripped_state(self, original_state):
        assert original_state in self.persistable_but_not_persisted_states
        task_key = original_state.task.keys[0]
        return self._stripped_states_by_task_key[task_key]
