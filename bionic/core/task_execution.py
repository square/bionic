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
from enum import auto, Enum, IntEnum
import logging
from typing import Optional

import attr

from ..datatypes import Result
from ..exception import CodeVersioningError
from ..persistence import Provenance, ProvenanceDigest
from ..utils.misc import oneline

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

    def __init__(self, context, state):
        self.context = context
        self.state = state
        self.future = None
        self._stage = None
        self.stage = EntryStage.COMPLETED

        # This is initially set to None to avoid eagerly recursing through the entire
        # DAG. We set this once we start processing the entry.
        self.dep_entries = None

        self.incoming_reqs = set()
        self.outgoing_reqs = set()
        self.priority = EntryPriority.NORMAL

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
        return (
            self.context.temp_result_cache.contains(self.state.task_key)
            or self.state.is_cached
        )

    @property
    def _is_primed(self):
        if self.state.yields_artifact:
            return self.state.is_cached
        else:
            return self.state.is_initialized

    @property
    def _is_initialized(self):
        return self.state.is_initialized

    def compute(self, context):
        """
        Computes the value of an entry by running its task. Requires that all
        the task's dependencies are already computed.
        """

        # TODO There are a few cases here where we acccess private members on
        # self.state; should we clean this up?

        state = self.state
        task = state.task

        assert state.is_initialized
        assert not state.is_cached

        dep_results = []
        for dep_entry, dep_key in zip(self.dep_entries, task.dep_keys):
            assert dep_entry._is_cached
            dep_result = dep_entry.get_cached_result(context)
            dep_results.append(dep_result)

        if not task.is_simple_lookup:
            context.task_key_logger.log_computing(state.task_key)

        dep_values = [dep_result.value for dep_result in dep_results]

        # If we have any missing outputs, exit early with a missing result.
        if state.output_would_be_missing():
            result = Result(
                task_key=task.key,
                value=None,
                value_is_missing=True,
            )
            value_hash = ""
            # TODO Should we do this even when memoization is disabled?
            state._result = result
            if state.yields_artifact:
                state._result_value_hash = value_hash
            return result

        else:
            # If we have no missing outputs, we should not be consuming any missing
            # inputs either.
            assert not any(
                dep_key.case_key.has_missing_values for dep_key in task.dep_keys
            )

        value = task.compute(dep_values)

        if task.is_simple_lookup:
            context.task_key_logger.log_accessed_from_definition(state.task_key)
        else:
            context.task_key_logger.log_computed(state.task_key)

        result = Result(
            task_key=task.key,
            value=value,
        )

        if state.yields_artifact:
            artifact = result.value
            state._cache_accessor.save_local_artifact(artifact)
            state._result_value_hash = artifact.content_hash

        # If we're not persisting the result, this is our only chance to memoize it;
        # otherwise, we can memoize it later if/when we load it from get_cached_result.
        # (It's important to memoize the value we loaded, not the one we just computed,
        # because they may be subtly different and we want all downstream tasks to get
        # exactly the same value.)
        elif state.should_memoize:
            state._result = result

        else:
            self.context.temp_result_cache.save(result)

    def get_cached_result(self, context):
        "Returns the result of an already-computed entry."

        assert self._is_cached

        result = self.context.temp_result_cache.load(self.state.task_key)
        if result is not None:
            return result

        return self.state.get_cached_result(context)

    def __str__(self):
        return f"TaskRunnerEntry({self.state.task_key})"


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

    A requirement indicates that we need a particular entry (``dst_entry``) to reach a
    certain level of progress (``level``). Once reached, this level needs to be
    maintained until a certain point (``expiration``); afterwards the requirement is no
    longer in effect and can be ignored (and deleted).

    There are three possible values for ``expiration``:

    - ``WHEN_MET``: When ``dst_entry` reaches ``level``.
    - ``WHEN_SRC_CACHED``: When another entry (``src_entry``) reaches the ``CACHED``
        stage. Generally ``src_entry`` will depend on the requirement being met, so this
        condition implies the previous one.
    - ``NEVER``: This requirement is in effect forever.
    """

    class Expiration(IntEnum):
        """
        Represents the point when a requirement ceases to be in effect.
        """

        WHEN_MET = auto()
        WHEN_SRC_CACHED = auto()
        NEVER = auto()

    dst_entry: TaskRunnerEntry = attr.ib()
    level: "EntryLevel" = attr.ib()
    expiration: Expiration = attr.ib()
    src_entry: Optional[TaskRunnerEntry] = attr.ib()

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

    3. PRIMED: If the TaskState's value is an artifact, this is equivalent to CACHED;
        otherwise it's equivalent to INITIALIZED. This abstract definition is useful
        because it guarantees two things:

        a.  The task's provenance digest is available, which means any downstream tasks
            can have their values loaded from the persisted cache. For a task with
            non-artifact output, its provenance digest depends only on its
            provenance; it doesn't actually require the task to be computed. However,
            for a task with artifact output, the provenance digest depends on its
            actual value, so the task must be computed and its output cached.

        b. There is no additional *persistable* work to do on this task. In other words,
            if we have any dependent tasks that we plan to run in a separate process,
            we can go ahead and start them; there may be more work to do on this
            task, but it will have to be done in that separate process, because its
            results can't be serialized and transmitted. (On the other hand, if we
            have a dependent task to run *in this same process*, we'll want to bring
            this task to the CACHED level instead.) As with (a), for a task with
            non-artifact output, this milestone is reached as soon as we compute
            its provenance; for a task with artifact output, it's reached only
            when the task is computed and its output is cached.

    4. CACHED: The task has been computed and its output value is stored somewhere --
        in the persistent cache, in memory on the TaskState, and/or in memory on this
        entry (depending on the cache settings). This is the final level: after this,
        there is no more work to do on this task.

    Normally an entry will only make forward progress through these levels; however,
    we do sometimes evict temporarily-memoized values, which can cause an entry to
    regress from CACHED to PRIMED.
    """

    CREATED = auto()
    INITIALIZED = auto()
    PRIMED = auto()
    CACHED = auto()


class EntryPriority(IntEnum):
    """
    Indicates a level of priority for a TaskRunnerEntry.

    When multiple entries are in the PENDING stage, an entry with higher priority will
    always be activated before one with lower priority. There are currently three
    priorities:

    1. NORMAL: Most entries will have this priority.
    2. HIGH: This is for entries that some meaningful side effect depends on. For
       example, after computing a value, we want to make sure it gets persisted to disk
       (if appropriate) before any other work happens.
    3. TOP: This is for entries that some effect depends on *and* that depends on a
       potentially large in-memory objet. For example, if we compute a tuple value, our
       first priority should be to decompose it into smaller objects, allowing the
       original tuple to be garbage-collected; until that happens, the individual
       objects can't be garbage-collected either.
    """

    NORMAL = auto()
    HIGH = auto()
    TOP = auto()


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
    func_attrs: FunctionAttributes
        Additional details about the task's `compute_func` function.
        TODO This should probably be on the Task object itself.
    desc_metadata: DescriptorMetadata
        Extra info about the descriptor whose value is produced by this task.
    """

    def __init__(
        self,
        task,
        dep_states,
        followup_states,
        func_attrs,
        desc_metadata,
    ):
        self.task = task
        self.dep_states = dep_states
        self.followup_states = followup_states
        self.func_attrs = func_attrs
        self.desc_metadata = desc_metadata

        # Cached values.
        self.task_key = task.key

        # These are set by initialize().
        self.is_initialized = False
        self._provenance = None
        self._cache_accessor = None

        # This can be set by compute(), _load_value_hash(), or
        # attempt_to_access_cached_artifact().
        # This will be present only if yields_artifact is True.
        self._result_value_hash = None

        # This can be set by get_cached_result() or compute().
        # TODO It would be nice to move this to a central in-memory cache object, like
        # context.temp_result_cache but with a longer lifetime. However, it would be
        # a little weird to move this but still have self._result_value_hash here.
        # Would it makes sense to remove the latter altogether and just retrieve it
        # lazily from self._cache_accessor?
        self._result = None

    @property
    def should_memoize(self):
        return self.desc_metadata.should_memoize

    @property
    def should_memoize_for_query(self):
        return self.desc_metadata.should_memoize_for_query

    # We could also have called this `is_artifact`, as we do in DescriptorMetadata, but
    # on this class we mostly use `is` to describe a condition that can change with
    # time.
    @property
    def yields_artifact(self):
        return self.desc_metadata.is_artifact and not self.output_would_be_missing()

    @property
    def is_cached(self):
        """
        Indicates whether the task state's result is cached.
        """
        return self._result_value_hash is not None or self._result is not None

    @property
    def is_cached_persistently(self):
        """
        Indicates whether the task state's result is cached persistently.
        """
        return self._result_value_hash is not None

    def output_would_be_missing(self):
        return self.task_key.case_key.has_missing_values

    def __repr__(self):
        return f"TaskState({self.task!r})"

    def get_cached_result(self, context):
        "Returns the result of an already-computed task state."

        assert self.is_cached

        if self._result is not None:
            context.task_key_logger.log_accessed_from_memory(self.task_key)
            return self._result

        assert self.desc_metadata.is_artifact
        value = self._cache_accessor.replicate_and_load_local_artifact()
        result = Result(
            task_key=self.task_key,
            value=value,
        )

        context.task_key_logger.log_loaded_from_disk(result.task_key)

        if self.should_memoize:
            self._result = result

        return result

    def attempt_to_access_cached_artifact(self):
        """
        Loads the hash of the cached artifact for this task, if one exists.

        If the artifact is available in the cache, this object's `is_cached`
        property will become True. Otherwise, nothing will happen.
        """
        assert self.is_initialized
        assert not self.is_cached

        if not self.yields_artifact:
            return
        if not self._cache_accessor.can_load():
            return

        self._load_value_hash()

    def refresh_all_persistent_cache_state(self, context):
        """
        Refreshes all state that depends on the persistent cache.

        This is useful if the external cache state might have changed since we last
        worked with this task.
        """

        # If this task state is not initialized or not persisted, there's nothing to
        # refresh.
        if not self.is_initialized or not self.yields_artifact:
            return

        self.refresh_cache_accessor(context)

        # If we haven't loaded anything from the cache, we can stop here.
        if self._result_value_hash is None:
            return

        # Otherwise, let's update our value hash from the cache.
        if self._cache_accessor.can_load():
            self._load_value_hash()
        else:
            self._result_value_hash = None

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

        assert self.yields_artifact

        # First, let's flush the stored entries in our cache accessor. Since we just
        # computed this entry in a subprocess, there should be a new cache entry that
        # isn't reflected yet in our local accessor.
        # (We don't just call self.refresh_cache_accessors() because we don't
        # particularly want to do the cache versioning check -- it's a little late to
        # do anything if it fails now.)
        self._cache_accessor.flush_stored_entries()

        # Then, populate the value hashes.
        if self._result_value_hash is None:
            self._load_value_hash()

    def initialize(self, context):
        "Initializes the task state to get it ready for completion."

        if self.is_initialized:
            return

        # First,  set up the provenance.
        dep_provenance_digests_by_task_key = {
            dep_key: dep_state._get_digest()
            for dep_key, dep_state in zip(self.task.dep_keys, self.dep_states)
        }

        self._provenance = Provenance.from_computation(
            task_key=self.task_key,
            code_fingerprint=self.func_attrs.code_fingerprint,
            dep_provenance_digests_by_task_key=dep_provenance_digests_by_task_key,
            treat_bytecode_as_functional=(
                context.core.versioning_policy.treat_bytecode_as_functional
            ),
            can_functionally_change_per_run=self.func_attrs.changes_per_run,
            flow_instance_uuid=context.flow_instance_uuid,
        )

        # Lastly, set up cache accessors.
        if self.yields_artifact:
            self.refresh_cache_accessor(context)

        self.is_initialized = True

    def refresh_cache_accessor(self, context):
        """
        Initializes the cache acessor for this task state.

        This sets up state that allows us to read and write cache entries for this
        task's value. This includes some in-memory representations of exernal persistent
        resources (files or cloud blobs); calling this multiple times can be necessary
        in order to wipe this state and allow it get back in sync with the real world.
        """

        self._cache_accessor = context.core.persistent_cache.get_accessor(
            task_key=self.task_key,
            provenance=self._provenance,
        )
        if context.core.versioning_policy.check_for_bytecode_errors:
            self._check_accessor_for_version_problems()

    def _check_accessor_for_version_problems(self):
        """
        Checks for any versioning errors -- i.e., any cases where a task's
        function code was updated but its version annotation was not.
        """

        old_prov = self._cache_accessor.load_provenance()
        if old_prov is None:
            return

        new_prov = self._cache_accessor.provenance
        if old_prov.exactly_matches(new_prov):
            return

        if old_prov.nominally_matches(new_prov):
            # If we have a nominal match but not an exact match, that means the
            # user must changed a function's bytecode but not its version. To report
            # this, we first need to figure out which function changed. It could be
            # the one for this task, or it could be any immediate non-artifact
            # ancestor of this one. Fortunately, each provenance contains links to each of
            # its dependency digests, and a digest of non-artifact value contains that
            # value's provenance, so we can recursively search through our ancestor
            # provenances until we find which one caused the mismatch.
            def locate_mismatched_provenances_and_raise(old_prov, new_prov):
                assert old_prov.nominally_matches(new_prov)
                # If the bytecode doesn't match, we found the problematic pair.
                if old_prov.bytecode_hash != new_prov.bytecode_hash:
                    message = f"""
                        Found a cached artifact with the same descriptor
                        ({self._cache_accessor.provenance.descriptor!r})
                        and version (major={old_prov.code_version_major!r},
                        minor={old_prov.code_version_minor!r}),
                        but created by different code.
                        It appears that the code function that outputs
                        {new_prov.descriptor}
                        was changed (old bytecode hash {old_prov.bytecode_hash!r};
                        new bytecode hash {new_prov.bytecode_hash!r})
                        but the function's version number was not.
                        Change @version(major=) to indicate that your
                        function's behavior has changed, or @version(minor=)
                        to indicate that it has *not* changed.
                        """
                    raise CodeVersioningError(oneline(message), new_prov.descriptor)
                # If the provenances nominally match, they must have essentially the
                # same structure.
                assert len(old_prov.dep_digests) == len(new_prov.dep_digests)
                # Since these provenances match nominally and have matching bytcode,
                # the mismatch must be in one of their dependencies. We'll iterate
                # through them to figure out which one.
                for old_dep_digest, new_dep_digest in zip(
                    old_prov.dep_digests, new_prov.dep_digests
                ):
                    # If this digest pair matches, it must not be where the problem is.
                    if old_dep_digest.exact_hash == new_dep_digest.exact_hash:
                        continue

                    # Not all digests have provenances, but these should. Digests of
                    # non-artifact values have provenances, and if these were artifacts
                    # then their exact hashes would be the same as their nominal hashes,
                    # so they would have matched above.
                    old_dep_prov = old_dep_digest.provenance
                    new_dep_prov = new_dep_digest.provenance
                    locate_mismatched_provenances_and_raise(old_dep_prov, new_dep_prov)
                assert False

            try:
                locate_mismatched_provenances_and_raise(old_prov, new_prov)
            except AssertionError as e:
                message = f"""
                Enncountered an internal error while performing an assisted versioning
                check. This should be impossible and is probably a bug in Bionic; please
                report this stace track to the developers. However, it's also likely
                that you need to update the ``@version`` annotation on the function
                that outputs {self._cache_accessor.provenance.descriptor}.
                If that doesn't fix the warning, you can try filtering the warning with
                ``warnings.filterwarnings``; deleting the disk cache; or disabling
                assisted versioning.
                """
                logger.warn(oneline(message), exc_info=e)

        self._cache_accessor.update_provenance()

    def _load_value_hash(self):
        """
        Reads (from disk or cloud) and saves (in memory) this task's value hash.
        """

        artifact = self._cache_accessor.load_artifact()
        if artifact is None or artifact.content_hash is None:
            raise AssertionError(
                oneline(
                    f"""
                Failed to load cached value (hash) for descriptor
                {self._cache_accessor.provenance.descriptor!r}.
                This suggests we did not successfully compute the task
                in a subprocess, or the entity wasn't cached;
                this should be impossible!"""
                )
            )
        self._result_value_hash = artifact.content_hash

    def _get_digest(self):
        if self.yields_artifact:
            assert self._result_value_hash is not None
            return ProvenanceDigest.from_value_hash(self._result_value_hash)
        else:
            assert self._provenance is not None
            return ProvenanceDigest.from_provenance(self._provenance)


class RemoteSubgraph:
    """
    Represents a subset of a task graph to be computed remotely (i.e., in another
    process).

    Given a target TaskState, this class identifies the minimal set of TaskStates that
    should be run along with it; this includes all of its immediate non-artifact
    (i.e., non-persistable) ancestors (which can't be serialized and transmitted to
    the other process) and any of their follow-up tasks (which need to be computed
    immediately after their parents). It also maintains a copy of this subgraph with
    unnecessary data pruned; these "stripped" TaskStates can be safely serialized
    with cloudpickle and sent to the other process.
    """

    def __init__(self, target_state, context):
        self._core = context.core

        self._stripped_states_by_task_key = {}
        self.persistable_but_not_persisted_states = set()
        self.non_serializable_stripped_states = set()
        self.stripped_states_with_aip_task_configs = set()

        self._strip_state(target_state)

    def _strip_state(self, original_state):
        task_key = original_state.task_key
        if task_key in self._stripped_states_by_task_key:
            return self._stripped_states_by_task_key[task_key]

        # Make a copy of the TaskState, which we'll strip down to make it easier to
        # serialize.
        # (This is a shallow copy, so we'll make sure to avoid mutating any of its
        # member variables.)
        stripped_state = copy.copy(original_state)
        self._stripped_states_by_task_key[task_key] = stripped_state

        # Strip out data cached in memory -- we can't necessarily pickle it, so
        # we need to get rid of it before trying to transmit this state to another
        # process.
        stripped_state._result = None

        # These fields are picklable, but only needed for cache setup and
        # initialization.
        if stripped_state.is_initialized:
            stripped_state.case_key = None

        if stripped_state.yields_artifact:
            assert len(stripped_state.followup_states) == 0

            if stripped_state.is_cached_persistently:
                stripped_state.task = None
                stripped_state.func_attrs = None
                stripped_state.dep_states = []

            else:
                self.persistable_but_not_persisted_states.add(original_state)

        if stripped_state.task is not None:
            if not stripped_state.task.can_be_serialized:
                self.non_serializable_stripped_states.add(stripped_state)
            if stripped_state.func_attrs.aip_task_config is not None:
                self.stripped_states_with_aip_task_configs.add(stripped_state)

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
        return self._stripped_states_by_task_key[original_state.task_key]

    @property
    def all_states_can_be_serialized(self):
        return len(self.non_serializable_stripped_states) == 0

    @property
    def distinct_aip_task_configs(self):
        return set(
            state.func_attrs.aip_task_config
            for state in self.stripped_states_with_aip_task_configs
        )


@attr.s(frozen=True)
class VacatedResult:
    """
    A fake Result.

    We use this when we're confident a result will never be accessed again and we
    want to remove its contents from memory. We use this class instead of None so that
    if someone does try to access it, the error will be more obvious.
    """

    task_key = attr.ib()
