import copy

from .datatypes import ProvenanceDigest, Query, Result
from .exception import CodeVersioningError
from .persistence import Provenance
from .util import oneline, single_unique_element

import logging
import warnings

logger = logging.getLogger(__name__)


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

        # These are set by initialize().
        self._is_initialized = False
        self._provenance = None
        self._queries = None
        self._cache_accessors = None
        self.should_memoize = None
        self.should_persist = None

        # This can be set by compute() or attempt_to_complete_from_cache().
        #
        # This will be present only if should_persist is True.
        self._result_value_hashes_by_dnode = None

        # This can be set by get_cached_results() or compute().
        self._results_by_dnode = None

    # TODO: We need a coherent story around incomplete states between parallel
    # and non-parallel mode. We have to compute non-persisted deps inside the
    # other subprocess for parallel mode but not for non-parallel mode. We should
    # consider making the way it computes in parallel mode the default.
    def blocking_dep_states(self):
        """
        Returns all of this task state's persisted dependencies that aren't yet completed and
        non-persisted dependencies that aren't yet completable.


        Note that the definition is complicated because dependencies that cannot be
        persisted can potentially be computed again in case of parallel execution.
        But we don't want to complete the persisted entities when trying to complete
        this task state. They should already be complete to avoid doing any unnecessary
        state tracking for completing this state.
        """
        return [
            dep_state
            for dep_state in self.dep_states
            if (dep_state.should_persist and not dep_state.is_cached)
            or not dep_state.is_completable
        ]

    def blocking_dep_task_keys(self):
        blocking_dep_states = self.blocking_dep_states()
        return set(
            blocking_dep_state_tk
            for blocking_dep_state in blocking_dep_states
            for blocking_dep_state_tk in blocking_dep_state.task_keys
        )

    @property
    def is_completable(self):
        """
        Indicates whether the task state can be completed in its current shape.
        True when the state is initialized and doesn't have any blocking deps.
        """
        return self._is_initialized and len(self.blocking_dep_states()) == 0

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
        "Returns the results of an already-completed task state."

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

    def attempt_to_complete_from_cache(self):
        """
        If the results are available in persistent cache, populates value hashes
        and marks the task state complete. Otherwise, it does nothing.
        """
        assert self.is_completable
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
        if not self._is_initialized or not self.should_persist:
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

    def compute(self, task_key_logger, return_results=False):
        """
        Computes the values of a task state by running its task. Requires that all
        the task's dependencies are already complete.

        Returns a dict of computed results by dnode when return_results is True.
        """

        assert self.is_completable
        assert not self.is_cached

        task = self.task

        dep_results = []
        for dep_state, dep_key in zip(self.dep_states, task.dep_keys):
            assert dep_state.is_cached or dep_state.is_completable
            if dep_state.is_cached:
                dep_results_by_dnode = dep_state.get_cached_results(task_key_logger)
            else:
                # If dep_state is not complete yet, that's probably because the results
                # aren't communicated between processes. Compute the results.
                dep_results_by_dnode = dep_state.compute(
                    task_key_logger, return_results=True
                )
            dep_results.append(dep_results_by_dnode[dep_key.dnode])

        if not task.is_simple_lookup:
            for task_key in self.task_keys:
                task_key_logger.log_computing(task_key)

        dep_values = [dep_result.value for dep_result in dep_results]

        # If we have any missing outputs, exit early with a missing result.
        if self.output_would_be_missing():
            results_by_dnode = {}
            result_value_hashes_by_dnode = {}
            for query in self._queries:
                result = Result(query=query, value=None, value_is_missing=True)
                results_by_dnode[query.dnode] = result
                result_value_hashes_by_dnode[query.dnode] = ""
            self._results_by_dnode = results_by_dnode
            self._result_value_hashes_by_dnode = result_value_hashes_by_dnode
            return self._results_by_dnode

        else:
            # If we have no missing outputs, we should not be consuming any missing
            # inputs either.
            assert not any(
                dep_key.case_key.has_missing_values for dep_key in task.dep_keys
            )

        values = task.compute(dep_values)
        assert len(values) == len(self.task_keys)

        for query in self._queries:
            if task.is_simple_lookup:
                task_key_logger.log_accessed_from_definition(query.task_key)
            else:
                task_key_logger.log_computed(query.task_key)

        results_by_dnode = {}
        result_value_hashes_by_dnode = {}
        for ix, (query, value) in enumerate(zip(self._queries, values)):
            query.protocol.validate(value)

            result = Result(query=query, value=value)

            if self.should_persist:
                accessor = self._cache_accessors[ix]
                accessor.save_result(result)

                value_hash = accessor.load_result_value_hash()
                result_value_hashes_by_dnode[query.dnode] = value_hash

            results_by_dnode[query.dnode] = result

        # We cache the hashed values eagerly since they are cheap to load.
        if self.should_persist:
            self._result_value_hashes_by_dnode = result_value_hashes_by_dnode
        # Memoize results at this point only if results should not persist.
        # Otherwise, load it lazily later so that if the serialized/deserialized
        # value is not exactly the same as the original, we still
        # always return the same value.
        elif self.should_memoize:
            self._results_by_dnode = results_by_dnode

        if return_results:
            if self.is_cached:
                return self.get_cached_results(task_key_logger)
            return results_by_dnode

    def sync_after_subprocess_completion(self):
        """
        Syncs the task state by populating and reloading data in the current process
        after completing the task state in a subprocess.

        This is necessary because values populated in the task state are not communicated
        back from the subprocess.
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

        if self._is_initialized:
            return

        # First, set up provenance.
        if bootstrap is None:
            # If we're still in the bootstrap resolution phase, we don't have
            # any versioning policy, so we don't attempt anything fancy.
            treat_bytecode_as_functional = False
        else:
            treat_bytecode_as_functional = (
                bootstrap.versioning_policy.treat_bytecode_as_functional
            )

        dep_provenance_digests_by_task_key = {}
        for dep_key, dep_state in zip(self.task.dep_keys, self.dep_states):
            # Use value hash of persistable values.
            if dep_state.should_persist:
                value_hash = dep_state._result_value_hashes_by_dnode[dep_key.dnode]
                dep_provenance_digests_by_task_key[
                    dep_key
                ] = ProvenanceDigest.from_value_hash(value_hash)
            # Otherwise, use the provenance.
            else:
                dep_provenance_digests_by_task_key[
                    dep_key
                ] = ProvenanceDigest.from_provenance(dep_state._provenance)

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

        # In theory different entities for a single task could have different cache
        # settings, but I'm not sure it can happen in practice (given the way
        # grouped entities are created). At any rate, once we have tuple
        # descriptors, each task state will only be responsible for a single entity
        # and this won't be an issue.
        optional_should_memoize, can_persist = single_unique_element(
            (entity_def.optional_should_memoize, entity_def.can_persist)
            for entity_def in self.entity_defs_by_dnode.values()
        )

        should_memoize = optional_should_memoize
        if should_memoize is None:
            if bootstrap is None:
                should_memoize = True
            else:
                should_memoize = bootstrap.should_memoize_default
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

        should_persist = can_persist and not self.output_would_be_missing()
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
            logging.warn(message)
            should_persist = False
        self.should_persist = should_persist

        # Lastly, set up cache accessors.
        if self.should_persist:
            self.refresh_cache_accessors(bootstrap)

        self._is_initialized = True

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
                    This suggests we did not successfully complete the task
                    in subprocess, or the entity wasn't cached;
                    this should be impossible!"""
                    )
                )
            result_value_hashes_by_dnode[accessor.query.dnode] = value_hash
        self._result_value_hashes_by_dnode = result_value_hashes_by_dnode

    def strip_state_for_subprocess(self, new_task_states_by_key=None):
        """
        Returns a copy of task state after keeping only the necessary data
        required for completion. In addition, this also removes all the memoized
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
            task_state._queries = None
        else:
            task_state.dep_states = [
                dep_state.strip_state_for_subprocess(new_task_states_by_key)
                for dep_state in task_state.dep_states
            ]

        new_task_states_by_key[task_state.task_keys[0]] = task_state
        return task_state
