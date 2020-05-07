import copy

from .cache import Provenance
from .datatypes import ProvenanceDigest, Query, Result
from .exception import CodeVersioningError
from .util import oneline, single_unique_element


class TaskState:
    """
    Represents the state of a task computation.  Keeps track of its position in
    the task graph, whether its values have been computed yet, additional
    intermediate state and the deriving logic.
    """

    def __init__(self, task, dep_states, case_key, provider):
        self.task = task
        self.dep_states = dep_states
        self.case_key = case_key
        self.provider = provider

        # Cached values.
        self.task_keys = task.keys
        self.should_memoize = provider.attrs.should_memoize()
        self.should_persist = (
            self.provider.attrs.should_persist() and not self.output_would_be_missing()
        )

        # These are set by initialize().
        self._is_initialized = False
        self._provenance = None
        self._queries = None
        self._cache_accessors = None

        # This can be set by complete() or _compute().
        #
        # This will be present if and only if both is_complete and
        # should_persist are True.
        self._result_value_hashes_by_name = None

        # This can be set by get_results_assuming_complete() or _compute().
        self._results_by_name = None

        # A completed task state has it's results computed and cached somewhere for
        # easy retrieval.
        self.is_complete = False

    # TODO: We need a coherent story around incomplete states between parallel
    # and non-parallel mode. We have to compute non-persisted deps inside the
    # other subprocess for parallel mode but not for non-parallel mode. We should
    # consider making the way it computes in parallel mode the default.
    def blocking_dep_states(self):
        """
        Returns all of this task state's persisted dependencies that aren't yet completed and
        non-persisted dependencies that aren't yet completable.


        Note that the definition is complicated because dependencies that cannot be
        persisted can potentially be computed again in case of parallel processing.
        But we don't want to complete the persisted entities when trying to complete
        this task state. They should already be complete to avoid doing any unnecessary
        state tracking for completing this state.
        """
        return [
            dep_state
            for dep_state in self.dep_states
            if (dep_state.should_persist and not dep_state.is_complete)
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

    def output_would_be_missing(self):
        return single_unique_element(
            task_key.case_key.has_missing_values for task_key in self.task.keys
        )

    def __repr__(self):
        return f"TaskState({self.task!r})"

    def complete(self, task_key_logger):
        """
        Ensures that a task state reaches completion -- i.e., that its results are
        available and can be retrieved. This can happen either by computing the task's
        values or by confirming that cached values already exist.
        """

        assert self.is_completable
        assert not self.is_complete

        # See if we can load it from the cache.
        if self.should_persist and all(axr.can_load() for axr in self._cache_accessors):
            # We only load the hashed result while completing task state
            # and lazily load the entire result when needed later.
            value_hashes_by_name = {}
            for accessor in self._cache_accessors:
                value_hash = accessor.load_result_value_hash()
                value_hashes_by_name[accessor.query.dnode.to_entity_name()] = value_hash

            self._result_value_hashes_by_name = value_hashes_by_name
        # If we cannot load it from cache, we compute the task state.
        else:
            self._compute(task_key_logger)

        self.is_complete = True

        return self.task_keys[0]

    def get_results_assuming_complete(self, task_key_logger):
        "Returns the results of an already-completed task state."

        assert self.is_complete

        if self._results_by_name:
            for task_key in self.task_keys:
                task_key_logger.log_accessed_from_memory(task_key)
            return self._results_by_name

        results_by_name = dict()
        for accessor in self._cache_accessors:
            result = accessor.load_result()
            task_key_logger.log_loaded_from_disk(result.query.task_key)

            # Make sure the result is saved in all caches under this exact
            # query.
            accessor.save_result(result)

            results_by_name[result.query.dnode.to_entity_name()] = result

        if self.should_memoize:
            self._results_by_name = results_by_name

        return results_by_name

    def _compute(self, task_key_logger):
        """
        Computes the values of a task state by running its task. Requires that all
        the task's dependencies are already complete.
        """

        assert self.is_completable

        task = self.task

        dep_results = []
        for dep_state, dep_key in zip(self.dep_states, task.dep_keys):
            assert dep_state.is_complete or dep_state.is_completable
            if dep_state.is_complete:
                dep_results_by_name = dep_state.get_results_assuming_complete(
                    task_key_logger
                )
            else:
                # If dep_state is not complete yet, that's probably because the results
                # aren't communicated between processes. Compute the results to populate
                # the in-memory cache.
                dep_results_by_name = dep_state._compute(task_key_logger)
            dep_results.append(dep_results_by_name[dep_key.dnode.to_entity_name()])

        if not task.is_simple_lookup:
            for task_key in self.task_keys:
                task_key_logger.log_computing(task_key)

        dep_values = [dep_result.value for dep_result in dep_results]

        # If we have any missing outputs, exit early with a missing result.
        if self.output_would_be_missing():
            results_by_name = {}
            result_value_hashes_by_name = {}
            for query in self._queries:
                result = Result(query=query, value=None, value_is_missing=True)
                entity_name = query.dnode.to_entity_name()
                results_by_name[entity_name] = result
                result_value_hashes_by_name[entity_name] = ""
            self._results_by_name = results_by_name
            self._result_value_hashes_by_name = result_value_hashes_by_name
            return self._results_by_name

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

        results_by_name = {}
        result_value_hashes_by_name = {}
        for ix, (query, value) in enumerate(zip(self._queries, values)):
            query.protocol.validate(value)

            result = Result(query=query, value=value)

            if self.should_persist:
                accessor = self._cache_accessors[ix]
                accessor.save_result(result)

                value_hash = accessor.load_result_value_hash()
                result_value_hashes_by_name[query.dnode.to_entity_name()] = value_hash

            results_by_name[query.dnode.to_entity_name()] = result

        # Memoize results at this point only if results should not persist.
        # Otherwise, load it lazily later so that if the serialized/deserialized
        # value is not exactly the same as the original, we still
        # always return the same value.
        if self.should_memoize and not self.should_persist:
            self._results_by_name = results_by_name

        # But we cache the hashed values eagerly since they are cheap to load.
        if self.should_persist:
            self._result_value_hashes_by_name = result_value_hashes_by_name

        return self._results_by_name

    def sync_after_subprocess_completion(self):
        """
        Syncs the task state by populating and reloading data in the current process
        after completing the task state in a subprocess.

        This is necessary because values populated in the task state are not communicated
        back from the subprocess.
        """

        assert self.should_persist

        # First, let's flush the stored entries in cache accessors. This is to prevent cases
        # like current process contains an outdated stored entries.
        #
        # For example: In assisted mode, main process checks for versioning error which can
        # populate the stored entries as entries without an artifact. Now subprocess can
        # update the stored entries after computing them but it won't be communicated back.
        # This allows the main process to fetch fresh entries which might have been updated.
        for accessor in self._cache_accessors:
            accessor.flush_stored_entries()

        # Then, populate the value hash.
        if self._result_value_hashes_by_name is None:
            self._result_value_hashes_by_name = {}
            for accessor in self._cache_accessors:
                value_hash = accessor.load_result_value_hash()
                name = accessor.query.dnode.to_entity_name()
                if value_hash is None:
                    raise AssertionError(
                        oneline(
                            f"""
                        Failed to load cached value (hash) for entity {name!r};
                        this suggests we did not successfully completed the entity
                        in subprocess or the entity wasn't cached which should not
                        happen."""
                        )
                    )
                self._result_value_hashes_by_name[name] = value_hash

        # Lastly, we can mark the process as complete.
        self.is_complete = True

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
                value_hash = dep_state._result_value_hashes_by_name[
                    dep_key.dnode.to_entity_name()
                ]
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
                protocol=self.provider.protocol_for_name(
                    task_key.dnode.to_entity_name()
                ),
                provenance=self._provenance,
            )
            for task_key in self.task_keys
        ]

        # Lastly, set up cache accessors.
        if self.should_persist:
            if bootstrap is None:
                name = self.task_keys[0].entity_name
                raise AssertionError(
                    oneline(
                        f"""
                    Attempting to load cached state for entity {name!r},
                    but the cache is not available yet because core bootstrap
                    entities depend on this one;
                    you should decorate entity {name!r} with `@persist(False)`
                    or `@immediate` to indicate that it can't be cached."""
                    )
                )

            self._cache_accessors = [
                bootstrap.persistent_cache.get_accessor(query)
                for query in self._queries
            ]

            if bootstrap.versioning_policy.check_for_bytecode_errors:
                self._check_accessors_for_version_problems()

        self._is_initialized = True

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
        task_state._results_by_name = None
        # Clear up fields not needed in subprocess, for computing or for cache lookup.
        task_state.provider = None
        task_state.case_key = None
        task_state._provenance = None

        # Any non-persisted value will have to be recomputed again in the other
        # subprocess and isn't complete anymore.
        if not task_state.should_persist:
            task_state.is_complete = False

        if task_state.is_complete:
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
