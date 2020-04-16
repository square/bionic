import copy

from .cache import Provenance
from .datatypes import ProvenanceDigest, Query, Result
from .exception import CodeVersioningError
from .util import oneline


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

        # These are set by initialize().
        self._is_initialized = False
        self._provenance = None
        self._queries = None
        self._cache_accessors = None

        # This can be set by complete() or _compute().
        #
        # This will be present if and only if both is_complete and
        # provider.attrs.should_persist() are True.
        self._result_value_hashes_by_name = None

        # This can be set by get_results_assuming_complete() or _compute().
        self._results_by_name = None

        self.is_complete = False

    def incomplete_dep_states(self):
        return [dep_state for dep_state in self.dep_states if not dep_state.is_complete]

    @property
    def is_blocked(self):
        return len(self.incomplete_dep_states()) > 0

    def __repr__(self):
        return f"TaskState({self.task!r})"

    def complete(self, task_key_logger):
        """
        Ensures that a task state reaches completion -- i.e., that its results are
        available and can be retrieved or can be readily computed. The results can
        be fetched from `self.get_results_assuming_complete()`.
        """

        assert self._is_initialized
        assert not self.is_blocked
        assert not self.is_complete

        # See if we can load it from the cache.
        if self.provider.attrs.should_persist() and all(
            axr.can_load() for axr in self._cache_accessors
        ):
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

    def get_results_assuming_complete(self, task_key_logger):
        """
        Returns the results of an already-completed task state. This can happen
        by returning the cached results if they exist, or by computing the results
        using `self._compute()` and returning them.
        """

        assert self.is_complete

        # If task state should persist but results aren't cached, that's probably
        # because the results aren't communicated between processes. Compute the
        # results to populate in memory cache.
        if not self.provider.attrs.should_persist() and not self._results_by_name:
            self._compute(task_key_logger)

        if self._results_by_name:
            for task_key in self.task.keys:
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

        if self.provider.attrs.should_memoize():
            self._results_by_name = results_by_name

        return results_by_name

    def _compute(self, task_key_logger):
        """
        Computes the values of a task state by running its task. Requires that all
        the task's dependencies are already complete.
        """

        task = self.task

        dep_results = [
            dep_state.get_results_assuming_complete(task_key_logger)[
                dep_key.dnode.to_entity_name()
            ]
            for dep_state, dep_key in zip(self.dep_states, task.dep_keys)
        ]

        provider = self.provider

        if not task.is_simple_lookup:
            for task_key in task.keys:
                task_key_logger.log_computing(task_key)

        dep_values = [dep_result.value for dep_result in dep_results]

        values = task.compute(dep_values)
        assert len(values) == len(provider.attrs.names)

        for query in self._queries:
            if task.is_simple_lookup:
                task_key_logger.log_accessed_from_definition(query.task_key)
            else:
                task_key_logger.log_computed(query.task_key)

        results_by_name = {}
        result_value_hashes_by_name = {}
        for ix, (query, value) in enumerate(zip(self._queries, values)):
            query.protocol.validate(value)

            result = Result(query=query, value=value,)

            if provider.attrs.should_persist():
                accessor = self._cache_accessors[ix]
                accessor.save_result(result)

                value_hash = accessor.load_result_value_hash()
                result_value_hashes_by_name[query.dnode.to_entity_name()] = value_hash

            results_by_name[query.dnode.to_entity_name()] = result

        # Memoize results at this point only if results should not persist.
        # Otherwise, load it lazily later so that if the serialized/deserialized
        # value is not exactly the same as the original, we still
        # always return the same value.
        if provider.attrs.should_memoize() and not provider.attrs.should_persist():
            self._results_by_name = results_by_name

        # But we cache the hashed values eagerly since they are cheap to load.
        if provider.attrs.should_persist():
            self._result_value_hashes_by_name = result_value_hashes_by_name

    def sync_after_subprocess_completion(self):
        """
        Syncs the task state by populating and reloading data in the current process
        after completing the task state in a subprocess.

        This is necessary because values populated in the task state are not communicated
        back from the subprocess.
        """

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
            if dep_state.provider.attrs.should_persist():
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
            for task_key in self.task.keys
        ]

        # Lastly, set up cache accessors.
        if self.provider.attrs.should_persist():
            if bootstrap is None:
                name = self.task.keys[0].entity_name
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

    # NOTE: This can be optimized further.
    def new_state_for_completion(self, new_task_states_by_key):
        """
        Returns a copy of task state after keeping only the necessary data
        required for completion. Along with keeping only necessary data for
        completion, this also removes all the memoized results since they can
        be expensive to serialize.

        Mainly used to reduce IPC overhead when sending the state over to a
        subprocess.

        Parameters
        ----------

        new_task_states_by_key: Dict from key to new task states.
            The cache for tracking new task states. Should be an empty dict when
            first called.
        """

        # All task keys should point to the same task state.
        if self.task.keys[0] in new_task_states_by_key:
            return new_task_states_by_key[self.task.keys[0]]

        # Let's make a copy of the task state.
        # Note that this is not a deep copy so don't mutate so be careful when
        # mutating state variables.
        task_state = copy.copy(self)
        task_state._results_by_name = None

        new_dep_states = []
        for dep_state in task_state.dep_states:
            new_dep_state = dep_state.new_state_for_completion(new_task_states_by_key)
            new_dep_states.append(new_dep_state)
        task_state.dep_states = new_dep_states

        new_task_states_by_key[task_state.task.keys[0]] = task_state
        return task_state
