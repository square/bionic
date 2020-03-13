'''
Contains the core logic for resolving Entities by executing Tasks.
'''

from .datatypes import ProvenanceDigest, Query, Result, ResultGroup
from .cache import Provenance
from .exception import UndefinedEntityError, CodeVersioningError
from .optdep import import_optional_dependency
from .util import oneline

import logging
# TODO At some point it might be good to have the option of Bionic handling its
# own logging.  Probably it would manage its own logger instances and inject
# them into tasks, while providing the option of either handling the output
# itself or routing it back to the global logging system.
logger = logging.getLogger(__name__)


class EntityDeriver(object):
    """
    Derives the values of Entities.

    This is the class that constructs the entity graph and computes the value
    or values of each entity.
    """

    # --- Public API.

    def __init__(self, flow_state):
        self._flow_state = flow_state

        # This state is needed to do any resolution at all.  Once it's
        # initialized, we can use it to bootstrap the requirements for "full"
        # resolution below.
        self._is_ready_for_bootstrap_resolution = False
        self._task_lists_by_entity_name = None
        self._task_states_by_key = None

        # This state allows us to do full resolution for external callers.
        self._is_ready_for_full_resolution = False
        self._persistent_cache = None
        self._versioning_policy = None

    def get_ready(self):
        """
        Make sure this Deriver is ready to derive().  Calling this is not
        necessary but allows errors to surface earlier.
        """
        self._get_ready_for_full_resolution()

    def derive(self, entity_name):
        """
        Given an entity name, computes and returns a ResultGroup containing
        all values for that entity.
        """
        self.get_ready()
        return self._compute_result_group_for_entity_name(entity_name)

    def export_dag(self, include_core=False):
        '''
        Constructs a NetworkX graph corresponding to the DAG of tasks.  There
        is one node per task key -- i.e., for each artifact that can be created
        (uniquely defined by an entity name and a case key); and one edge from
        each task key to each key that depends on it.  Each node is represented
        by a TaskKey, and also has the following attributes:

            name: a short, unique, human-readable identifier
            entity_name: the name of the entity for this task key
            case_key: the case key for this task key
            task_ix: the task key's index in the ordered series of case keys
                     for its entity
        '''
        nx = import_optional_dependency(
            'networkx', purpose='constructing the flow DAG')

        def should_include_entity_name(name):
            return include_core or not self.entity_is_internal(entity_name)

        self.get_ready()

        graph = nx.DiGraph()

        for entity_name, tasks in (
                self._task_lists_by_entity_name.items()):
            if not should_include_entity_name(entity_name):
                continue

            if len(tasks) == 1:
                name_template = '{entity_name}'
            else:
                name_template = '{entity_name}[{task_ix}]'

            for task_ix, task in enumerate(sorted(
                    tasks, key=lambda task: task.keys[0].case_key)):
                task_key = task.key_for_entity_name(entity_name)
                state = self._task_states_by_key[task_key]

                node_name = name_template.format(
                    entity_name=entity_name, task_ix=task_ix)

                graph.add_node(
                    task_key,
                    name=node_name,
                    entity_name=entity_name,
                    case_key=task_key.case_key,
                    task_ix=task_ix,
                )

                for child_state in state.children:
                    for child_task_key in child_state.task.keys:
                        if not should_include_entity_name(
                                child_task_key.entity_name):
                            continue
                        if task_key not in child_state.task.dep_keys:
                            continue
                        graph.add_edge(task_key, child_task_key)

        return graph

    def entity_is_internal(self, entity_name):
        return entity_name.startswith('core__')

    # --- Private helpers.

    def _get_ready_for_full_resolution(self):
        if self._is_ready_for_full_resolution:
            return

        self._get_ready_for_bootstrap_resolution()

        self._persistent_cache = self._bootstrap_singleton(
            'core__persistent_cache')

        self._versioning_policy = self._bootstrap_singleton(
            'core__versioning_policy')

        self._is_ready_for_full_resolution = True

    def _get_ready_for_bootstrap_resolution(self):
        if self._is_ready_for_bootstrap_resolution:
            return

        # Generate the static key spaces and tasks for each entity.
        self._key_spaces_by_entity_name = {}
        self._task_lists_by_entity_name = {}
        for name in self._flow_state.providers_by_name.keys():
            self._populate_entity_info(name)

        # Create a state object for each task.
        self._task_states_by_key = {}
        for tasks in self._task_lists_by_entity_name.values():
            for task in tasks:
                task_state = TaskState(task)
                for key in task.keys:
                    self._task_states_by_key[key] = task_state

        # Initialize each of the task states.
        for tasks in self._task_lists_by_entity_name.values():
            for task in tasks:
                task_state = self._task_states_by_key[task.keys[0]]
                self._initialize_task_state(task_state)

        self._is_ready_for_bootstrap_resolution = True

    def _initialize_task_state(self, task_state):
        if task_state.is_initialized:
            return

        task = task_state.task

        dep_states = [
            self._task_states_by_key[dep_key]
            for dep_key in task.dep_keys
        ]

        for dep_state in dep_states:
            self._initialize_task_state(dep_state)

            task_state.parents.append(dep_state)
            dep_state.children.append(task_state)

        # All names in this task should point to the same provider.
        task_state.provider, = set(
            self._flow_state.get_provider(task_key.entity_name)
            for task_key in task.keys
        )
        # And all the task keys should have the same case key.
        task_state.case_key, = set(task_key.case_key for task_key in task.keys)

        task_state.is_initialized = True

    def _populate_entity_info(self, entity_name):
        if entity_name in self._task_lists_by_entity_name:
            return

        provider = self._flow_state.get_provider(entity_name)

        dep_names = provider.get_dependency_names()
        for dep_name in dep_names:
            self._populate_entity_info(dep_name)

        dep_key_spaces_by_name = {
            dep_name: self._key_spaces_by_entity_name[dep_name]
            for dep_name in dep_names
        }

        dep_task_key_lists_by_name = {
            dep_name: [
                task.key_for_entity_name(dep_name)
                for task in self._task_lists_by_entity_name[dep_name]
            ]
            for dep_name in dep_names
        }

        self._key_spaces_by_entity_name[entity_name] =\
            provider.get_key_space(dep_key_spaces_by_name)

        self._task_lists_by_entity_name[entity_name] = provider.get_tasks(
            dep_key_spaces_by_name,
            dep_task_key_lists_by_name)

    def _bootstrap_singleton(self, entity_name):
        result_group = self._compute_result_group_for_entity_name(
            entity_name)
        if len(result_group) == 0:
            raise ValueError(oneline(f'''
                No values were defined for internal bootstrap entity
                {entity_name!r}'''))
        if len(result_group) > 1:
            values = [result.value for result in result_group]
            raise ValueError(oneline(f'''
                Bootstrap entity {entity_name!r} must have exactly one
                value; got {len(values)} ({values!r})'''))
        return result_group[0].value

    def _compute_result_group_for_entity_name(self, entity_name):
        tasks = self._task_lists_by_entity_name.get(entity_name)
        if tasks is None:
            raise UndefinedEntityError.for_name(entity_name)
        requested_task_states = [
            self._task_states_by_key[task.keys[0]]
            for task in tasks
        ]

        ready_task_states = list(requested_task_states)

        blocked_task_key_tuples = set()

        logged_task_keys = set()

        while ready_task_states:
            state = ready_task_states.pop()

            # If this task is already complete, we don't need to do any work.
            # But if this is this is the first time we've seen this task, we
            # should should log a message .
            if state.is_complete:
                for task_key in state.task.keys:
                    if task_key not in logged_task_keys:
                        self._log(
                            'Accessed    %s from in-memory cache', task_key)
                        logged_task_keys.add(task_key)
                continue

            # If blocked, let's mark it and try to derive its parents.
            if state.is_blocked():
                ready_task_states.extend(state.parents)
                blocked_task_key_tuples.add(state.task.keys)
                continue

            # If the task isn't complete or blocked, we can complete the task.
            self._complete_task_state(state)
            for task_key in state.task.keys:
                logged_task_keys.add(task_key)

            # See if we can unblock its children after completing task state.
            for child_state in state.children:
                if child_state.task.keys in blocked_task_key_tuples and\
                        not child_state.is_blocked():
                    ready_task_states.append(child_state)
                    blocked_task_key_tuples.remove(child_state.task.keys)

        assert len(blocked_task_key_tuples) == 0, blocked_task_key_tuples
        for state in requested_task_states:
            assert state.is_complete, state

        return ResultGroup(
            results=[
                self._get_results_for_complete_task_state(state)[entity_name]
                for state in requested_task_states
            ],
            key_space=self._key_spaces_by_entity_name[entity_name],
        )

    def _complete_task_state(self, task_state):
        assert not task_state.is_blocked()
        assert not task_state.is_complete

        # First, set up provenance.
        if not self._is_ready_for_full_resolution:
            # If we're still in the bootstrap resolution phase, we don't have
            # any versioning policy, so we don't attempt anything fancy.
            treat_bytecode_as_functional = False
        else:
            treat_bytecode_as_functional =\
                self._versioning_policy.treat_bytecode_as_functional

        dep_provenance_digests_by_task_key = {}
        for dep_key, dep_state in zip(task_state.task.dep_keys, task_state.parents):
            # Use value hash of persistable values.
            if dep_state.provider.attrs.should_persist:
                value_hash = dep_state.result_value_hashes_by_name[dep_key.entity_name]
                dep_provenance_digests_by_task_key[dep_key] =\
                    ProvenanceDigest.from_value_hash(value_hash)
            # Otherwise, use the provenance.
            else:
                dep_provenance_digests_by_task_key[dep_key] =\
                    ProvenanceDigest.from_provenance(dep_state.provenance)

        task_state.provenance = Provenance.from_computation(
            code_fingerprint=task_state.provider.get_code_fingerprint(
                task_state.case_key),
            case_key=task_state.case_key,
            dep_provenance_digests_by_task_key=dep_provenance_digests_by_task_key,
            treat_bytecode_as_functional=treat_bytecode_as_functional,
        )

        # Then set up queries.
        task_state.queries = [
            Query(
                task_key=task_key,
                protocol=task_state.provider.protocol_for_name(task_key.entity_name),
                provenance=task_state.provenance
            )
            for task_key in task_state.task.keys
        ]

        # Lastly, set up cache accessors.
        if task_state.provider.attrs.should_persist:
            if not self._is_ready_for_full_resolution:
                name = task_state.task.keys[0].entity_name
                raise AssertionError(oneline(f'''
                    Attempting to load cached state for entity {name!r},
                    but the cache is not available yet because core bootstrap
                    entities depend on this one;
                    you should decorate entity {name!r} with `@persist(False)`
                    or `@immediate` to indicate that it can't be cached.'''))

            task_state.cache_accessors = [
                self._persistent_cache.get_accessor(query)
                for query in task_state.queries
            ]

            if self._versioning_policy.check_for_bytecode_errors:
                self._check_accessors_for_version_problems(task_state)

        # See if we can load it from the cache.
        if task_state.provider.attrs.should_persist and \
                all(axr.can_load() for axr in task_state.cache_accessors):
            # We only load the hashed result while completing task state
            # and lazily load the entire result when needed later.
            value_hashes_by_name = {}
            for accessor in task_state.cache_accessors:
                value_hash = accessor.load_result_value_hash()
                value_hashes_by_name[accessor.query.entity_name] = value_hash

            task_state.result_value_hashes_by_name = value_hashes_by_name
        # If we cannot load it from cache, we compute the task state.
        else:
            self._compute_task_state(task_state)

        task_state.is_complete = True

    def _check_accessors_for_version_problems(self, task_state):
        accessors_needing_saving = []
        for accessor in task_state.cache_accessors:
            old_prov = accessor.load_provenance()

            if old_prov is None:
                continue

            new_prov = accessor.query.provenance

            if old_prov.exactly_matches(new_prov):
                continue
            accessors_needing_saving.append(accessor)

            if old_prov.code_version_minor == new_prov.code_version_minor:
                if old_prov.bytecode_hash != new_prov.bytecode_hash:
                    raise CodeVersioningError(oneline(f'''
                        Found a cached artifact with the same
                        name ({accessor.query.entity_name!r}) and
                        version (major={old_prov.code_version_major!r},
                        minor={old_prov.code_version_minor!r}),
                        But created by different code
                        (old hash {old_prov.bytecode_hash!r},
                        new hash {new_prov.bytecode_hash!r}).
                        Did you change your code but not update the
                        version number?
                        Change @version(major=) to indicate that your
                        function's behavior has changed, or @version(minor=)
                        to indicate that it has *not* changed.'''))

        for accessor in accessors_needing_saving:
            accessor.update_provenance()

    def _get_results_for_complete_task_state(self, task_state):
        assert task_state.is_complete

        if task_state._results_by_name:
            return task_state._results_by_name

        results_by_name = dict()
        for accessor in task_state.cache_accessors:
            result = accessor.load_result()
            self._log(
                'Loaded      %s from disk cache', result.query.task_key)

            # Make sure the result is saved in all caches under this exact
            # query.
            accessor.save_result(result)

            results_by_name[result.query.entity_name] = result

        if task_state.provider.attrs.should_memoize:
            task_state._results_by_name = results_by_name

        return results_by_name

    def _compute_task_state(self, task_state):
        task = task_state.task
        dep_keys = task.dep_keys
        dep_results = [
            self._get_results_for_complete_task_state(
                self._task_states_by_key[dep_key])[dep_key.entity_name]
            for dep_key in dep_keys
        ]

        provider = task_state.provider

        if not task.is_simple_lookup:
            for task_key in task.keys:
                self._log('Computing   %s ...', task_key)

        dep_values = [dep_result.value for dep_result in dep_results]

        values = task_state.task.compute(dep_values)
        assert len(values) == len(provider.attrs.names)

        for query in task_state.queries:
            if task.is_simple_lookup:
                self._log('Accessed    %s from definition', query.task_key)
            else:
                self._log('Computed    %s', query.task_key)

        results_by_name = {}
        result_value_hashes_by_name = {}
        for ix, (query, value) in enumerate(zip(task_state.queries, values)):
            query.protocol.validate(value)

            result = Result(
                query=query,
                value=value,
            )

            if provider.attrs.should_persist:
                accessor = task_state.cache_accessors[ix]
                accessor.save_result(result)

                value_hash = accessor.load_result_value_hash()
                result_value_hashes_by_name[query.entity_name] = value_hash

            results_by_name[query.entity_name] = result

        # Memoize results at this point only if results should not persist.
        # Otherwise, load it lazily later so that if the serialized/deserialized
        # value is not exactly the same as the original, we still
        # always return the same value.
        if provider.attrs.should_memoize and not provider.attrs.should_persist:
            task_state._results_by_name = results_by_name

        # But we cache the hashed values eagerly since they are cheap to load.
        if provider.attrs.should_persist:
            task_state.result_value_hashes_by_name = result_value_hashes_by_name

    def _log(self, message, *args):
        if self._is_ready_for_full_resolution:
            log_level = logging.INFO
        else:
            log_level = logging.DEBUG
        logger.log(log_level, message, *args)


class TaskState(object):
    """
    Represents the state of a task computation.  Keeps track of its position in
    the task graph, whether its values have been computed yet, and additional
    intermediate state.
    """

    def __init__(self, task):
        self.task = task

        # These are set together by EntityDeriver._initialize_task_state().
        # All task states are initialized together.
        self.is_initialized = False
        self.parents = []
        self.children = []
        self.case_key = None
        self.provider = None

        # These are set by EntityDeriver._complete_task_state(), just
        # before the task state becomes eligible for cache lookup / computation.
        #
        # They will be present if and only if is_complete is True.
        self.provenance = None
        self.queries = None
        self.cache_accessors = None

        # This can be set by
        # EntityDeriver._complete_task_state() or
        # EntityDeriver._compute_task_state().
        #
        # This will be present if and only if both is_complete and
        # provider.attrs.should_persist are True.
        self.result_value_hashes_by_name = None

        # This can be set by
        # EntityDeriver._get_results_for_complete_task_state() or
        # EntityDeriver._compute_task_state().
        #
        # This should never be accessed directly, instead use
        # EntityDeriver._get_results_for_complete_task_state().
        self._results_by_name = None

        self.is_complete = False

    def is_blocked(self):
        return not all(parent.is_complete for parent in self.parents)

    def __repr__(self):
        return f'TaskState({self.task!r})'
