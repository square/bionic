'''
Contains the core logic for resolving Entities by executing Tasks.
'''
from __future__ import absolute_import

from builtins import object

from .datatypes import Query, Result, ResultGroup
from .cache import Provenance
from .exception import UndefinedEntityError

import logging
# TODO At some point it might be good to have the option of Bionic handling its
# own logging.  Probably it would manage its own logger instances and inject
# them into tasks, while providing the option of either handling the output
# itself or routing it back to the global logging system.
logger = logging.getLogger(__name__)


class EntityDeriver(object):
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
        self._cache_system = None

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
        import networkx as nx

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

        self._is_ready_for_full_resolution = True

    def _get_ready_for_bootstrap_resolution(self):
        if self._is_ready_for_bootstrap_resolution:
            return

        # Generate the static key spaces and tasks for each entity.
        self._key_spaces_by_entity_name = {}
        self._task_lists_by_entity_name = {}
        for name in self._flow_state.providers_by_name.keys():
            self._populate_entity_info(name)

        # Initialize a state object for each task.
        self._task_states_by_key = {}
        for tasks in self._task_lists_by_entity_name.values():
            for task in tasks:
                task_state = TaskState(task)
                for key in task.keys:
                    self._task_states_by_key[key] = task_state

        # Connect the task states to each other in a graph.
        for tasks in self._task_lists_by_entity_name.values():
            for task in tasks:
                task_state = self._task_states_by_key[task.keys[0]]
                for dep_key in task.dep_keys:
                    dep_state = self._task_states_by_key[dep_key]

                    task_state.parents.append(dep_state)
                    dep_state.children.append(task_state)

        self._is_ready_for_bootstrap_resolution = True

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
            raise ValueError(
                "No values were defined for internal bootstrap entity %r" %
                entity_name)
        if len(result_group) > 1:
            values = [result.value for result in result_group]
            raise ValueError(
                "Bootstrap entity %r must have exactly one value; "
                "got %d (%r)" % (entity_name, len(values), values))
        return result_group[0].value

    def _compute_result_group_for_entity_name(self, entity_name):
        tasks = self._task_lists_by_entity_name.get(entity_name)
        if tasks is None:
            raise UndefinedEntityError(
                "Entity %r is not defined" % entity_name)
        requested_task_states = [
            self._task_states_by_key[task.keys[0]]
            for task in tasks
        ]
        ready_task_states = list(requested_task_states)

        blocked_task_key_tuples = set()

        logged_task_keys = set()

        def log_completed_task_exactly_once(task):
            for task_key in task.keys:
                if task_key not in logged_task_keys:
                    readable_name = self._readable_name_for_task_key(task_key)
                    self._log(
                        'Accessed    %s from in-memory cache', readable_name)
                    logged_task_keys.add(task_key)

        while ready_task_states:
            state = ready_task_states.pop()

            if state.is_complete():
                log_completed_task_exactly_once(state.task)
                continue

            if not state.is_blocked():
                self._compute_task_state(state)

                for task_key in state.task.keys:
                    logged_task_keys.add(task_key)

                for child_state in state.children:
                    if child_state.task.keys in blocked_task_key_tuples and\
                            not child_state.is_blocked():
                        ready_task_states.append(child_state)
                        blocked_task_key_tuples.remove(child_state.task.keys)

                continue

            for dep_state in state.parents:
                if dep_state.is_complete():
                    log_completed_task_exactly_once(dep_state.task)
                else:
                    ready_task_states.append(dep_state)
            blocked_task_key_tuples.add(state.task.keys)

        assert len(blocked_task_key_tuples) == 0, blocked_task_key_tuples
        for state in requested_task_states:
            assert state.is_complete(), state

        return ResultGroup(
            results=[
                state.results_by_name[entity_name]
                for state in requested_task_states
            ],
            key_space=self._key_spaces_by_entity_name[entity_name],
        )

    def _compute_task_state(self, task_state):
        assert not task_state.is_blocked()
        task = task_state.task

        dep_keys = task.dep_keys
        dep_results = [
            self._task_states_by_key[dep_key]
                .results_by_name[dep_key.entity_name]
            for dep_key in dep_keys
        ]

        # All names should point to the same provider.
        provider, = set(
            self._flow_state.get_provider(task_key.entity_name)
            for task_key in task.keys
        )
        # And all the task keys should have the same case key.
        case_key, = set(task_key.case_key for task_key in task.keys)
        provenance = Provenance.from_computation(
            code_id=provider.get_code_id(case_key),
            case_key=case_key,
            dep_provenances_by_name={
                dep_result.query.entity_name: dep_result.query.provenance
                for dep_result in dep_results
            },
        )

        query_states = [
            QueryState(
                query=Query(
                    entity_name=task_key.entity_name,
                    protocol=provider.protocol_for_name(task_key.entity_name),
                    case_key=case_key,
                    provenance=provenance,
                    readable_name=self._readable_name_for_task_key(task_key),
                ),
            )
            for task_key in task.keys
        ]

        # If persisting, attempt to load each query.
        should_persist = provider.attrs.should_persist

        if should_persist:
            if not self._is_ready_for_full_resolution:
                raise AssertionError(
                    "Can't apply persistent caching to bootstrap entities %r"
                    % (tuple(provider.attrs.names),))
            cache = self._persistent_cache

            for query_state in query_states:
                query_state.result = cache.load(query_state.query)

        # If not all entities were already cached, we'll have to compute them.
        all_results_present = all(
            query_state.result is not None
            for query_state in query_states
        )
        if all_results_present:
            for query_state in query_states:
                self._log(
                    'Loaded      %s from %s cache',
                    query_state.readable_name,
                    query_state.result.cache_source_name,
                )

                # Even if it was in the cache, we should write it back so it
                # gets replicated to all tiers (local and cloud).
                # TODO Should this be the cache's responsibility?
                self._persistent_cache.save(query_state.result)

        else:
            if not task.is_simple_lookup:
                for query_state in query_states:
                    self._log('Computing   %s ...', query_state.readable_name)

            dep_values = [dep_result.value for dep_result in dep_results]

            values = task_state.task.compute(dep_values)
            assert len(values) == len(provider.attrs.names)

            for value, query_state in zip(values, query_states):
                query_state.query.protocol.validate(value)

                result = Result(
                    query=query_state.query,
                    value=value,
                )
                if should_persist:
                    cache.save(result)
                    # We immediately reload the value and treat that as the
                    # real value.  That way, if the serialized/deserialized
                    # value is not exactly the same as the original, we still
                    # always return the same value.
                    result = cache.load(query_state.query)

                query_state.result = result

            if task.is_simple_lookup:
                self._log(
                    'Accessed    %s from definition',
                    query_state.readable_name)
            else:
                self._log('Computed    %s', query_state.readable_name)

        # Collect the results.
        task_state.results_by_name = {
            query_state.query.entity_name: query_state.result
            for query_state in query_states
        }

    def _readable_name_for_task_key(self, task_key):
        return '%s(%s)' % (
           task_key.entity_name,
           ', '.join(
               '%s=%s' % (name, value)
               for name, value in task_key.case_key.items())
        )

    def _log(self, message, *args):
        if self._is_ready_for_full_resolution:
            log_level = logging.INFO
        else:
            log_level = logging.DEBUG
        logger.log(log_level, message, *args)


class TaskState(object):
    """
    Represents the state of a task computation.  Keeps track of its position
    in the task graph and whether its values have been computed yet.
    """

    def __init__(self, task):
        self.task = task
        self.results_by_name = None
        self.parents = []
        self.children = []

    def is_complete(self):
        return self.results_by_name is not None

    def is_blocked(self):
        return not all(parent.is_complete() for parent in self.parents)

    def __repr__(self):
        return 'TaskState(%r)' % self.task


class QueryState(object):
    """Tracks a query that we may or may not have computed yet."""

    def __init__(self, query):
        self.query = query
        self.readable_name = self.query.readable_name

        # This will get written later.
        self.result = None

    def __repr__(self):
        return 'QueryState(%r)' % (self.readable_name,)
