'''
Contains the core logic for resolving Resources by executing Tasks.
'''
from __future__ import absolute_import

from builtins import object
from .entity import Provenance, Query, Result, ResultGroup
from .exception import UndefinedResourceError

import logging
# TODO At some point it might be good to have the option of Bionic handling its
# own logging.  Probably it would manage its own logger instances and inject
# them into tasks, while providing the option of either handling the output
# itself or routing it back to the global logging system.
logger = logging.getLogger(__name__)


class ResourceResolver(object):
    # --- Public API.

    def __init__(self, flow_state):
        self._flow_state = flow_state

        # This state is needed to do any resolution at all.  Once it's
        # initialized, we can use it to bootstrap the requirements for "full"
        # resolution below.
        self._is_ready_for_bootstrap_resolution = False
        self._task_lists_by_resource_name = None
        self._task_states_by_key = None

        # This state allows us to do full resolution for external callers.
        self._is_ready_for_full_resolution = False
        self._persistent_cache = None

    def get_ready(self):
        """
        Make sure this Resolver is ready to resolve().  Calling this is not
        necessary but allows errors to surface earlier.
        """
        self._get_ready_for_full_resolution()

    def resolve(self, resource_name):
        """
        Given a resource name, computes and returns a ResultGroup containing
        all values for that resource.
        """
        self.get_ready()
        return self._compute_result_group_for_resource_name(resource_name)

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

        self._key_spaces_by_resource_name = {}
        self._task_lists_by_resource_name = {}
        for name in self._flow_state.resources_by_name.keys():
            self._populate_resource_info(name)

        self._task_states_by_key = {
            task.key: TaskState(task)
            for tasks in self._task_lists_by_resource_name.values()
            for task in tasks
        }
        for task_state in self._task_states_by_key.values():
            for dep_key in task_state.task.dep_keys:
                dep_state = self._task_states_by_key[dep_key]

                task_state.parents.append(dep_state)
                dep_state.children.append(task_state)

        self._is_ready_for_bootstrap_resolution = True

    def _populate_resource_info(self, resource_name):
        if resource_name in self._task_lists_by_resource_name:
            return

        resource = self._flow_state.get_resource(resource_name)

        dep_names = resource.get_dependency_names()
        for dep_name in dep_names:
            self._populate_resource_info(dep_name)

        dep_key_spaces_by_name = {
            dep_name: self._key_spaces_by_resource_name[dep_name]
            for dep_name in dep_names
        }

        dep_task_key_lists_by_name = {
            dep_name: [
                task.key
                for task in self._task_lists_by_resource_name[dep_name]
            ]
            for dep_name in dep_names
        }

        self._key_spaces_by_resource_name[resource_name] =\
            resource.get_key_space(dep_key_spaces_by_name)
        self._task_lists_by_resource_name[resource_name] = resource.get_tasks(
            dep_key_spaces_by_name,
            dep_task_key_lists_by_name)

    def _populate_key_spaces_for_resource_name(self, resource_name):
        if resource_name in self._task_lists_by_resource_name:
            return

    def _bootstrap_singleton(self, resource_name):
        result_group = self._compute_result_group_for_resource_name(
            resource_name)
        if len(result_group) == 0:
            raise ValueError(
                "No values were defined for internal bootstrap resource %r" %
                resource_name)
        if len(result_group) > 1:
            values = [result.value for result in result_group]
            raise ValueError(
                "Bootstrap resource %r must have exactly one value; "
                "got %d (%r)" % (resource_name, len(values), values))
        return result_group[0].value

    def _compute_result_group_for_resource_name(self, resource_name):
        tasks = self._task_lists_by_resource_name.get(resource_name)
        if tasks is None:
            raise UndefinedResourceError(
                "Resource %r is not defined" % resource_name)
        requested_task_states = [
            self._task_states_by_key[task.key]
            for task in tasks
        ]
        ready_task_states = list(requested_task_states)

        blocked_task_keys = set()

        while ready_task_states:
            state = ready_task_states.pop()

            if state.is_complete():
                continue

            if not state.is_blocked():
                self._compute_task_state(state)
                for child_state in state.children:
                    if child_state.task.key in blocked_task_keys and\
                            not child_state.is_blocked():
                        ready_task_states.append(child_state)
                        blocked_task_keys.remove(child_state.task.key)

                continue

            for dep_state in state.parents:
                if not dep_state.is_complete():
                    ready_task_states.append(dep_state)
            blocked_task_keys.add(state.task.key)

        assert len(blocked_task_keys) == 0, blocked_task_keys
        for state in requested_task_states:
            assert state.is_complete(), state

        return ResultGroup(
            results=[state.result for state in requested_task_states],
            key_space=self._key_spaces_by_resource_name[resource_name],
        )

    def _compute_task_state(self, task_state):
        assert not task_state.is_blocked()
        task = task_state.task

        dep_keys = task.dep_keys
        dep_results = [
            self._task_states_by_key[dep_key].result
            for dep_key in dep_keys
        ]

        resource = self._flow_state.get_resource(task.key.resource_name)
        case_key = task.key.case_key
        provenance = Provenance.from_computation(
            code_id=resource.get_code_id(case_key),
            case_key=case_key,
            dep_provenances_by_name={
                dep_result.query.name: dep_result.query.provenance
                for dep_result in dep_results
            },
        )
        query = Query(
            name=resource.attrs.name,
            protocol=resource.attrs.protocol,
            case_key=case_key,
            provenance=provenance,
        )

        loggable_task_str = '%s(%s)' % (
           task.key.resource_name,
           ', '.join(
               '%s=%s' % (name, value)
               for name, value in task.key.case_key.items())
        )

        should_persist = resource.attrs.should_persist
        if should_persist:
            if not self._is_ready_for_full_resolution:
                raise AssertionError(
                    "Can't apply persistent caching to bootstrap resource %r" %
                    task.key.resource_name)
            result = self._persistent_cache.load(query)
        else:
            result = None

        if result is not None:
            logger.info('Loaded    %s from cache', loggable_task_str)
        else:
            logger.info('Computing %s ...', loggable_task_str)

            dep_values = [dep_result.value for dep_result in dep_results]
            task_value = task_state.task.compute(
                query=query,
                dep_values=dep_values,
            )

            result = Result(query, task_value)

            if should_persist:
                self._persistent_cache.save(result)
                # We immediately reload the value and treat that as the real
                # value.  That way, if the serialized/deserialized value is not
                # exactly the same as the original, we still always return the
                # same value.
                result = self._persistent_cache.load(query)

            logger.info('Computed  %s', loggable_task_str)

        task_state.result = result


class TaskState(object):
    def __init__(self, task):
        self.task = task
        self.result = None
        self.parents = []
        self.children = []

    def is_complete(self):
        return self.result is not None

    def is_blocked(self):
        return not all(parent.is_complete() for parent in self.parents)

    def __repr__(self):
        return 'TaskState(%r)' % self.task
