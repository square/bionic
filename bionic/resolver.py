'''
Contains the core logic for resolving Resources by executing Tasks.
'''

from entity import Provenance, Query, Result, ResultGroup
from exception import UndefinedResourceError


class ResourceResolver(object):
    def __init__(self, immutable_flow_state_handle):
        # TODO Do we need handle holders to distinguish between mutable and
        # immutable?  Can we just convert incoming handles to the type we want?
        self._immutable_flow_state_handle = immutable_flow_state_handle

        self._is_ready = False
        self._flow_state = None
        self._task_lists_by_resource_name = None
        self._task_states_by_key = None

    def get_ready(self):
        if self._is_ready:
            return

        self._flow_state = self._immutable_flow_state_handle.get()

        self._key_spaces_by_resource_name = {}
        self._task_lists_by_resource_name = {}
        for name in self._flow_state.resources_by_name.iterkeys():
            self._populate_resource_info(name)

        self._task_states_by_key = {
            task.key: TaskState(task)
            for tasks in self._task_lists_by_resource_name.itervalues()
            for task in tasks
        }
        for task_state in self._task_states_by_key.itervalues():
            for dep_key in task_state.task.dep_keys:
                dep_state = self._task_states_by_key[dep_key]

                task_state.parents.append(dep_state)
                dep_state.children.append(task_state)

        self._is_ready = True

    def _populate_resource_info(self, resource_name):
        if resource_name in self._task_lists_by_resource_name:
            return

        resource = self._flow_state.resources_by_name.get(resource_name)
        if resource is None:
            # TODO Consider adding some more context here?
            raise UndefinedResourceError(
                "Resource %r is not defined" % resource_name)

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

    def compute_result_group_for_resource_name(self, resource_name):
        self.get_ready()

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
        dep_values = [dep_result.value for dep_result in dep_results]

        resource = self._flow_state.resources_by_name[task.key.resource_name]
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

        task_value = task_state.task.compute(
            query=query,
            dep_values=dep_values,
        )
        task_state.result = Result(query, task_value)


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
