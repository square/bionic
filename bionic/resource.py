'''
Contains Bionic's concept of a "resource": an element of a workflow that can
have one or more values, possible depending on other resources.  This module
includes a BaseResource class and various subclasses.

The whole architecture is a bit of a mess and probably needs a substantial
rethink.
'''

import inspect
from copy import copy
from collections import defaultdict
from entity import Task, TaskKey
import functools

import pandas as pd

from entity import CaseKey, CaseKeySpace, Result
from util import groups_dict

import logging
logger = logging.getLogger(__name__)


class ResourceAttributes(object):
    def __init__(self, name, protocol=None, code_version=None):
        self.name = name
        self.protocol = protocol
        self.code_version = code_version


class BaseResource(object):
    def __init__(self, attrs, is_mutable=False):
        self.attrs = attrs
        self.is_mutable = is_mutable

    def get_code_id(self, case_key):
        return 'code_version=%s' % self.attrs.code_version

    def get_dependency_names(self):
        return []

    def get_key_space(self, dep_key_spaces_by_name):
        raise NotImplementedError()

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        raise NotImplementedError()

    def get_source_func(self):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()

    def copy_if_mutable(self):
        if self.is_mutable:
            return self.copy()
        else:
            return self

    def __repr__(self):
        return '%s(%r))' % (self.__class__.__name__, self.attrs.name)


class WrappingResource(BaseResource):
    def __init__(self, wrapped_resource):
        if wrapped_resource.is_mutable:
            raise ValueError(
                "Can only wrap immutable resources; got mutable resource %r" %
                wrapped_resource)
        super(WrappingResource, self).__init__(wrapped_resource.attrs)
        self.wrapped_resource = wrapped_resource

    def get_code_id(self, case_key):
        return self.wrapped_resource.get_code_id(case_key)

    def get_dependency_names(self):
        return self.wrapped_resource.get_dependency_names()

    def get_key_space(self, dep_key_spaces_by_name):
        return self.wrapped_resource.get_key_space(dep_key_spaces_by_name)

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        return self.wrapped_resource.get_tasks(
            dep_key_spaces_by_name, dep_task_key_lists_by_name)

    def get_source_func(self):
        return self.wrapped_resource.get_source_func()

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self.wrapped_resource)


class AttrUpdateResource(WrappingResource):
    def __init__(self, wrapped_resource, attr_name, attr_value):
        super(AttrUpdateResource, self).__init__(wrapped_resource)

        old_attr_value = getattr(wrapped_resource.attrs, attr_name)
        if old_attr_value is not None:
            raise ValueError(
                "Attempted to set attribute %r twice on %r; old value was %r, "
                "new value is %r" % (
                    attr_name, wrapped_resource, old_attr_value, attr_value))

        self.attrs = copy(wrapped_resource.attrs)
        setattr(self.attrs, attr_name, attr_value)


class VersionedResource(WrappingResource):
    def __init__(self, wrapped_resource, version):
        assert wrapped_resource.attrs.code_version is None
        super(VersionedResource, self).__init__(wrapped_resource)
        self._version = version

    def get_code_id(self, case_key):
        return 'code_version=%s' % self._version


class ValueResource(BaseResource):
    def __init__(self, name, protocol):
        super(ValueResource, self).__init__(
            attrs=ResourceAttributes(name=name, protocol=protocol),
            is_mutable=True,
        )

        self.clear_cases()

    def copy(self):
        resource = ValueResource(self.attrs.name, self.attrs.protocol)
        resource.key_space = self.key_space
        resource._has_any_values = self._has_any_values
        resource._values_by_case_key = self._values_by_case_key.copy()
        resource._code_ids_by_case_key = self._code_ids_by_case_key.copy()
        return resource

    def clear_cases(self):
        self.key_space = CaseKeySpace()
        self._has_any_values = False
        self._values_by_case_key = {}
        self._code_ids_by_case_key = {}

    def check_can_add_case(self, case_key, value):
        self.attrs.protocol.validate(value)

        if self._has_any_values:
            if case_key.space != self.key_space:
                raise ValueError(
                    "Can't add %r to resource %r: key space doesn't match %r" %
                    (case_key, self.attrs.name, self.key_space))

            if case_key in self._values_by_case_key:
                raise ValueError(
                    "Can't add %r to resource %r; that case key already exists"
                    % (case_key, self.attrs.name))

    def add_case(self, case_key, value):
        code_id = self.attrs.protocol.write_to_str(value)

        if not self._has_any_values:
            self.key_space = case_key.space
            self._has_any_values = True

        self._values_by_case_key[case_key] = value
        self._code_ids_by_case_key[case_key] = code_id

    def get_code_id(self, case_key):
        return self._code_ids_by_case_key[case_key]

    def get_key_space(self, dep_key_spaces_by_name):
        assert not dep_key_spaces_by_name
        return self.key_space

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        assert not dep_key_spaces_by_name
        assert not dep_task_key_lists_by_name

        return [
            Task(
                key=TaskKey(
                    resource_name=self.attrs.name,
                    case_key=case_key,
                ),
                dep_keys=[],
                compute_func=functools.partial(
                    self._compute,
                    case_key=case_key,
                ),
            )
            for case_key in self._values_by_case_key.iterkeys()
        ]

    def _compute(self, query, dep_values, case_key):
        return self._values_by_case_key[case_key]


class FunctionResource(BaseResource):
    def __init__(self, func):
        super(FunctionResource, self).__init__(attrs=ResourceAttributes(
            name=func.func_name))

        self._func = func

        argspec = inspect.getargspec(func)
        if argspec.varargs:
            raise ValueError('Functions with varargs are not supported')
        if argspec.keywords:
            raise ValueError('Functions with keyword args are not supported')
        self._dep_names = list(argspec.args)

    def get_dependency_names(self):
        return self._dep_names

    def get_source_func(self):
        return self._func

    def get_key_space(self, dep_key_spaces_by_name):
        return CaseKeySpace.union_all(dep_key_spaces_by_name.values())

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        dep_case_key_lists = [
            [
                task_key.case_key
                for task_key in dep_task_key_lists_by_name[name]
            ]
            for name in self._dep_names
        ]
        out_case_keys = self._merge_case_key_lists(dep_case_key_lists)

        if len(out_case_keys) == 0:
            return []

        return [
            Task(
                key=TaskKey(self.attrs.name, case_key),
                dep_keys=[
                    TaskKey(
                        dep_name,
                        case_key.project(dep_key_spaces_by_name[dep_name]),
                    )
                    for dep_name in self._dep_names
                ],
                compute_func=self._apply,
            )
            for case_key in out_case_keys
        ]

    def _merge_case_key_lists(self, case_key_lists):
        merged_case_keys = [CaseKey()]
        merged_key_space = CaseKeySpace()

        for cur_case_keys in case_key_lists:
            # If any dependency has no keys, the entire Cartesian product must
            # be empty.
            if len(cur_case_keys) == 0:
                return []

            # Find the key space of the current dependency's keys.
            cur_key_space = cur_case_keys[0].space

            # Identify the names shared with already-merged keys.
            common_key_space = cur_key_space.intersection(merged_key_space)

            # Group the current keys by their common parts.
            cur_key_lists_by_common_key = groups_dict(
                cur_case_keys, common_key_space.select)

            # Likewise, group the already-merged keys.
            merged_key_lists_by_common_key = groups_dict(
                merged_case_keys, common_key_space.select)

            # For each distinct common key, take the Cartesian product of the
            # new and already-merged keys.
            merged_case_keys = []
            for common_key, merged_keys in\
                    merged_key_lists_by_common_key.iteritems():
                for cur_key in cur_key_lists_by_common_key.get(common_key, []):
                    for merged_key in merged_keys:
                        new_merged_key = merged_key.merge(cur_key)
                        merged_case_keys.append(new_merged_key)

            merged_key_space = merged_key_space.union(cur_key_space)

        return merged_case_keys

    def _apply(self, query, dep_values):
        value = self._func(*dep_values)
        query.protocol.validate(value)
        return value

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._func)


class StorageCachedResource(WrappingResource):
    def __init__(self, wrapped_resource, cache_name='core__storage_cache'):
        super(StorageCachedResource, self).__init__(wrapped_resource)
        self._cache_name = cache_name

    def get_dependency_names(self):
        wrapped_dep_names = list(self.wrapped_resource.get_dependency_names())
        # TODO In theory we could handle this gracefully, but I'm not sure it's
        # likely to come up.
        assert self._cache_name not in wrapped_dep_names
        return [self._cache_name] + wrapped_dep_names

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        dep_task_key_lists_by_name = dict(dep_task_key_lists_by_name)
        cache_task_keys = dep_task_key_lists_by_name.pop(self._cache_name)

        wrapped_tasks = self.wrapped_resource.get_tasks(
            dep_key_spaces_by_name,
            dep_task_key_lists_by_name)

        return [
            Task(
                key=task.key,
                dep_keys=([cache_task_key] + task.dep_keys),
                compute_func=functools.partial(
                    self._compute_task_using_cache,
                    task=task,
                ),
            )
            for task in wrapped_tasks
            for cache_task_key in cache_task_keys
        ]

    def _compute_task_using_cache(self, query, dep_values, task):
        dep_values = list(dep_values)
        cache = dep_values.pop(0)

        result = cache.load(query)
        if result is None:
            value = task.compute(query, dep_values)
            result = Result(query, value)
            cache.save(result)

            # We immediately reload the value and treat that as the real value.
            # That way, if the serialized/deserialized value is not exactly the
            # same as the original, we still always return the same value.
            result = cache.load(query)
            assert result is not None

        return result.value


# TODO Consider allowing multiple gathered_dep_names?
class GatherResource(WrappingResource):
    def __init__(
            self, wrapped_resource,
            gather_over_names, gathered_dep_name, new_gathered_dep_name):

        super(GatherResource, self).__init__(wrapped_resource)

        self._gather_over_names = gather_over_names
        self._outer_gathered_dep_name = gathered_dep_name
        self._inner_gathered_dep_name = new_gathered_dep_name

        self._inner_dep_names = self.wrapped_resource.get_dependency_names()

        self._all_outer_deps_to_gather = list(self._gather_over_names)
        if self._outer_gathered_dep_name not in self._all_outer_deps_to_gather:
            self._all_outer_deps_to_gather.append(
                self._outer_gathered_dep_name)

        gather_dep_ix = self._inner_dep_names.index(
            self._inner_gathered_dep_name)
        if gather_dep_ix < 0:
            raise ValueError(
                "Expected wrapped %r to have dependency name %r, but "
                "only found names %r" % (
                    self.wrapped_resource, self._inner_gathered_dep_name,
                    self._inner_dep_names))

        modified_inner_dep_names = list(self._inner_dep_names)
        modified_inner_dep_names[gather_dep_ix] = self._outer_gathered_dep_name

        extra_dep_names = [
            name for name in self._gather_over_names
            if name not in modified_inner_dep_names
        ]

        self._outer_dep_names = \
            extra_dep_names + modified_inner_dep_names

    def get_dependency_names(self):
        return self._outer_dep_names

    def get_key_space(self, dep_key_spaces_by_name):
        unmodified_dep_spaces = [
            dep_key_spaces_by_name[name]
            for name in self._inner_dep_names
            if name != self._inner_gathered_dep_name
        ]
        gathering_key_spaces = [
            dep_key_spaces_by_name[name]
            for name in self._gather_over_names
        ]
        full_gathered_key_space = dep_key_spaces_by_name[
            self._outer_gathered_dep_name]

        total_gathering_key_space = CaseKeySpace.union_all(
            gathering_key_spaces)
        collapsed_gathered_key_space = full_gathered_key_space.difference(
            total_gathering_key_space)

        return CaseKeySpace.union_all(
            unmodified_dep_spaces + [collapsed_gathered_key_space])

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        # These are the key spaces and task keys that the outside world sees.
        outer_key_spaces_by_name = dep_key_spaces_by_name
        outer_dtkls = dep_task_key_lists_by_name

        gathering_key_space = CaseKeySpace.union_all(
            dep_key_spaces_by_name[name]
            for name in self._gather_over_names
        )

        # Take the primary dependency task keys, and group them by their outer
        # case keys.  (Keys in the same group will be gathered into the same
        # task.)
        gathering_dep_task_keys = outer_dtkls[self._outer_gathered_dep_name]
        primary_tkls_by_outer_case_key = defaultdict(list)
        for task_key in gathering_dep_task_keys:
            outer_case_key = task_key.case_key.drop(gathering_key_space)
            primary_tkls_by_outer_case_key[outer_case_key].append(task_key)

        # Create new task keys for the gathered values that the inner resource
        # will consume.
        inner_gathered_dep_task_keys = [
            TaskKey(
                resource_name=self._inner_gathered_dep_name,
                case_key=case_key,
            )
            for case_key in primary_tkls_by_outer_case_key.iterkeys()
        ]
        full_gathered_key_space = \
            outer_key_spaces_by_name[self._outer_gathered_dep_name]
        inner_gathered_key_space = full_gathered_key_space\
            .difference(gathering_key_space)
        active_gathering_key_space = gathering_key_space\
            .intersection(full_gathered_key_space)

        # Now we can construct the dicts of key spaces and task keys that the
        # wrapper resource will see.
        inner_key_spaces_by_name = {
            name: (
                inner_gathered_key_space
                if name == self._inner_gathered_dep_name else
                outer_key_spaces_by_name[name]
            ) for name in self._inner_dep_names
        }

        inner_dtkls = {
            name: (
                inner_gathered_dep_task_keys
                if name == self._inner_gathered_dep_name else
                outer_dtkls[name]
            ) for name in self._inner_dep_names
        }

        # Define how we'll convert an inner task to an outer task.
        def wrap_task(task):
            # Identify the index of the inner task key that corresponds to the
            # aggregated value.  That's the one we'll be replacing.
            inner_dep_keys = task.dep_keys
            gather_task_key_ix, = [
                ix
                for ix, dep_key in enumerate(inner_dep_keys)
                if dep_key.resource_name == self._inner_gathered_dep_name
            ]

            # Remove the key for the aggregated value, and remember its case
            # key.
            passthrough_dep_keys = list(inner_dep_keys)
            inner_gather_case_key = passthrough_dep_keys\
                .pop(gather_task_key_ix).case_key

            outer_gather_case_key = inner_gather_case_key\
                .drop(gathering_key_space)

            # Assemble the primary dep keys for this particular case key.
            primary_gathered_task_keys = \
                primary_tkls_by_outer_case_key[outer_gather_case_key]

            all_gather_over_keys = [
                task_key
                for dep_name in self._all_outer_deps_to_gather
                for task_key in outer_dtkls[dep_name]
            ]

            # Combine the gathering task keys with the keys expected by the
            # wrapped task (except the one key we removed, since we'll be
            # synthesizing it ourselves).
            prepended_keys = all_gather_over_keys + primary_gathered_task_keys
            wrapped_dep_keys = prepended_keys + passthrough_dep_keys

            def wrapped_compute_func(query, dep_values):
                # Split off the extra values for the keys we prepended.
                prepended_values = dep_values[:len(prepended_keys)]
                passthrough_values = dep_values[len(prepended_keys):]

                values_by_task_key = dict(zip(
                    prepended_keys, prepended_values))

                # Gather the prepended values into a single frame.
                df_row_case_keys = [
                    task_key.case_key
                    for task_key in primary_gathered_task_keys
                ]

                if len(active_gathering_key_space) > 0:
                    df_index = multi_index_from_case_keys(
                        case_keys=df_row_case_keys,
                        ordered_key_names=list(active_gathering_key_space))
                else:
                    df_index = None
                gathered_df = pd.DataFrame(index=df_index)
                for name in self._all_outer_deps_to_gather:
                    key_space = dep_key_spaces_by_name[name]
                    gathered_df[name] = [
                        values_by_task_key.get(
                            TaskKey(name, case_key.project(key_space)), None)
                        for case_key in df_row_case_keys
                    ]

                # Construct the final values to pass to the wrapped task.
                inner_dep_values = passthrough_values
                inner_dep_values.insert(gather_task_key_ix, gathered_df)

                return task.compute(query, inner_dep_values)

            # NOTE This Task object will not be picklable, because its
            # compute_func is a nested function.  If we ever want to send Tasks
            # over the network, we need to either use dill or refactor this
            # code.
            return Task(
                key=task.key,
                dep_keys=wrapped_dep_keys,
                compute_func=wrapped_compute_func,
            )

        orig_tasks = self.wrapped_resource.get_tasks(
            inner_key_spaces_by_name, inner_dtkls)
        return [wrap_task(task) for task in orig_tasks]


def multi_index_from_case_keys(case_keys, ordered_key_names):
    assert len(ordered_key_names) > 0
    return pd.MultiIndex.from_tuples(
        tuples=[
            tuple(case_key[name] for name in ordered_key_names)
            for case_key in case_keys
        ],
        names=ordered_key_names,
    )


RESOURCE_METHODS = [
    'get_code_id', 'get_dependency_names', 'get_tasks', 'get_source_func']


def is_resource(obj):
    return all(hasattr(obj, method_name) for method_name in RESOURCE_METHODS)


def as_resource(func_or_resource):
    if is_resource(func_or_resource):
        resource = func_or_resource
    elif callable(func_or_resource):
        resource = FunctionResource(func_or_resource)
    else:
        raise ValueError('func must be either callable or a Resource')

    return resource


def resource_wrapper(wrapper_fn, *args, **kwargs):
    def decorator(func_or_resource):
        resource = as_resource(func_or_resource)
        return wrapper_fn(resource, *args, **kwargs)
    return decorator
