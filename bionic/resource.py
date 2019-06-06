'''
Contains Bionic's concept of a "resource": an element of a workflow that can
have one or more values, possible depending on other resources.  This module
includes a BaseResource class and various subclasses.

The whole architecture is a bit of a mess and probably needs a substantial
rethink.
'''
from __future__ import absolute_import

from builtins import zip, object

import six
import inspect
from copy import copy
from collections import defaultdict
import functools
from io import BytesIO

import pandas as pd
from PIL import Image

from .entity import Task, TaskKey, CaseKey, CaseKeySpace
from .util import groups_dict, init_matplotlib

import logging
logger = logging.getLogger(__name__)


class ResourceAttributes(object):
    def __init__(
            self, name, protocol=None, code_version=None, should_persist=None):
        self.name = name
        self.protocol = protocol
        self.code_version = code_version
        self.should_persist = None


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
            attrs=ResourceAttributes(
                name=name, protocol=protocol, should_persist=False),
            is_mutable=True,
        )

        self.clear_cases()

    def copy(self):
        resource = ValueResource(self.attrs.name, self.attrs.protocol)
        resource.key_space = self.key_space
        resource._has_any_values = self._has_any_values
        resource._values_by_case_key = self._values_by_case_key.copy()
        resource._code_ids_by_key = self._code_ids_by_key.copy()
        return resource

    def clear_cases(self):
        self.key_space = CaseKeySpace()
        self._has_any_values = False
        self._values_by_case_key = {}
        self._code_ids_by_key = {}

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
        code_id = self.attrs.protocol.tokenize(value)

        if not self._has_any_values:
            self.key_space = case_key.space
            self._has_any_values = True

        self._values_by_case_key[case_key] = value
        self._code_ids_by_key[case_key] = code_id

    def get_code_id(self, case_key):
        return self._code_ids_by_key[case_key]

    def get_source_func(self):
        return None

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
            for case_key in self._values_by_case_key.keys()
        ]

    def _compute(self, query, dep_values, case_key):
        return self._values_by_case_key[case_key]


class FunctionResource(BaseResource):
    def __init__(self, func):
        super(FunctionResource, self).__init__(attrs=ResourceAttributes(
            name=func.__name__))

        self._func = func

        if six.PY2:
            argspec = inspect.getargspec(func)
            args = argspec.args
            varargs = argspec.varargs
            varkw = argspec.keywords
        else:
            argspec = inspect.getfullargspec(func)
            args = argspec.args
            varargs = argspec.varargs
            varkw = argspec.varkw

        if varargs:
            raise ValueError('Functions with varargs are not supported')
        if varkw:
            raise ValueError('Functions with keyword args are not supported')
        self._dep_names = list(args)

    def get_dependency_names(self):
        return self._dep_names

    def get_source_func(self):
        return self._func

    def get_key_space(self, dep_key_spaces_by_name):
        return CaseKeySpace.union_all(list(dep_key_spaces_by_name.values()))

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        dep_case_key_lists = [
            [
                task_key.case_key
                for task_key in dep_task_key_lists_by_name[name]
            ]
            for name in self._dep_names
        ]
        out_case_keys = merge_case_key_lists(dep_case_key_lists)

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

    def _apply(self, query, dep_values):
        value = self._func(*dep_values)
        query.protocol.validate(value)
        return value

    def __repr__(self):
        return '%s(%s)' % (self.__class__.__name__, self._func)


class GatherResource(WrappingResource):
    def __init__(
            self, wrapped_resource,
            primary_names, secondary_names, gathered_dep_name):
        # TODO This is still pretty confusing, I think.
        '''
        This a very involved wrapper implementing the "gather" decorator.  It
        collects multiple dependencies into a single DataFrame argument, which
        is accessible to the wrapped resource along with its other arguments.
        Multiple instances of these dependencies may be grouped in a single
        DataFrame rather than being passed to separate instances of this
        resource.

        The gathered dependencies are provided in two groups: "primary" and
        "secondary"; putting dependencies in one group or the other will change
        how the grouping happens.  If a dependency is "primary", then any
        variation caused by it (i.e., variation from its ancestors) will be
        collapsed into a single frame.  Secondary dependencies do not affect
        the grouping; any variation due to them will result in multiple
        instances of this resource, as normal.
        '''

        super(GatherResource, self).__init__(wrapped_resource)

        self._primary_names = primary_names
        self._secondary_names = secondary_names
        self._inner_gathered_dep_name = gathered_dep_name

        self._inner_dep_names = self.wrapped_resource.get_dependency_names()

        self._gather_names = (
            list(self._primary_names) + list(self._secondary_names))

        inner_gathered_dep_ix = self._inner_dep_names.index(
            self._inner_gathered_dep_name)
        if inner_gathered_dep_ix < 0:
            raise ValueError(
                "Expected wrapped %r to have dependency name %r, but "
                "only found names %r" % (
                    self.wrapped_resource, self._inner_gathered_dep_name,
                    self._inner_dep_names))

        self._passthrough_dep_names = list(self._inner_dep_names)
        self._passthrough_dep_names.pop(inner_gathered_dep_ix)
        assert self._inner_gathered_dep_name not in self._passthrough_dep_names

        extra_dep_names = [
            name for name in self._gather_names
            if name not in self._passthrough_dep_names
        ]
        self._outer_dep_names = extra_dep_names + self._passthrough_dep_names

    def get_dependency_names(self):
        return self._outer_dep_names

    def get_key_space(self, dep_key_spaces_by_name):
        return self._compute_key_spaces(dep_key_spaces_by_name).outer

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        # These are the key spaces and task keys that the outside world sees.
        outer_key_spaces_by_name = dep_key_spaces_by_name
        outer_dtkls = dep_task_key_lists_by_name

        # We'll need to derive some useful key spaces.
        key_spaces = self._compute_key_spaces(dep_key_spaces_by_name)

        # Identify the full case keys that will be partitioned into
        # gathered frames.
        gather_case_key_lists = [
            [task_key.case_key for task_key in outer_dtkls[dep_name]]
            for dep_name in self._gather_names
        ]
        gather_case_keys = merge_case_key_lists(gather_case_key_lists)

        gather_case_key_lists_by_delta_case_key = defaultdict(list)
        for gather_case_key in gather_case_keys:
            delta_case_key = gather_case_key.project(key_spaces.delta)
            gather_case_key_lists_by_delta_case_key[delta_case_key].append(
                gather_case_key)

        delta_case_keys = gather_case_key_lists_by_delta_case_key.keys()

        # Create new task keys for the gathered values that the inner resource
        # will consume.
        inner_gathered_key_space = key_spaces.delta
        inner_gathered_dep_task_keys = [
            TaskKey(
                resource_name=self._inner_gathered_dep_name,
                case_key=case_key,
            )
            for case_key in delta_case_keys
        ]

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
            delta_case_key = inner_gather_case_key

            # Find the task keys that need to be gathered together.
            unique_gather_task_keys = set()
            for dep_name in self._gather_names:
                dep_key_space = dep_key_spaces_by_name[dep_name]
                for gather_case_key in gather_case_keys:
                    unique_gather_task_keys.add(TaskKey(
                        resource_name=dep_name,
                        case_key=gather_case_key.project(dep_key_space),
                    ))
            prepended_keys = list(unique_gather_task_keys)

            # Combine the gathering task keys with the keys expected by the
            # wrapped task (except the one key we removed, since we'll be
            # synthesizing it ourselves).
            wrapped_dep_keys = prepended_keys + passthrough_dep_keys

            def wrapped_compute_func(query, dep_values):
                # Split off the extra values for the keys we prepended.
                prepended_values = dep_values[:len(prepended_keys)]
                passthrough_values = dep_values[len(prepended_keys):]

                values_by_task_key = dict(list(zip(
                    prepended_keys, prepended_values)))

                # Gather the prepended values into a single frame.
                row_case_keys =\
                    gather_case_key_lists_by_delta_case_key[delta_case_key]

                gathered_df = pd.DataFrame()
                for name in self._gather_names:
                    key_space = dep_key_spaces_by_name[name]
                    gathered_df[name] = [
                        values_by_task_key.get(
                            TaskKey(name, case_key.project(key_space)), None)
                        for case_key in row_case_keys
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

    def _compute_key_spaces(self, dep_key_spaces_by_name):
        return self._KeySpaces(self, dep_key_spaces_by_name)

    class _KeySpaces(object):
        def __init__(self, gather_resource, dep_key_spaces_by_name):
            # The combined keyspace of all the non-gathered dependencies of the
            # wrapped resource.
            self.passthrough = CaseKeySpace.union_all(
                dep_key_spaces_by_name[name]
                for name in gather_resource._passthrough_dep_names
            )

            # The combined keyspace of the all the primary gathered
            # dependencies.  This corresponds to the index of the gathered
            # frame.
            self.primary = CaseKeySpace.union_all(
                dep_key_spaces_by_name[name]
                for name in gather_resource._primary_names
            )

            # The combined keyspace of the all the secondary gathered
            # dependencies.
            self.secondary = CaseKeySpace.union_all(
                dep_key_spaces_by_name[name]
                for name in gather_resource._secondary_names
            )

            # The difference between the secondary and primary key spaces.
            # This is the key space of the gathered frame resource that the
            # wrapped resource sees.
            self.delta = self.secondary.difference(self.primary)

            # The combination of the passthrough and delta key spaces -- i.e.,
            # the combined key space of all the dependencies seen by the
            # wrapped resource.  This is also the key space of this resource.
            self.outer = self.passthrough.union(self.delta)


# TODO Matplotlib has global state, which means it may run differently and
# produce different output (or errors) in Jupyter vs in a script.  I think this
# may produce some annoying gotchas in the future.  Some possible solutions:
# 1. Have all tasks run in a separate process, at least by default.
# 2. Have special handling for certain tasks that need to run in a separate
#    process.
# 3. Try to make Bionic's matplotlib initialization identical to Jupyter's.
class PyplotResource(WrappingResource):
    def __init__(self, wrapped_resource, name='pyplot'):
        super(PyplotResource, self).__init__(wrapped_resource)

        self._pyplot_name = name

        inner_dep_names = wrapped_resource.get_dependency_names()
        self._pyplot_dep_ix = inner_dep_names.index(self._pyplot_name)
        if self._pyplot_dep_ix == -1:
            raise ValueError(
                "When using %s, expected wrapped %s to have a dependency "
                "named %r; only found %r" % (
                    self.__class__.__name__, wrapped_resource, inner_dep_names)
            )

        self._outer_dep_names = list(inner_dep_names)
        self._outer_dep_names.remove(self._pyplot_name)

    def get_dependency_names(self):
        return self._outer_dep_names

    def get_tasks(self, dep_key_spaces_by_name, dep_task_key_lists_by_name):
        outer_dkss = dep_key_spaces_by_name

        inner_dkss = outer_dkss.copy()
        inner_dkss[self._pyplot_name] = CaseKey([])

        outer_dtkls = dep_task_key_lists_by_name

        inner_dtkls = outer_dtkls.copy()
        inner_dtkls[self._pyplot_name] = [
            TaskKey(
                resource_name=self._pyplot_name,
                case_key=CaseKey([]),
            )
        ]

        inner_tasks = self.wrapped_resource.get_tasks(inner_dkss, inner_dtkls)

        def wrap_task(task):
            def wrapped_compute_func(query, dep_values):
                # Make sure matplotlib is set up.
                init_matplotlib()
                from matplotlib import pyplot as plt

                outer_dep_values = dep_values

                inner_dep_values = list(outer_dep_values)
                inner_dep_values.insert(self._pyplot_dep_ix, plt)

                # Create a new figure so our task has a blank canvas to work
                # with.
                plt.figure()

                # Run the task, which will do the plotting.
                value = task.compute(query, inner_dep_values)
                if value is not None:
                    raise ValueError(
                        "Resources wrapped by %s should not return values; "
                        "got value %r" % (self.__class__.__name__, value))

                # Save the plot into a buffer.
                bio = BytesIO()
                plt.savefig(bio, format='png')
                plt.close()
                # Reset the buffer's position so that when we read from it, we
                # read from the beginning.
                bio.seek(0)
                # Load the buffer into an Image object.
                image = Image.open(bio)

                return image

            return Task(
                key=task.key,
                dep_keys=[
                    dep_key
                    for dep_key in task.dep_keys
                    if dep_key.resource_name != self._pyplot_name
                ],
                compute_func=wrapped_compute_func,
            )

        outer_tasks = [wrap_task(task) for task in inner_tasks]
        return outer_tasks


def merge_case_key_lists(case_key_lists):
    merged_case_keys = [CaseKey([])]
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
                merged_key_lists_by_common_key.items():
            for cur_key in cur_key_lists_by_common_key.get(common_key, []):
                for merged_key in merged_keys:
                    new_merged_key = merged_key.merge(cur_key)
                    merged_case_keys.append(new_merged_key)

        merged_key_space = merged_key_space.union(cur_key_space)

    return merged_case_keys


def multi_index_from_case_keys(case_keys, ordered_key_names):
    assert len(ordered_key_names) > 0
    return pd.MultiIndex.from_tuples(
        tuples=[
            tuple(case_key.values[name] for name in ordered_key_names)
            for case_key in case_keys
        ],
        names=ordered_key_names,
    )


# -- Helpers for working with resources.

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
