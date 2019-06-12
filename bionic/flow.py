'''
Contains the FlowBuilder and Flow classes, which implement the core workflow
construction and execution APIs (respectively).
'''
from __future__ import absolute_import

from builtins import object
import os
import shutil
import functools

import pyrsistent as pyrs
import pandas as pd
from pathlib2 import Path
from six.moves import reload_module


# A bit annoying that we have to rename this when we import it.
from . import protocols as protos
from .cache import PersistentCache
from .entity import CaseKey
from .exception import UndefinedResourceError
from .resource import ValueResource, multi_index_from_case_keys, as_resource
from .resolver import ResourceResolver
from . import decorators
from .util import group_pairs, check_exactly_one_present
from . import dagviz

import logging
logger = logging.getLogger(__name__)

DEFAULT_PROTOCOL = protos.CombinedProtocol(
    protos.DataFrameProtocol(),
    protos.ImageProtocol(),
    protos.PicklableProtocol(),
)


# We use an immutable persistent data structure to represent our state.  This
# has several advantages:
# 1. We can provide both a mutable (FlowBuilder) and immutable (Flow) interface
# to the state without worrying about the mutable interface breaking the
# immutable one.
# 2. It's easy to provide exception safety: when FlowBuilder is performing an
# update, it only adopts the new state at the very end.  If something fails
# and throws an exception partway through, the state remains unchanged.
# 3. We can maintain a single "blessed" state object that's eligible for
# reloading.
class FlowState(pyrs.PClass):
    '''
    Contains the state for a Flow or FlowBuilder object.  This is a
    "pyrsistent" class, which means it's immutable, but a modified copy can be
    efficiently created with the set() method.
    '''
    resources_by_name = pyrs.field(initial=pyrs.pmap())
    last_added_case_key = pyrs.field(initial=None)

    # These are used to keep track of whether a flow state is safe to reload.
    # To keep things sane, we try to ensure that there is at most one flow
    # with a given name that's eligible for reload.  This is the first flow
    # created by a builder, and it's marked as "blessed".  Any modified
    # versions of the flow are ineligible for blessing, as are any other flows
    # created by the same builder.
    # This reloading behavior is pretty finicky and magical, but thankfully
    # it shouldn't have much effect on code that doesn't use it.
    can_be_blessed = pyrs.field(initial=True)
    is_blessed = pyrs.field(initial=False)

    def bless(self):
        assert self.can_be_blessed and not self.is_blessed
        return self.set(is_blessed=True, can_be_blessed=False)

    def touch(self):
        return self.set(is_blessed=False)

    def get_resource(self, name):
        if name not in self.resources_by_name:
            raise UndefinedResourceError("Resource %r is not defined" % name)
        return self.resources_by_name[name]

    def has_resource(self, name):
        return name in self.resources_by_name

    def create_resource(self, name, protocol):
        if name in self.resources_by_name:
            raise ValueError("Resource %r already exists" % name)

        resource = ValueResource(name, protocol)
        return self._set_resource(resource).touch()

    def install_resource(self, resource, create_if_not_set=False):
        for name in resource.attrs.names:
            if name in self.resources_by_name:
                raise ValueError("Resource %r already exists" % name)

        return self._set_resource(resource).touch()

    def add_case(self, name, case_key, value):
        resource = self.get_resource(name).copy_if_mutable()
        if not isinstance(resource, ValueResource):
            raise ValueError("Can't add case to function resource %r" % name)
        resource.check_can_add_case(case_key, value)
        resource.add_case(case_key, value)

        return self._set_resource(resource).touch()

    def clear_resources(self, names):
        state = self

        # Remember the original protocol for each resource.
        protocols_by_resource_name = {}
        for name in names:
            if not state.has_resource(name):
                continue
            resource = self.get_resource(name)
            for res_name, res_protocol in zip(
                    resource.attrs.names, resource.attrs.protocols):
                protocols_by_resource_name[res_name] = res_protocol

        # Delete the resources (or fail if not possible).
        state = state.delete_resources(names)

        # Recreate an empty version of each resource.
        for res_name, res_protocol in protocols_by_resource_name.items():
            state = state.create_resource(res_name, res_protocol)

        return state.touch()

        # TODO Consider checking downstream resources too.

    def delete_resources(self, names):
        state = self

        for name in names:
            # Make sure the name is safe to delete.
            if not state.has_resource(name):
                continue
            resource = state.get_resource(name)

            if isinstance(resource, ValueResource):
                for related_name in resource.key_space:
                    if related_name not in names:
                        raise ValueError(
                            "Can't remove cases for resource %r without also "
                            "removing resources %r" % (
                                name, list(resource.key_space)))

            resource_names = resource.attrs.names
            for related_name in resource_names:
                if related_name not in names:
                    raise ValueError(
                        "Can't remove cases for resource %r without also "
                        "removing resources %r" % (
                            name, list(resource_names)))

            # Delete it.
            state = state.set(
                resources_by_name=state.resources_by_name.remove(name))

        return state.touch()

    def _set_resource(self, resource):
        state = self
        for name in resource.attrs.names:
            state = state.set(
                resources_by_name=state.resources_by_name.set(name, resource))
        return state


class FlowBuilder(object):
    '''
    A mutable builder (as in "builder pattern") for Flows.  Use declare/assign/
    set/derive to add resources to the builder, then use build() to convert it
    into a Flow.
    '''

    # --- Public API.

    def __init__(self, name, _state=None):
        if _state is None:
            if name is None:
                raise ValueError("A name must be provided")

            self._state = create_default_flow_state()
            self._set_name(name)

        else:
            assert name is None
            self._state = _state

    def build(self):
        state = self._state
        if state.can_be_blessed:
            state = state.bless()

        flow = Flow._from_state(state)
        flow._resolver.get_ready()

        self._state = state.touch()
        return flow

    def declare(self, name, protocol=None):
        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        self._state = self._state.create_resource(name, protocol)

    def assign(self, name, value=None, values=None, protocol=None):
        check_exactly_one_present(value=value, values=values)
        if value is not None:
            values = [value]

        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        for value in values:
            protocol.validate(value)

        state = self._state

        state = state.create_resource(name, protocol)
        for value in values:
            case_key = CaseKey([(name, value, protocol.tokenize(value))])
            state = state.add_case(name, case_key, value)

        self._state = state

    def set(self, name, value=None, values=None):
        check_exactly_one_present(value=value, values=values)
        if value is not None:
            values = [value]

        state = self._state

        state = state.clear_resources([name])
        resource = state.get_resource(name)
        # This resource must have a single name and single protocol; otherwise
        # we wouldn't have been able to clear it.
        protocol, = resource.attrs.protocols

        protocol.validate(value)

        for value in values:
            case_key = CaseKey([(name, value, protocol.tokenize(value))])
            state = state.add_case(name, case_key, value)

        self._state = state

    # TODO Should we allow undeclared names?  Having to declare them first is
    # basically always clunky and annoying, but should we allow add_case to
    # create new resources?  (On the other hand, it sort of makes sense in cases
    # where we're using add_case('label', text).then_set('var', value) in order
    # to add a clean label to an unhashable value.  In that case it'd be nice
    # that we can create new resources even on a Flow.)
    def add_case(self, *name_values):
        name_value_pairs = group_pairs(name_values)

        state = self._state

        case_nvt_tuples = []
        for name, value in name_value_pairs:
            resource = state.get_resource(name)
            if len(resource.attrs.protocols) > 1:
                raise ValueError(
                    "Can't add case for resource with multiple names %r" % (
                        (tuple(resource.attr.names),)))
            protocol, = resource.attrs.protocols
            protocol.validate(value)
            token = protocol.tokenize(value)

            case_nvt_tuples.append((name, value, token))

        case_key = CaseKey(case_nvt_tuples)

        for name, value, _ in case_nvt_tuples:
            state = state.add_case(name, case_key, value)

        case = FlowCase(self, case_key)
        state = state.set(last_added_case_key=case.key)

        self._state = state

        return case

    def clear_cases(self, *names):
        self._state = self._state.clear_resources(names)

    def delete(self, *names):
        self._state = self._state.delete_resources(names)

    def derive(self, func_or_resource):
        resource = as_resource(func_or_resource)
        if resource.attrs.protocols is None:
            resource = DEFAULT_PROTOCOL(resource)
        if resource.attrs.should_persist is None:
            resource = decorators.persist(True)(resource)

        state = self._state

        state = state.delete_resources(resource.attrs.names)
        state = state.install_resource(resource)

        self._state = state

        return resource.get_source_func()

    def __call__(self, func_or_resource):
        '''
        Convenience wrapper for derive().
        '''
        return self.derive(func_or_resource)

    # --- Private helpers.

    @classmethod
    def _from_state(cls, state):
        return cls(name=None, _state=state)

    @classmethod
    def _with_empty_state(cls):
        return cls(name=None, _state=FlowState())

    def _set_name(self, name):
        self.set('core__flow_name', name)

    def _set_for_case_key(self, case_key, name, value):
        self._state = self._state.add_case(name, case_key, value)

    def _set_for_last_case(self, name, value):
        last_case_key = self._state.last_added_case_key
        if last_case_key is None:
            raise ValueError(
                "A case must have been added before calling this method")

        self._set_for_case_key(last_case_key, name, value)


class FlowCase(object):
    def __init__(self, builder, key):
        self.key = key
        self._builder = builder

    def then_set(self, name, value):
        self._builder._set_for_case_key(self.key, name, value)
        return self


class Flow(object):
    '''
    An immutable workflow object.  You can use get() to compute any resource
    in the workflow, or setting() to create a new workflow with modifications.
    Not all modifications are possible with this interface, but to_builder()
    can be used to get a mutable FlowBuilder version of a Flow.
    '''
    # --- Public API.

    def all_resource_names(self, include_core=False):
        return [
            name
            for name in self._state.resources_by_name.keys()
            if include_core or not self._resolver.resource_is_core(name)
        ]

    def resource_protocol(self, name):
        return self._state.get_resource(name).protocol_for_name(name)

    def to_builder(self):
        return FlowBuilder._from_state(self._state)

    def get(self, name, fmt=None):
        result_group = self._resolver.resolve(name)
        if fmt is None or fmt is object:
            if len(result_group) == 0:
                raise ValueError("Resource %s has no defined values" % name)
            if len(result_group) > 1:
                raise ValueError("Resource %s has multiple values" % name)
            result, = result_group
            return result.value
        elif fmt is list or fmt == 'list':
            return [result.value for result in result_group]
        elif fmt is set or fmt == 'set':
            return set(result.value for result in result_group)
        elif fmt is pd.Series or fmt == 'series':
            if len(result_group.key_space) > 0:
                index = multi_index_from_case_keys(
                    case_keys=[
                        result.query.case_key for result in result_group],
                    ordered_key_names=list(result_group.key_space),
                )
            else:
                index = None
            return pd.Series(
                name=name,
                data=[result.value for result in result_group],
                index=index,
            )
        else:
            raise ValueError("Unrecognized format %r" % fmt)

    def export(self, name, file_path=None, dir_path=None):
        '''
        Provides access to the persisted file corresponding to a resource.  Can
        be called in three ways:

            export(name): returns a path the cached file
            export(name, file_path=path): copies the cached file to the
                specified path
            export(name, dir_path=path): copies the cached file to the
                specified directory

        The resource must be persisted and have only one instance.
        '''
        result_group = self._resolver.resolve(name)
        if len(result_group) != 1:
            raise ValueError(
                "Can only export a resource if it has a single value; "
                "resource %r has %d values" % (name, len(result_group)))

        result, = result_group
        if result.cache_path is None:
            raise ValueError("Resource %r is not persisted" % name)

        src_file_path = result.cache_path

        if dir_path is None and file_path is None:
            return src_file_path

        check_exactly_one_present(dir_path=dir_path, file_path=file_path)

        if dir_path is not None:
            dst_dir_path = Path(dir_path)
            filename = name + src_file_path.suffix
            dst_file_path = dst_dir_path / filename
        else:
            dst_file_path = Path(file_path)
            dst_dir_path = dst_file_path.parent

        if not dst_dir_path.exists():
            dst_dir_path.mkdir(parents=True)

        shutil.copyfile(str(src_file_path), str(dst_file_path))

    def setting(self, name, value=None, values=None):
        return self._updating(lambda builder: builder.set(name, value, values))

    def adding_case(self, *name_values):
        return self._updating(lambda builder: builder.add_case(*name_values))

    def then_setting(self, name, value):
        return self._updating(
            lambda builder: builder._set_for_last_case(name, value))

    def clearing_cases(self, *names):
        return self._updating(lambda builder: builder.clear_cases(*names))

    @property
    def name(self):
        return self.get('core__flow_name')

    def render_dag(self, include_core=False, vertical=False, curvy_lines=False):
        graph = self._resolver.export_dag(include_core)
        dot = dagviz.dot_from_graph(graph, vertical, curvy_lines)
        image = dagviz.image_from_dot(dot)
        return image

    # TODO Should we offer an in-place version of this?  It's contrary to the
    # idea of an immutable API, but it might be more natural for the user, and
    # reloading is already updating global state....
    def reloading(self):
        '''
        Attempts to reload all modules used directly by this Flow.  For safety,
        this only works if this flow meets the following requirements:
        - is the first Flow built by its FlowBuilder
        - has never been modified (i.e., isn't derived from another Flow)
        - is assigned to a top-level variable in a module that one of its
          functions is defined in

        The most straightforward way to meet these requirements is to define
        your flow in a module as:

            builder = ...

            @builder
            def ...

            ...

            flow = builder.build()

        and then import in the notebook like so:

            from mymodule import flow
            ...
            flow.reloading().get('myresource')

        This will reload the modules and use the most recent version of the
        flow before doing the get().
        '''
        # TODO If we wanted, I think we could support reloading on modified
        # versions of flows by keeping a copy of the original blessed flow,
        # plus all the operations performed to get to the current version.
        # Then if we want to reload, we reload the blessed flow and re-apply
        # those operations.

        from sys import modules as module_registry

        state = self._state

        if not state.is_blessed:
            raise ValueError(
                "A flow can only be reloaded if it's the first flow built "
                "from its builder and it hasn't been modified")

        self_name = self.name

        module_names = set()
        for resource in state.resources_by_name.values():
            source_func = resource.get_source_func()
            if source_func is None:
                continue
            module_names.add(source_func.__module__)

        blessed_candidate_flows = []
        unblessed_candidate_flows = []
        for module_name in module_names:
            module = reload_module(module_registry[module_name])
            for key in dir(module):
                element = getattr(module, key)
                if not isinstance(element, Flow):
                    continue
                flow = element
                if flow.name != self_name:
                    continue
                if not flow._state.is_blessed:
                    unblessed_candidate_flows.append(flow)
                else:
                    blessed_candidate_flows.append(flow)

        if len(blessed_candidate_flows) == 0:
            if len(unblessed_candidate_flows) > 0:
                raise Exception(
                    "Found a matching flow, but it had been modified" %
                    self_name)
            else:
                raise Exception(
                    "Couldn't find any flow named %r in modules %r" % (
                        self_name, module_names))
        if len(blessed_candidate_flows) > 1:
            raise Exception(
                "Too many flows named %r in modules %r; found %d, wanted 1" % (
                    self_name, module_names, len(blessed_candidate_flows)))
        flow, = blessed_candidate_flows

        return flow

    # --- Private helpers.

    @classmethod
    def _from_state(self, state):
        return Flow(_official=True, state=state)

    def __init__(self, state, _official=False):
        if not _official:
            raise ValueError(
                "Don't construct this class directly; "
                "use one of the classmethod constructors")

        self._state = state
        self._resolver = ResourceResolver(state)

        self.get = ShortcutProxy(self.get)
        self.setting = ShortcutProxy(self.setting)

    def _updating(self, builder_update_func):
        builder = FlowBuilder._from_state(self._state)
        builder_update_func(builder)
        return Flow._from_state(builder._state)


class ShortcutProxy(object):
    '''
    Wraps a method on a Flow object to allow it to be called via an alternative
    style.

    Original style:

        flow.get('resource')
        flow.setting('resource', 7)

    Alternative style:

        flow.get.resource()
        flow.setting.resource(7)

    The advantage of the alternative style is that it can be autocompleted in
    IPython, Jupyter, etc.
    '''

    def __init__(self, wrapped_method):
        self._wrapped_method = wrapped_method
        self._flow = wrapped_method.__self__
        assert isinstance(self._flow, Flow)

        self.__doc__ = self._wrapped_method.__doc__

    def __call__(self, *args, **kwargs):
        return self._wrapped_method(*args, **kwargs)

    def __dir__(self):
        return self._flow.all_resource_names()

    def __getattr__(self, name):
        return functools.partial(self._wrapped_method, name)


# Construct a default state object.
def create_default_flow_state():
    builder = FlowBuilder._with_empty_state()
    builder.declare('core__flow_name')
    builder.assign('core__persistent_cache__global_dir', 'bndata')

    @builder.derive
    @decorators.immediate
    def core__persistent_cache__flow_dir(
            core__persistent_cache__global_dir, core__flow_name):
        return os.path.join(
            core__persistent_cache__global_dir, core__flow_name)

    @builder.derive
    @decorators.immediate
    def core__persistent_cache(core__persistent_cache__flow_dir):
        return PersistentCache(core__persistent_cache__flow_dir)

    return builder._state
