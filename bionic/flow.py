'''
Contains the FlowBuilder and Flow classes, which implement the core workflow
construction and execution APIs (respectively).
'''

import pandas as pd

# A bit annoying that we have to rename this when we import it.
import protocol as proto
from cache import StorageCache, CACHEABLE_NAME_PROTOCOL
from entity import CaseKey
from exception import UndefinedResourceError
from resource import ValueResource, multi_index_from_case_keys, as_resource
from resolver import ResourceResolver
from handle import MutableHandle
from util import group_pairs, check_exactly_one_present

import logging
logger = logging.getLogger(__name__)

DEFAULT_PROTOCOL = proto.picklable()


class FlowState(object):
    '''
    Holds the internal state for a flow.  Flow and FlowBuilder instances access
    these via handles to minimize copying as we move between mutable and
    immutable interfaces.
    '''
    def __init__(self, state=None):
        if state is None:
            self.resources_by_name = {}
            self.last_added_case_key = None

        else:
            self.resources_by_name = {
                name: resource.copy_if_mutable()
                for name, resource in state.resources_by_name.iteritems()
            }
            self.last_added_case_key = state.last_added_case_key

    def copy(self):
        return FlowState(self)


class FlowBuilder(object):
    '''
    A mutable builder (as in "builder pattern") for Flows.  Use declare/assign/
    set/derive to add resources to the builder, then use build() to convert it
    into a Flow.
    '''

    # --- Public API.

    def __init__(self, _mutable_state_handle=None):
        if _mutable_state_handle is None:
            _mutable_state_handle = DEFAULT_IMMUTABLE_STATE_HANDLE.as_mutable()
        self._mutable_state_handle = _mutable_state_handle

    def build(self):
        flow = Flow._from_builder(self)
        flow._resolver.get_ready()
        return flow

    def declare(self, name, protocol=None):
        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        self._check_resource_does_not_exist(name)
        self._create_resource(name, protocol)

    def assign(self, name, value=None, values=None, protocol=None):
        check_exactly_one_present(value=value, values=values)

        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        if value is not None:
            protocol.validate(value)

            self._check_resource_does_not_exist(name)

            resource = self._create_resource(name, protocol)
            resource.add_case(CaseKey(), value)

        else:  # if values is not None
            for value in values:
                protocol.validate(value)
                CACHEABLE_NAME_PROTOCOL.validate(value)

            self._check_resource_does_not_exist(name)

            resource = self._create_resource(name, protocol)
            for value in values:
                resource.add_case(CaseKey({name: value}), value)

    def set(self, name, value=None, values=None):
        check_exactly_one_present(value=value, values=values)

        self._check_resource_exists(name)
        self._check_is_safe_to_clear([name])

        state = self._get_state()
        resource = state.resources_by_name[name]
        protocol = resource.attrs.protocol

        if value is not None:
            protocol.validate(value)

            resource = self._clear_resource(resource)
            resource.add_case(CaseKey(), value)

        else:  # values is not None
            for value in values:
                protocol.validate(value)
                CACHEABLE_NAME_PROTOCOL.validate(value)

            resource = self._clear_resource(resource)
            for value in values:
                resource.add_case(CaseKey({name: value}), value)

    # TODO Consider allowing adding cases to formerly-singleton values?  Not
    # sure if we should allow this or not.
    # TODO Should we allow undeclared names?  Having to declare them first is
    # basically always clunky and annoying, but should we allow add_case to
    # create new resources?  (On the other hand, it sort of makes sense in cases
    # where we're using add_case('label', text).then_set('var', value) in order
    # to add a clean label to an unhashable value.  In that case it'd be nice
    # that we can create new resources even on an Flow.)
    def add_case(self, *name_values):
        name_value_pairs = group_pairs(name_values)

        for _, value in name_value_pairs:
            CACHEABLE_NAME_PROTOCOL.validate(value)

        case_key = CaseKey(dict(name_value_pairs))

        state = self._get_state()
        for name, value in name_value_pairs:
            self._check_resource_exists(name)
            self._check_can_add_case(name, case_key, value)

        for name, value in name_value_pairs:
            resource = state.resources_by_name[name]
            resource.add_case(case_key, value)

        case = FlowCase(self, case_key)
        state.last_added_case_key = case.key
        return case

    def clear_cases(self, *names):
        state = self._get_state()

        for name in names:
            if name not in state.resources_by_name:
                raise ValueError(
                    "Can't clear cases for nonexistent resource %r" % name)
        self._check_is_safe_to_clear(names)

        for name in names:
            resource = state.resources_by_name[name]
            self._clear_resource(resource)

    def delete(self, *names):
        self._check_is_safe_to_clear(names)

        state = self._get_state()
        for name in names:
            del state.resources_by_name[name]

    def derive(self, func_or_resource):
        resource = as_resource(func_or_resource)
        if resource.attrs.protocol is None:
            resource = proto.picklable()(resource)

        self._check_resource_does_not_exist(resource.attrs.name)

        state = self._get_state()
        state.resources_by_name[resource.attrs.name] = resource

        return resource.get_source_func()

    def __call__(self, func_or_resource):
        '''
        Convenience wrapper for derive().
        '''
        return self.derive(func_or_resource)

    # --- Private helpers.

    def _get_state(self):
        return self._mutable_state_handle.get()

    def _check_resource_exists(self, name):
        state = self._get_state()
        if name not in state.resources_by_name:
            raise ValueError("Resource %r doesn't exist" % name)

    def _check_resource_does_not_exist(self, name):
        state = self._get_state()
        if name in state.resources_by_name:
            raise ValueError("Resource %r already exists" % name)

    def _check_can_add_case(self, name, case_key, value):
        state = self._get_state()

        resource = state.resources_by_name[name]
        if not isinstance(resource, ValueResource):
            raise ValueError("Can't add case to function resource %r" % name)
        resource.check_can_add_case(case_key, value)

    def _check_is_safe_to_clear(self, names):
        state = self._get_state()

        for name in names:
            resource = state.resources_by_name.get(name)
            if resource is None:
                continue

            if not isinstance(resource, ValueResource):
                continue

            for related_name in resource.key_space:
                if related_name not in names:
                    raise ValueError(
                        "Can't remove cases for resource %r without also "
                        "removing resources %r" % (
                            name, list(resource.key_space)))

        # TODO Consider checking downstream resources too.

    def _clear_resource(self, resource):
        if isinstance(resource, ValueResource):
            resource.clear_cases()
            return resource
        else:
            return self._create_resource(
                resource.attrs.name, resource.attrs.protocol)

    def _create_resource(self, name, protocol):
        state = self._get_state()
        resource = ValueResource(name, protocol)
        state.resources_by_name[name] = resource
        return resource

    def _set_for_case_key(self, case_key, name, value):
        self._check_resource_exists(name)
        self._check_can_add_case(name, case_key, value)

        state = self._get_state()
        resource = state.resources_by_name[name]
        resource.add_case(case_key, value)

    def _set_for_last_case(self, name, value):
        state = self._get_state()

        last_case_key = state.last_added_case_key
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
            for name in self._get_state().resources_by_name.keys()
            if include_core or not name.startswith('core__')
        ]

    def resource_protocol(self, name):
        resource = self._get_state().resources_by_name.get(name)
        if resource is None:
            raise UndefinedResourceError("Resource %r is not defined" % name)
        return resource.attrs.protocol

    def to_builder(self):
        return FlowBuilder(
            _mutable_state_handle=self._immutable_state_handle.as_mutable())

    def get(self, name, fmt=None):
        result_group = self._resolver.compute_result_group_for_resource_name(
            name)

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
        elif fmt is dict or fmt == 'dict':
            return {
                result.query.case_key: result.value
                for result in result_group
            }
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

    def setting(self, name, value=None, values=None):
        return self._updating(lambda builder: builder.set(name, value, values))

    def adding_case(self, *name_values):
        return self._updating(lambda builder: builder.add_case(*name_values))

    def then_setting(self, name, value):
        return self._updating(
            lambda builder: builder._set_for_last_case(name, value))

    def clearing_cases(self, *names):
        return self._updating(lambda builder: builder.clear_cases(*names))

    # --- Private helpers.

    @classmethod
    def _from_builder(self, builder):
        return Flow(
            _official=True,
            immutable_state_handle=(
                builder._mutable_state_handle.as_immutable()),
        )

    def __init__(self, immutable_state_handle, _official=False):
        if not _official:
            raise ValueError(
                "Don't construct this class directly; "
                "use one of the classmethod constructors")

        self._immutable_state_handle = immutable_state_handle
        self._resolver = ResourceResolver(immutable_state_handle)

    def _get_state(self):
        return self._immutable_state_handle.get()

    def _updating(self, builder_update_func):
        def update_state(state):
            # Create a new builder that will update this value.
            builder = FlowBuilder(
                _mutable_state_handle=MutableHandle(mutable_value=state))
            builder_update_func(builder)
            return builder._get_state()

        updated_handle = self._immutable_state_handle.updating(
            update_state, eager=True)
        return Flow(_official=True, immutable_state_handle=updated_handle)


# Construct a default state object.

default_builder = FlowBuilder(MutableHandle(mutable_value=FlowState()))
default_builder.assign('core__storage_cache__dir_name', 'bndata')


@default_builder.derive
def core__storage_cache(core__storage_cache__dir_name):
    return StorageCache(core__storage_cache__dir_name)


DEFAULT_IMMUTABLE_STATE_HANDLE = \
    default_builder._mutable_state_handle.as_immutable()
