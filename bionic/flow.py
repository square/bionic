'''
Contains the FlowBuilder and Flow classes, which implement the core workflow
construction and execution APIs (respectively).
'''
from __future__ import absolute_import

from builtins import object
import os
import posixpath
import shutil
import functools
import warnings

import pyrsistent as pyrs
import pandas as pd
from pathlib2 import Path, PosixPath
from six.moves import reload_module

# A bit annoying that we have to rename this when we import it.
from . import protocols as protos
from .cache import (
        LocalFileCache, GcsFileCache, PersistentCache, CACHE_SOURCE_NAME_LOCAL)
from .datatypes import CaseKey
from .exception import UndefinedEntityError, AlreadyDefinedEntityError
from .provider import ValueProvider, multi_index_from_case_keys, as_provider
from .deriver import EntityDeriver
from . import decorators
from .util import group_pairs, check_exactly_one_present, copy_to_gcs
from . import dagviz

import logging
logger = logging.getLogger(__name__)

DEFAULT_PROTOCOL = protos.CombinedProtocol(
    protos.ParquetDataFrameProtocol(),
    protos.ImageProtocol(),
    protos.NumPyProtocol(),
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
    """
    Contains the state for a Flow or FlowBuilder object.  This is a
    "pyrsistent" class, which means it's immutable, but a modified copy can be
    efficiently created with the set() method.
    """

    providers_by_name = pyrs.field(initial=pyrs.pmap())
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

    def get_provider(self, name):
        if name not in self.providers_by_name:
            raise UndefinedEntityError.for_name(name)
        return self.providers_by_name[name]

    def has_provider(self, name):
        return name in self.providers_by_name

    def create_provider(self, name, protocol):
        if name in self.providers_by_name:
            raise AlreadyDefinedEntityError.for_name(name)

        provider = ValueProvider(name, protocol)
        return self._set_provider(provider).touch()

    def install_provider(self, provider, create_if_not_set=False):
        for name in provider.attrs.names:
            if name in self.providers_by_name:
                raise AlreadyDefinedEntityError.for_name(name)

        return self._set_provider(provider).touch()

    def add_case(self, name, case_key, value):
        provider = self.get_provider(name).copy_if_mutable()
        if not isinstance(provider, ValueProvider):
            raise ValueError("Can't add case to function entity %r" % name)
        provider.check_can_add_case(case_key, value)
        provider.add_case(case_key, value)

        return self._set_provider(provider).touch()

    def clear_providers(self, names):
        state = self

        # Remember the original protocol for each provider.
        protocols_by_entity_name = {}
        for name in names:
            if not state.has_provider(name):
                continue
            provider = self.get_provider(name)
            for res_name, res_protocol in zip(
                    provider.attrs.names, provider.attrs.protocols):
                protocols_by_entity_name[res_name] = res_protocol

        # Delete the providers (or fail if not possible).
        state = state.delete_providers(names)

        # Recreate an empty version of each provider.
        for res_name, res_protocol in protocols_by_entity_name.items():
            state = state.create_provider(res_name, res_protocol)

        return state.touch()

        # TODO Consider checking downstream entity providers too.

    def delete_providers(self, names):
        state = self

        for name in names:
            # Make sure the name is safe to delete.
            if not state.has_provider(name):
                continue
            provider = state.get_provider(name)

            if isinstance(provider, ValueProvider):
                for related_name in provider.key_space:
                    if related_name not in names:
                        raise ValueError(
                            "Can't remove cases for entity %r without also "
                            "removing entities %r" % (
                                name, list(provider.key_space)))

            entity_names = provider.attrs.names
            for related_name in entity_names:
                if related_name not in names:
                    raise ValueError(
                        "Can't remove cases for entity %r without also "
                        "removing entities %r" % (
                            name, list(entity_names)))

            # Delete it.
            state = state.set(
                providers_by_name=state.providers_by_name.remove(name))

        return state.touch()

    def _set_provider(self, provider):
        state = self
        for name in provider.attrs.names:
            state = state.set(
                providers_by_name=state.providers_by_name.set(name, provider))
        return state


class FlowBuilder(object):
    """
    A mutable builder for Flows.

    Allows ``Flow`` objects to be constructed incrementally.  Use ``declare``,
    ``assign``, ``set``, and/or ``__call__`` to add entities to the builder,
    then use ``build`` to convert it into a Flow.

    Parameters
    ----------

    name: String
        Identifies the flow and provides a namespace for cached data.
    """

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
        """
        Constructs a ``Flow`` object from this builder's state.

        The returned flow is immutable and will not be affected by future
        changes to this builder's state.
        """

        state = self._state
        if state.can_be_blessed:
            state = state.bless()

        flow = Flow._from_state(state)
        flow._deriver.get_ready()

        self._state = state.touch()
        return flow

    def declare(self, name, protocol=None):
        """
        Creates a new entity but does not assign it a value.

        The entity must not already exist.

        Parameters
        ----------

        name: String
            The name of the new entity.
        protocol: Protocol, optional
            The entity's protocol.  The default is a smart type-detecting
            protocol.
        """

        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        self._state = self._state.create_provider(name, protocol)

    def assign(self, name, value=None, values=None, protocol=None):
        """
        Creates a new entity and assigns it a value.

        Exactly one of ``value`` or ``values`` must be provided.  The entity
        must not already exist.

        Parameters
        ----------

        name: String
            The name of the new entity.
        value: Object, optional
            A single value for the entity.
        values: Sequence, optional
            A sequence of values for the entity.
        protocol: Protocol, optional
            The entity's protocol.  The default is a smart type-detecting
            protocol.
        """

        check_exactly_one_present(value=value, values=values)
        if value is not None:
            values = [value]

        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        for value in values:
            protocol.validate(value)

        state = self._state

        state = state.create_provider(name, protocol)
        for value in values:
            case_key = CaseKey([(name, value, protocol.tokenize(value))])
            state = state.add_case(name, case_key, value)

        self._state = state

    def set(self, name, value=None, values=None):
        """
        Sets the value of an existing entity.

        Exactly one of ``value`` or ``values`` must be provided.  The entity
        must already exist and may already have a value (which will be
        overwritten).

        Parameters
        ----------

        name: String
            The name of the new entity.
        value: Object, optional
            A single value for the entity.
        values: Sequence, optional
            A sequence of values for the entity.
        """

        check_exactly_one_present(value=value, values=values)
        if value is not None:
            values = [value]

        state = self._state

        state = state.clear_providers([name])
        provider = state.get_provider(name)
        # This provider must have a single name and single protocol; otherwise
        # we wouldn't have been able to clear it.
        protocol, = provider.attrs.protocols

        protocol.validate(value)

        for value in values:
            case_key = CaseKey([(name, value, protocol.tokenize(value))])
            state = state.add_case(name, case_key, value)

        self._state = state

    # TODO Should we allow undeclared names?  Having to declare them first is
    # basically always clunky and annoying, but should we allow add_case to
    # create new entities?  (On the other hand, it sort of makes sense in cases
    # where we're using add_case('label', text).then_set('var', value) in order
    # to add a clean label to an unhashable value.  In that case it'd be nice
    # that we can create new entities even on a Flow.)
    def add_case(self, *name_values):
        """
        Adds a "case": a collection of associated values for a set of
        entities.

        Assigning entity values by case is an alternative to ``set`` (or
        ``assign``).  If ``set`` is used to set multiple values for some
        entities, then every combination of those values will be considered
        for downstream entities.  On the other hand, if ``add_case`` is used,
        only the specified combinations will be considered.

        Example Using ``assign``:

        .. code-block:: python

            builder = FlowBuilder()

            builder.assign('first_name', values=['Alice', 'Bob'])
            builder.assign('last_name', values=['Smith', 'Jones'])

            @builder
            def full_name(first_name, last_name):
                return first_name + ' ' + last_name

            # Prints: {'Alice Jones', 'Alice Smith', 'Bob Jones', 'Bob Smith'}
            print(builder.build().get('full_name', set))

        Example using ``add_case``:

        .. code-block:: python

            builder = FlowBuilder()

            builder.declare('first_name')
            builder.declare('last_name')

            builder.add_case('first_name', 'Alice', 'last_name', 'Jones')
            builder.add_case('first_name', 'Alice', 'last_name', 'Smith')
            builder.add_case('first_name', 'Bob', 'last_name', 'Smith')

            @builder
            def full_name(first_name, last_name):
                return first_name + ' ' + last_name

            print(builder.build().get('full_name', set))
            # Prints: {'Alice Jones', 'Alice Smith', 'Bob Smith'}

        All entities must already exist.  They may have existing values, but
        those values must have been set case-by-case with the same structure
        as this call.

        Parameters
        ----------

        name_values: String/Object
            Alternating entity names and values.

        Returns
        -------

        FlowCase
            An object which can be used to set values on additional entities
            with this case.
        """

        name_value_pairs = group_pairs(name_values)

        state = self._state

        case_nvt_tuples = []
        for name, value in name_value_pairs:
            provider = state.get_provider(name)
            if len(provider.attrs.protocols) > 1:
                raise ValueError(
                    "Can't add case for entity co-generated with other "
                    "entities %r" % (tuple(provider.attr.names),))
            protocol, = provider.attrs.protocols
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
        """
        Removes all values assigned to one or more entities.

        The values will still exist, but not have any values, as if they had
        just been created with ``declare``.  If any of the entities were set
        in a group using ``add_case``, they must all be cleared together.

        Parameters
        ----------

        names: Sequence of strings
            The entities whose values should be cleared.
        """

        self._state = self._state.clear_providers(names)

    def delete(self, *names):
        """
        Deletes one or more entities.

        If any of the entities were set in a group using ``add_case``, they
        must all be cleared together.

        Parameters
        ----------

        names: Sequence of strings
            The entities to be deleted.
        """

        self._state = self._state.delete_providers(names)

    def __call__(self, func_or_provider):
        """
        Defines an entity by providing a function that derives its value from
        other entities.

        By default, the name of the provided function will be the name of the
        new entity; the arguments of the function should be other entities.
        If the name of the new entity already exists, it will be overwritten.

        This function is intended to be used as a decorator.  However, as a
        convenience, a builder can be used as a decorator with the same effect.

        Parameters
        ----------

        func_or_provider: Function or entity
            A Python function, optionally decorated with one or more Bionic
            entity decorators.
        """

        provider = as_provider(func_or_provider)
        if provider.attrs.protocols is None:
            provider = DEFAULT_PROTOCOL(provider)
        if provider.attrs.should_persist is None:
            provider = decorators.persist(True)(provider)

        state = self._state

        state = state.delete_providers(provider.attrs.names)
        state = state.install_provider(provider)

        self._state = state

        return provider.get_source_func()

    def derive(self, func_or_provider):
        """
        (Deprecated) An alias for ``__call__``; use that instead.
        """
        warnings.warn(
            "FlowBuilder.derive is deprecated and will be repurposed in the "
            "future; use FlowBuilder.__call__ (i.e., using the builder as a "
            "decorator) instead.")

        return self.derive(func_or_provider)

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
    """
    A specific case for which entities can have associated values.

    These should be constructed by the ``FlowBuilder`` object, not by users.
    """
    def __init__(self, builder, key):
        self.key = key
        self._builder = builder

    def then_set(self, name, value):
        """Sets a single value for an entity for this case."""
        self._builder._set_for_case_key(self.key, name, value)
        return self


class Flow(object):
    """
    An immutable workflow object.  You can use get() to compute any entity
    in the workflow, or setting() to create a new workflow with modifications.
    Not all modifications are possible with this interface, but to_builder()
    can be used to get a mutable FlowBuilder version of a Flow.
    """

    # --- Public API.

    def all_entity_names(self, include_core=False):
        """
        Returns a list of all declared entity names in this flow.

        Parameters
        ----------

        include_core: Boolean, optional (default false)
            Include internal entities used for Bionic infrastructure.
        """
        return [
            name
            for name in self._state.providers_by_name.keys()
            if include_core or not self._deriver.entity_is_internal(name)
        ]

    def entity_protocol(self, name):
        """
        Returns the protocol for a given entity.

        Parameters
        ----------

        name: String
            The name of a entity.
        """

        return self._state.get_provider(name).protocol_for_name(name)

    def to_builder(self):
        """
        Returns a ``FlowBuilder`` with a copy of this ``Flow``'s state.

        Since this flow is immutable, it won't be affected by any changes to
        the returned builder.
        """

        return FlowBuilder._from_state(self._state)

    def get(self, name, fmt=None):
        """
        Computes the value(s) associated with an entity.

        If the entity has multiple values, the ``fmt`` parameter indicates
        how to handle them.  It can have any of the following values:

        * ``object``: return a single value or throw an exception
        * ``list`` or ``'list'``: return a list of values
        * ``set`` or ``'set'``: return a set of values
        * ``pandas.Series`` or ``'series'``: return a series whose index is
          the root cases distinguishing the different values

        Parameters
        ----------

        name: String
            The name of a entity.
        fmt: String or type, optional, default is ``object``
            The data structure to use if the entity has multiple values.

        Returns
        -------

        The value of the entity, or a collection containing its values.
        """

        result_group = self._deriver.derive(name)
        if fmt is None or fmt is object:
            if len(result_group) == 0:
                raise ValueError("Entity %r has no defined values" % name)
            if len(result_group) > 1:
                raise ValueError("Entity %r has multiple values" % name)
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

    # TODO Maybe this wants to be two different functions?
    def export(self, name, file_path=None, dir_path=None):
        """
        Provides access to the persisted file corresponding to an entity.

        Can be called in three ways:

        .. code-block:: python

            # Returns a path to the persisted file.
            export(name)

            # Copies the persisted file to the specified file path.
            export(name, file_path=path)

            # Copies the persisted file to the specified directory.
            export(name, dir_path=path)

        The entity must be persisted and have only one instance. The dir_path and file_path
        options support paths on GCS, specified like: gs://mybucket/subdir/
        """

        result_group = self._deriver.derive(name)
        if len(result_group) != 1:
            raise ValueError(
                "Can only export an entity if it has a single value; "
                "entity %r has %d values" % (name, len(result_group)))

        result, = result_group

        if result.cache_source_name != CACHE_SOURCE_NAME_LOCAL:
            raise ValueError("Entity %r is not locally persisted" % name)
        src_file_path = Path(result.cache_path_str)

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

        if not dst_dir_path.exists() and 'gs:/' not in str(dst_dir_path):
            dst_dir_path.mkdir(parents=True)

        dst_file_path_str = str(dst_file_path)

        if dst_file_path_str.startswith('gs:/'):
            # The path object combines // into /, so we revert it here
            copy_to_gcs(str(src_file_path), dst_file_path_str.replace('gs:/', 'gs://'))
        else:
            shutil.copyfile(str(src_file_path), dst_file_path_str)

    def declaring(self, name, protocol=None):
        """
        Like ``FlowBuilder.declare``, but returns a new copy of this flow.
        """

        return self._updating(lambda builder: builder.declare(name, protocol))

    def assigning(self, name, value=None, values=None, protocol=None):
        """
        Like ``FlowBuilder.assign``, but returns a new copy of this flow.
        """

        return self._updating(lambda builder: builder.assign(
            name, value, values, protocol))

    def setting(self, name, value=None, values=None):
        """
        Like ``FlowBuilder.set``, but returns a new copy of this flow.
        """

        return self._updating(lambda builder: builder.set(name, value, values))

    def adding_case(self, *name_values):
        """
        Like ``FlowBuilder.add_case``, but returns a new copy of this flow.
        """

        return self._updating(lambda builder: builder.add_case(*name_values))

    def then_setting(self, name, value):
        """
        Like ``FlowCase.then_set``, but returns a new copy of this flow.

        Use after calling ``Flow.adding_case``.
        """

        return self._updating(
            lambda builder: builder._set_for_last_case(name, value))

    def clearing_cases(self, *names):
        """
        Like ``FlowBuilder.clear_cases``, but returns a new copy of this flow.
        """

        return self._updating(lambda builder: builder.clear_cases(*names))

    @property
    def name(self):
        """Returns the name of this flow."""

        return self.get('core__flow_name')

    def render_dag(self, include_core=False, vertical=False, curvy_lines=False):
        """
        Returns an image with a visualization of this flow's DAG.

        Will fail if Graphviz is not installed on the system.
        """

        graph = self._deriver.export_dag(include_core)
        dot = dagviz.dot_from_graph(graph, vertical, curvy_lines)
        image = dagviz.image_from_dot(dot)
        return image

    # TODO Should we offer an in-place version of this?  It's contrary to the
    # idea of an immutable API, but it might be more natural for the user, and
    # reloading is already updating global state....
    def reloading(self):
        """
        Attempts to reload all modules used directly by this flow.

        For safety, this only works if this flow meets the following
        requirements:

        * is the first Flow built by its FlowBuilder
        * has never been modified (i.e., isn't derived from another Flow)
        * is assigned to a top-level variable in a module that one of its
          functions is defined in

        The most straightforward way to meet these requirements is to define
        your flow in a module as:

        .. code-block:: python

            builder = ...

            @builder
            def ...

            ...

            flow = builder.build()

        and then import in the notebook like so:

        .. code-block:: python

            from mymodule import flow
            ...
            flow.reloading().get('my_entity')

        This will reload the modules and use the most recent version of the
        flow before doing the ``get()``.
        """

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
        for provider in state.providers_by_name.values():
            source_func = provider.get_source_func()
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
        self._deriver = EntityDeriver(state)

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

        flow.get('my_entity')
        flow.setting('my_entity', 7)

    Alternative style:

        flow.get.my_entity()
        flow.setting.my_entity(7)

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
        return self._flow.all_entity_names()

    def __getattr__(self, name):
        return functools.partial(self._wrapped_method, name)


# Construct a default state object.
def create_default_flow_state():
    builder = FlowBuilder._with_empty_state()

    builder.declare('core__flow_name')
    builder.declare('core__persistent_cache__cloud_cache')

    builder.assign('core__persistent_cache__global_dir', 'bndata')

    @builder
    @decorators.immediate
    def core__persistent_cache__flow_dir(
            core__persistent_cache__global_dir, core__flow_name):
        return os.path.join(
            core__persistent_cache__global_dir, core__flow_name)

    # TODO I'm not sure what happens if an entity value is None, or whether
    # we want to allow it at all.  So for now we'll use a sentinel value to
    # indicate that this entity hasn't been set.
    SENTINEL_VALUE = '__IGNORE__'
    builder.assign('core__persistent_cache__gcs__bucket_name', SENTINEL_VALUE)
    builder.assign('core__persistent_cache__gcs__enabled', False)

    @builder
    @decorators.immediate
    def core__persistent_cache__gcs__object_path():
        import getpass
        return '%s/bndata/' % getpass.getuser()

    @builder
    @decorators.immediate
    def core__persistent_cache__gcs__url(
            core__persistent_cache__gcs__bucket_name,
            core__persistent_cache__gcs__object_path):
        bucket_name = core__persistent_cache__gcs__bucket_name
        object_path_str = core__persistent_cache__gcs__object_path

        if bucket_name == SENTINEL_VALUE:
            return SENTINEL_VALUE

        path = PosixPath(bucket_name) / object_path_str
        return 'gs://%s' % path

    @builder
    @decorators.immediate
    def core__persistent_cache(
                core__persistent_cache__flow_dir,
                core__persistent_cache__gcs__url,
                core__persistent_cache__gcs__enabled,
            ):
        local_flow_dir = core__persistent_cache__flow_dir
        gcs_url = core__persistent_cache__gcs__url
        gcs_enabled = core__persistent_cache__gcs__enabled

        entity_cache_dir = os.path.join(local_flow_dir, 'artifacts')
        local_cache = LocalFileCache(Path(entity_cache_dir))

        if gcs_enabled:
            if gcs_url == SENTINEL_VALUE:
                raise AssertionError(
                    'core__persistent_cache__gcs__url has invalid sentinel '
                    'value %r -- it needs to be set' % gcs_url)
            cloud_cache = GcsFileCache(posixpath.join(gcs_url, 'artifacts'))
        else:
            cloud_cache = None

        return PersistentCache(
            tmp_working_dir_str=os.path.join(local_flow_dir, 'tmp'),
            local_cache=local_cache,
            cloud_cache=cloud_cache,
        )

    return builder._state
