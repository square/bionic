"""
Contains the FlowBuilder and Flow classes, which implement the core workflow
construction and execution APIs (respectively).
"""

import os
import shutil
import warnings
from collections import defaultdict
from pathlib import Path, PosixPath
from importlib import reload
from textwrap import dedent
from uuid import uuid4

import pyrsistent as pyrs
import pandas as pd

# A bit annoying that we have to rename this when we import it.
from . import protocols as protos
from .cache_api import Cache
from .datatypes import (
    CaseKey,
    EntityDefinition,
    VersioningPolicy,
    ResultGroup,
)
from .exception import (
    UndefinedEntityError,
    AlreadyDefinedEntityError,
    IncompatibleEntityError,
    UnsetEntityError,
    AttributeValidationError,
)
from .executor import Executor
from .persistence import LocalStore, GcsCloudStore, PersistentCache
from .provider import (
    ValueProvider,
    HashableWrapper,
    AttrUpdateProvider,
)
from .deriver import EntityDeriver, entity_is_internal
from .descriptors.parsing import entity_dnode_from_descriptor
from . import decorators, decoration
from .util import (
    group_pairs,
    check_exactly_one_present,
    check_at_most_one_present,
    copy_to_gcs,
    FileCopier,
    oneline,
)

DEFAULT_PROTOCOL = protos.CombinedProtocol(
    # Every GeoPandas DataFrame is also a Pandas DataFrame, so we need to do the
    # GeoPandas check first.
    protos.GeoPandasProtocol(),
    protos.ParquetDataFrameProtocol(),
    protos.ImageProtocol(),
    protos.NumPyProtocol(),
    protos.DaskProtocol(),
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
    entity_defs_by_name = pyrs.field(initial=pyrs.pmap())
    default_entity_names = pyrs.field(initial=pyrs.pset())
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

    def define_entity(self, entity_def):
        if entity_def.name in self.entity_defs_by_name:
            raise AlreadyDefinedEntityError(entity_def.name)

        if entity_is_internal(entity_def.name):
            if entity_def.can_persist:
                message = f"""
                Attempted to set @persist to True for Bionic internal
                entity {entity_def.name!r}.
                Disable persistence for the decorators by applying
                `@persist(False)` or `@immediate` to the corresponding
                function or passing `persist=False` when you
                `declare` / `assign` the entity values.
                """
                raise AttributeValidationError(oneline(message))
            if entity_def.optional_should_memoize is False:
                message = f"""
                Attempted to set @memoize to False for Bionic internal
                entity {entity_def.name!r}.
                Enable memoization by removing `@memoize(False)` from the
                corresponding function.
                """
                raise AttributeValidationError(oneline(message))

        return self._set_entity_def(entity_def).touch()

    def delete_entity_defs(self, names):
        state = self

        for name in names:
            if name not in self.entity_defs_by_name:
                continue
            state = state._erase_entity_def(name).touch()

        return state.touch()

    def get_entity_def(self, name):
        if name not in self.entity_defs_by_name:
            raise UndefinedEntityError.for_name(name)
        return self.entity_defs_by_name[name]

    def get_provider(self, name):
        if name not in self.providers_by_name:
            raise UndefinedEntityError.for_name(name)
        return self.providers_by_name[name]

    def has_provider(self, name):
        return name in self.providers_by_name

    def create_provider(self, name):
        if name in self.providers_by_name:
            raise AlreadyDefinedEntityError.for_name(name)

        provider = ValueProvider([name])
        return self._set_provider(provider).touch()

    def install_provider(self, provider):
        for name in provider.entity_names:
            if name in self.providers_by_name:
                raise AlreadyDefinedEntityError.for_name(name)

        return self._set_provider(provider).touch()

    def group_names_together(self, names):
        providers = [self.get_provider(name) for name in names]
        providers_set = set(providers)

        # If there's only one provider, then the grouping is already done.
        if len(providers_set) == 1:
            provider = providers[0]
            if set(provider.entity_names) != set(names):
                if len(names) == 1:
                    message = f"""
                    Can't assign to individual entity {names[0]!r}:
                    it's already defined jointly with entities
                    {provider.entity_names!r},
                    and all subsequent assignments must be to that same group
                    """
                else:
                    message = f"""
                    Can't jointly assign to entities {names!r}:
                    they're already defined jointly with entities
                    {provider.entity_names!r},
                    and all subsequent assignments must be to that same group
                    """
                raise IncompatibleEntityError(oneline(message))

            return self

        # If each name has a different provider, we'll try to combine into a single
        # provider.
        elif len(providers_set) == len(names):
            for provider in providers:
                if len(provider.entity_names) != 1:
                    message = f"""
                    Can't jointly assign to entities {names!r}:
                    they overlap with existing grouping {provider.entity_names!r}
                    """
                    raise IncompatibleEntityError(oneline(message))

                if not isinstance(provider, ValueProvider):
                    message = f"""
                    Can't jointly assign to entities {names!r}:
                    entity {provider.entity_names[0]!r} is already defined as a
                    function
                    """
                    raise IncompatibleEntityError(oneline(message))

                if provider.has_any_cases():
                    message = f"""
                    Can't jointly assign to entities {names!r}:
                    entity {provider.entity_names[0]!r} already has individual
                    values assigned to it
                    """
                    raise IncompatibleEntityError(oneline(message))

            provider = ValueProvider.from_single_value_providers(providers)
            return self._set_provider(provider).touch()

        else:
            groupings = [provider.entity_names for provider in providers]
            message = f"""
            Can't jointly assign to entities {names!r}:
            they're already in separate groupings {", ".join(repr(groupings))}
            """
            raise IncompatibleEntityError(oneline(message))

    def add_case(self, case_key, names, values):
        (provider,) = set(self.get_provider(name) for name in names)
        if not isinstance(provider, ValueProvider):
            raise IncompatibleEntityError(
                f"Can't add case to function entities {names!r}"
            )

        # We should already have called group_names_together(), so we know the names
        # argument should match the names of the provider. However, the names may not
        # be in the expected order. If not, we'll reorder the names and values to
        # correspond to the expected order.
        names_in_orig_order = provider.entity_names
        if names != names_in_orig_order:
            values = [values[names.index(name)] for name in names_in_orig_order]
            names = names_in_orig_order

        tokens = []
        for name, value in zip(names, values):
            protocol = self.get_entity_def(name).protocol
            protocol.validate(value)
            tokens.append(protocol.tokenize(value))
        provider = provider.add_case(case_key, values, tokens)

        return self._set_provider(provider).touch()

    def clear_providers(self, names):
        state = self

        # Delete the providers (or fail if not possible).
        state = state.delete_providers(names)

        # Recreate an empty version of each provider.
        entity_names = [name for name in names if name in state.entity_defs_by_name]
        for name in entity_names:
            state = state.create_provider(name)

        return state.touch()

        # TODO Consider checking downstream entity providers too.

    def delete_providers(self, names):
        state = self

        for name in names:
            # Make sure the name is safe to delete.
            if not state.has_provider(name):
                continue
            provider = state.get_provider(name)

            joint_names = provider.entity_names
            for related_name in joint_names:
                if related_name not in names:
                    raise IncompatibleEntityError(
                        oneline(
                            f"""
                        Can't remove cases for entity {name!r} without also
                        removing entities {joint_names!r}"""
                        )
                    )

            # Delete it.
            state = state._erase_provider(name)

        return state.touch()

    def mark_all_entities_default(self):
        new_default_names = pyrs.pset(self.entity_defs_by_name.keys())
        return self.set(default_entity_names=new_default_names).touch()

    def _erase_entity_def(self, name):
        assert name not in self.providers_by_name

        state = self
        state = state.set(entity_defs_by_name=state.entity_defs_by_name.remove(name),)
        return state

    def _erase_provider(self, name):
        state = self
        if name in state.providers_by_name:
            state = state.set(providers_by_name=state.providers_by_name.remove(name),)
        if name in state.default_entity_names:
            state = state.set(
                default_entity_names=(state.default_entity_names.remove(name))
            )
        return state

    def _set_entity_def(self, entity_def):
        return self.set(
            entity_defs_by_name=self.entity_defs_by_name.set(
                entity_def.name, entity_def
            ),
        )

    def _set_provider(self, provider):
        for name in provider.entity_names:
            assert name in self.entity_defs_by_name

        state = self
        for name in provider.entity_names:
            state = state._erase_provider(name)
            state = state.set(
                providers_by_name=state.providers_by_name.set(name, provider),
            )
        return state


class FlowBuilder:
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

    def declare(self, name, protocol=None, doc=None, docstring=None, persist=True):
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
        doc: String, optional
            Description of the new entity.
        persist: Boolean, optional (default: True)
            Whether this entity's values should be cached persistently.
            The only reason to set this to False is if an internal Bionic
            entity depends on it; in this case, persistence is impossible
            because Bionic's cache won't be constructed by the time this
            entity is calculated. The downside of setting this to False is
            that it won't be possible to retrieve a serialized file for
            this entity using the mode argument to Flow.get.
        """

        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        if docstring is not None:
            check_at_most_one_present(doc=doc, docstring=docstring)
            warnings.warn(
                "The `docstring` argument to `FlowBuilder.declare` is "
                "deprecated; use `doc` instead."
            )
            doc = docstring

        self._state = self._state.define_entity(
            EntityDefinition(
                name=name,
                protocol=protocol,
                doc=doc,
                can_persist=persist,
                optional_should_memoize=True,
            )
        ).create_provider(name)

    def assign(
        self,
        name,
        value=None,
        values=None,
        protocol=None,
        doc=None,
        docstring=None,
        persist=True,
    ):
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
        doc: String, optional
            Description of the new entity.
        persist: Boolean, optional (default: True)
            Whether this entity's values should be cached persistently.
            The only reason to set this to False is if an internal Bionic
            entity depends on it; in this case, persistence is impossible
            because Bionic's cache won't be constructed by the time this
            entity is calculated. The downside of setting this to False is
            that it won't be possible to retrieve a serialized file for
            this entity using the mode argument to Flow.get.
        """

        check_at_most_one_present(value=value, values=values)
        if values is None:
            values = [value]

        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        if docstring is not None:
            check_at_most_one_present(doc=doc, docstring=docstring)
            warnings.warn(
                "The `docstring` argument to `FlowBuilder.assign` is "
                "deprecated; use `doc` instead."
            )
            doc = docstring

        # TODO We can remove this validation, since it also happens in add_case below.
        for value in values:
            protocol.validate(value)

        state = self._state

        state = state.define_entity(
            EntityDefinition(
                name=name,
                protocol=protocol,
                doc=doc,
                can_persist=persist,
                optional_should_memoize=True,
            )
        )
        state = state.create_provider(name)
        for value in values:
            case_key = CaseKey([(name, protocol.tokenize(value))])
            state = state.add_case(case_key, [name], [value])

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

        check_at_most_one_present(value=value, values=values)
        if values is None:
            values = [value]

        state = self._state

        state = state.clear_providers([name])

        protocol = state.get_entity_def(name).protocol

        for value in values:
            # TODO We can remove this validation, since it also happens in add_case
            # below.
            protocol.validate(value)
            case_key = CaseKey([(name, protocol.tokenize(value))])
            state = state.add_case(case_key, [name], [value])

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

        names = [name for name, value in name_value_pairs]
        state = state.group_names_together(names)

        values = []
        name_token_pairs = []
        for name, value in name_value_pairs:
            protocol = state.get_entity_def(name).protocol
            # TODO Both the validation and tokenization are also happening in
            # state.add_case below; maybe we can remove some duplicate work.
            protocol.validate(value)
            token = protocol.tokenize(value)

            values.append(value)
            name_token_pairs.append((name, token))

        case_key = CaseKey(name_token_pairs)

        state = state.add_case(case_key, names, values)

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

        self._state = self._state.delete_providers(names).delete_entity_defs(names)

    def merge(self, flow, keep="error", allow_name_match=False):
        """
        Updates this builder by importing all entities from another flow.

        If any incoming entity has the same name as an existing entity, the
        conflict is resolved by apply the following rules, in order:

        1. The name (`core__flow_name`) of this builder is never changed; the
           original value is always kept.
        2. Entities that were set by default (not explicitly set by the user)
           are never imported and can be overwritten.
        3. Assignments (definitions with values) take precedence over
           declarations (definitions with no values).
        4. Otherwise, the ``keep`` parameter can be used to specify which
           entity to keep.

        Parameters
        ----------

        flow: Flow
            Any Bionic Flow.

        keep: 'error', 'self', or 'arg' (default: 'error')
            How to handle conflicting entity names.  Options:

            * 'error': throw an ``AlreadyDefinedEntityError``
            * 'self' or 'old': use the definition from this builder
            * 'arg' or 'new': use the definition from ``flow``

        allow_name_match: boolean (default: False)
            Allows the incoming flow to have the same name as this builder.
            (If this is False, we handle duplicate names by throwing an
            exception.  It's technically possible to share a name between
            flows, but it's generally not good practice.)
        """

        # These are the two states we're going to merge.
        new_state = flow._state
        old_state = self._state

        # Check that the flows don't have the same name.
        # TODO The mechanics of this check really suck, since this builder's
        # name is stored like any other entity, but we have to figure it out
        # without using an EntityDeriver.  Since this is a best-effort check, I
        # guess it's not a huge deal.  But overall, the way we store a flow's
        # name might deserve to be revisited later.
        if not allow_name_match:
            old_name_provider = old_state.providers_by_name.get("core__flow_name")
            if (
                old_name_provider is not None
                and isinstance(old_name_provider, ValueProvider)
                and len(old_name_provider._value_tuples_by_case_key.keys()) == 1
            ):
                (
                    (old_flow_name,),
                ) = old_name_provider._value_tuples_by_case_key.values()
                new_flow_name = flow.name
                if old_flow_name == new_flow_name and not allow_name_match:
                    raise ValueError(
                        oneline(
                            f"""
                        Attempting to merge two flows with the same name
                        ({new_flow_name!r}).
                        Sharing names between flows is generally unwise,
                        since they will then also share the same cache space;
                        however, you can disable this check by passing
                        ``allow_name_match=True``."""
                        )
                    )

        # Identify all the names that could appear in the merged flow, and
        # associate each one with a potential conflict.
        all_names = set(old_state.providers_by_name.keys()).union(
            new_state.providers_by_name.keys()
        )
        conflicts_by_name = {
            name: MergeConflict(old_state=old_state, new_state=new_state, name=name)
            for name in all_names
        }

        # Resolve each conflict individually.
        for conflict in conflicts_by_name.values():
            if conflict.old_provider is None:
                conflict.resolve("new", "no conflicting definition")
                continue

            if conflict.new_provider is None:
                conflict.resolve("old", "no conflicting definition")
                continue

            if conflict.name == "core__flow_name":
                conflict.resolve("old", "flow name is never merged")
                continue

            if conflict.new_is_default:
                conflict.resolve("old", "conflicting definition is default")
                continue

            if conflict.old_is_default:
                conflict.resolve("new", "conflicting definition is default")
                continue

            if conflict.old_entity_def.protocol is conflict.new_entity_def.protocol:
                if conflict.new_is_only_declaration:
                    conflict.resolve(
                        "old",
                        "conflicting definition has matching protocol and " "no value",
                    )
                    continue
                elif conflict.old_is_only_declaration:
                    conflict.resolve(
                        "new",
                        "conflicting definition has matching protocol and " "no value",
                    )
                    continue

            if keep == "error":
                raise AlreadyDefinedEntityError(
                    oneline(
                        f"""
                    Merge failure: Entity {conflict.name!r} exists in both
                    old and new flows;
                    use the ``keep`` argument to specify which to keep"""
                    )
                )

            elif keep == "self":
                conflict.resolve("old", "keep=self")
                continue

            elif keep == "arg":
                conflict.resolve("new", "keep=arg")
                continue

            elif keep == "old":
                conflict.resolve("old", "keep=old")
                continue

            elif keep == "new":
                conflict.resolve("new", "keep=new")
                continue

            raise ValueError(
                "Value of ``keep`` must be one of {'error', 'self', 'arg'}; "
                f"got {keep!r}"
            )

        # For both states, check that each jointly-defined name group is kept
        # or discarded as a whole.
        for state_name, state in [
            ("old", old_state),
            ("new", new_state),
        ]:
            for provider in state.providers_by_name.values():
                names = provider.entity_names
                if len(names) == 1:
                    continue

                conflicts = [conflicts_by_name[name] for name in names]
                kept_conflicts = [
                    conflict
                    for conflict in conflicts
                    if conflict.resolution == state_name
                ]
                discarded_conflicts = [
                    conflict
                    for conflict in conflicts
                    if conflict.resolution != state_name
                ]
                if kept_conflicts and discarded_conflicts:
                    kept = ", ".join(f"{c.name} ({c.reason})" for c in kept_conflicts)
                    discarded = ", ".join(
                        f"{c.name} ({c.reason})" for c in discarded_conflicts
                    )
                    raise IncompatibleEntityError(
                        oneline(
                            f"""
                        Merge failure: Names {names!r} in {state_name} state
                        are defined jointly and must be kept or discarded
                        together,
                        but merge logic dictates that we keep [{kept}] and
                        discard [{discarded}];
                        you should manually remove some of these names from
                        one of the flows before merging"""
                        )
                    )

        # Now we start building up our final, merged state.
        cur_state = old_state

        conflicts_keeping_new = [
            conflict
            for conflict in conflicts_by_name.values()
            if conflict.resolution == "new"
        ]

        # First, delete all old providers that collide with our incoming ones.
        names_to_delete = [conflict.name for conflict in conflicts_keeping_new]
        try:
            cur_state = cur_state.delete_providers(names_to_delete)
            cur_state = cur_state.delete_entity_defs(names_to_delete)
        except IncompatibleEntityError as e:
            raise IncompatibleEntityError("Merge failure: " + e)

        # Then install each new provider -- keeping in mind that a provider
        # may have multiple names and hence multiple conflicts, but should be
        # installed exactly once.
        providers_to_install = {
            tuple(conflict.new_provider.entity_names): conflict.new_provider
            for conflict in conflicts_keeping_new
        }.values()
        for provider in providers_to_install:
            # For function providers, we attach the name of their original flow
            # so that their cached data won't be confused with whatever we
            # had before.
            if (
                not isinstance(provider, ValueProvider)
                and provider.attrs.orig_flow_name is None
            ):
                provider = AttrUpdateProvider(provider, "orig_flow_name", new_flow_name)
            for name in provider.entity_names:
                new_entity_def = new_state.get_entity_def(name)
                cur_state = cur_state.define_entity(new_entity_def)
            cur_state = cur_state.install_provider(provider)

        self._state = cur_state

    def __call__(self, func):
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

        func: Function
            A Python function, optionally decorated with one or more Bionic
            entity decorators.
        """

        decoration.init_accumulator_if_not_set_on_func(func)
        acc = decoration.pop_accumulator_from_func(func)

        provider = acc.provider

        # TODO Data like `changes_per_run` and `version` should probably also be stored
        # in the DecorationAccumulator and then attached to some kind of
        # FunctionAttributes object.
        if provider.attrs.changes_per_run is None:
            provider = AttrUpdateProvider(provider, "changes_per_run", False)

        names = provider.entity_names

        protocol = acc.protocol
        if protocol is None:
            protocol = DEFAULT_PROTOCOL

        docs = acc.docs
        orig_docstring = provider.get_source_func().__doc__
        if docs is None:
            if orig_docstring is not None and len(names) > 1:
                preamble = """
                Using a single doc string for a multi-output function is
                deprecated and will become an error condition in a future
                release; use the ``@docs`` decorator to specify
                one doc string for each entity.  Details:
                """
                details = f"""
                got {len(names)} names {tuple(names)!r} and
                just one docstring {orig_docstring}
                """
                warnings.warn(oneline(preamble) + "\n" + oneline(details))

            docs = [orig_docstring for name in names]
        if len(docs) != len(names):
            message = f"""
            Number of docs must match the number of names;
            got {len(names)} names {tuple(names)!r} and
            {len(docs)} docs {tuple(docs)!r}"""
            raise ValueError(oneline(message))

        can_persist = acc.can_persist
        if can_persist is None:
            can_persist = True

        state = self._state

        # Delete the original definitions.
        # TODO Deleting the original entity definition is a little counterintuitive. It
        # would probably make more sense to keep the original definition, although
        # that would be a breaking change at this point. We need to think through the
        # relationship between APIs for fixed and derived entities.
        state = state.delete_providers(provider.entity_names)
        state = state.delete_entity_defs(provider.entity_names)

        # Create the new definitions.
        for name, doc in zip(names, docs):
            entity_def = EntityDefinition(
                name=name,
                protocol=protocol,
                doc=doc,
                can_persist=can_persist,
                optional_should_memoize=acc.should_memoize,
            )
            state = state.define_entity(entity_def)
        state = state.install_provider(provider)

        self._state = state

        return provider.get_source_func()

    def derive(self, func):
        """
        (Deprecated) An alias for ``__call__``; use that instead.
        """
        warnings.warn(
            "FlowBuilder.derive is deprecated and will be repurposed in the "
            "future; use FlowBuilder.__call__ (i.e., using the builder as a "
            "decorator) instead."
        )

        return self.derive(func)

    # --- Private helpers.

    @classmethod
    def _from_state(cls, state):
        return cls(name=None, _state=state)

    @classmethod
    def _with_empty_state(cls):
        return cls(name=None, _state=FlowState())

    def _set_name(self, name):
        self.set("core__flow_name", name)

    def _set_for_case_key(self, case_key, name, value):
        self._state = self._state.add_case(case_key, [name], [value])

    def _set_for_last_case(self, name, value):
        last_case_key = self._state.last_added_case_key
        if last_case_key is None:
            raise ValueError("A case must have been added before calling this method")

        self._set_for_case_key(last_case_key, name, value)


class MergeConflict:
    def __init__(self, old_state, new_state, name):
        self.name = name
        self.old_provider = old_state.providers_by_name.get(name)
        self.new_provider = new_state.providers_by_name.get(name)
        self.old_is_default = name in old_state.default_entity_names
        self.new_is_default = name in new_state.default_entity_names
        self.old_entity_def = old_state.entity_defs_by_name.get(name)
        self.new_entity_def = new_state.entity_defs_by_name.get(name)

    @property
    def old_is_only_declaration(self):
        return self._provider_is_only_declaration(self.old_provider)

    @property
    def new_is_only_declaration(self):
        return self._provider_is_only_declaration(self.new_provider)

    def _provider_is_only_declaration(self, provider):
        return isinstance(provider, ValueProvider) and not provider.has_any_cases()

    def resolve(self, resolution, reason):
        self.resolution = resolution
        self.reason = reason


class FlowCase:
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


class Flow:
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
            if include_core or not entity_is_internal(name)
        ]

    def entity_protocol(self, name):
        """
        Returns the protocol for a given entity.

        Parameters
        ----------

        name: String
            The name of an entity.
        """

        return self._state.get_entity_def(name).protocol

    def entity_doc(self, name):
        """
        Returns the doc for the named entity if one is defined, otherwise
        return None.

        Parameters
        ----------

        name: String
            The name of an entity.
        """
        return self._state.get_entity_def(name).doc

    def entity_docstring(self, name):
        """
        (Deprecated in favor of `entity_doc`.)
        Returns the doc for the named entity if one is defined, otherwise
        return None.

        Parameters
        ----------

        name: String
            The name of an entity.
        """
        warnings.warn(
            "`Flow.entity_docstring` is deprecated; " "use `Flow.entity_doc` instead."
        )
        return self.entity_doc(name)

    def to_builder(self):
        """
        Returns a ``FlowBuilder`` with a copy of this ``Flow``'s state.

        Since this flow is immutable, it won't be affected by any changes to
        the returned builder.
        """

        return FlowBuilder._from_state(self._state)

    def get(self, name, collection=None, fmt=None, mode=object):
        """
        Computes the value(s) associated with an entity.

        If the entity has multiple values, the ``collection`` parameter
        indicates how to handle them.  It can have any of the following values:

        * ``None``: return a single value or throw an exception
        * ``list`` or ``'list'``: return a list of values
        * ``set`` or ``'set'``: return a set of values
        * ``pandas.Series`` or ``'series'``: return a series whose index is
          the root cases distinguishing the different values

        The user can specify the type of object (implicitly specifying in-memory vs. persisted data)
        to return in the collection using the ``mode`` parameter.  It can have any of the
        following values:
        * ``object`` or ``'object'`` for a value in-memory
        * ``'FileCopier'`` for a wrapper for a path to the persisted file for the computed entity
        * ``Path`` or ``'path'`` for a path to persisted file
        * ``'filename'`` for a string representing a path to a persisted file

        Parameters
        ----------

        name: String
            The name of an entity.

        collection: String or type, optional, default is ``None``
            The data structure to use if the entity has multiple values.

        fmt: String or type, optional, default is ``None``
            The data structure to use if the entity has multiple values.  Deprecated in favor of
            ``collection`` and will be removed in future release.

        mode: String or type, optional, default is ``object``
            The type of object to return in the collection.

        Returns
        -------

        The value of the entity, or a collection containing its values.
        """

        dnode = entity_dnode_from_descriptor(name)
        orig_result_group = self._deriver.derive(dnode)
        # Remove all results with missing values.
        result_group = ResultGroup(
            results=[
                result for result in orig_result_group if not result.value_is_missing
            ],
            key_space=orig_result_group.key_space,
        )

        if mode is object or mode == "object":
            values = [result.value for result in result_group]
        else:
            # all other modes expect the entity to be persisted
            result_file_paths = [result.file_path for result in result_group]

            if None in result_file_paths:
                raise ValueError(
                    oneline(
                        f"""
                    Entity {name!r} is not persisted but persisted file is
                    expected by mode {mode!r}"""
                    )
                )

            if mode is Path or mode == "path":
                values = result_file_paths
            elif mode == "FileCopier":
                values = [FileCopier(fp) for fp in result_file_paths]
            elif mode == "filename":
                values = [str(fp) for fp in result_file_paths]
            else:
                raise ValueError(f"Unrecognized mode {mode!r}")

        check_at_most_one_present(fmt=fmt, collection=collection)

        if fmt:
            warnings.warn(
                "The fmt argument is deprecated and will be removed in a future release. "
                "Please use collection as a replacement."
            )

        collection = fmt or collection

        if collection is None or collection is object:
            if len(values) == 0:
                # There should always be at least one result, but some of them may have
                # missing values.
                assert len(orig_result_group) > 0
                missing_result = orig_result_group[0]
                missing_names = missing_result.query.case_key.missing_names
                message = f"""
                Entity {name} could not be computed because the following entities are
                declared but have no values set:
                {", ".join(repr(name) for name in missing_names)}
                """
                raise UnsetEntityError(oneline(message))
            if len(values) > 1:
                raise ValueError(f"Entity {name!r} has multiple values")
            return values[0]
        elif collection is list or collection == "list":
            return values
        elif collection is set or collection == "set":
            return set(values)
        elif collection is pd.Series or collection == "series":
            key_space = list(result_group.key_space)
            if len(key_space) > 0:
                ancestor_values_by_case_key_by_name = defaultdict(dict)
                ancestor_key_space_by_name = {}
                for ancestor_name in key_space:
                    ancestor_result_group = self._deriver.derive(
                        entity_dnode_from_descriptor(ancestor_name)
                    )
                    ancestor_key_space_by_name[
                        ancestor_name
                    ] = ancestor_result_group.key_space
                    for ancestor_result in ancestor_result_group:
                        ancestor_case_key = ancestor_result.query.case_key
                        ancestor_values_by_case_key_by_name[ancestor_name][
                            ancestor_case_key
                        ] = ancestor_result.value

                orig_case_keys = [result.query.case_key for result in result_group]
                index = pd.MultiIndex.from_tuples(
                    tuples=[
                        tuple(
                            HashableWrapper(
                                value=ancestor_values_by_case_key_by_name[
                                    ancestor_name
                                ][
                                    orig_case_key.project(
                                        ancestor_key_space_by_name[ancestor_name]
                                    )
                                ],
                                token=orig_case_key.tokens[ancestor_name],
                            )
                            for ancestor_name in key_space
                        )
                        for orig_case_key in orig_case_keys
                    ],
                    names=list(key_space),
                )
            else:
                index = None
            return pd.Series(name=name, data=values, index=index,)
        else:
            raise ValueError(f"Unrecognized collection type {collection!r}")

    def export(self, name, file_path=None, dir_path=None):
        """
        Provides access to the persisted file corresponding to an entity.  Note:
        this method is deprecated and the same functionality is available through Flow#get.

        Can be called in three ways:

        .. code-block:: python

            # Returns a path to the persisted file.
            export(name)

            # Copies the persisted file to the specified file path.
            export(name, file_path=path)

            # Copies the persisted file to the specified directory.
            export(name, dir_path=path)

        The entity must be persisted and have only one instance. The dir_path
        and file_path options support paths on GCS, specified like:
        gs://mybucket/subdir/
        """

        warnings.warn(
            "Flow#export is deprecated; use the mode argument to Flow#get " "instead"
        )

        dnode = entity_dnode_from_descriptor(name)
        result_group = self._deriver.derive(dnode)
        if len(result_group) != 1:
            raise ValueError(
                oneline(
                    f"""
                Can only export an entity if it has a single value;
                entity {name!r} has {len(result_group)} values"""
                )
            )

        (result,) = result_group

        if result.file_path is None:
            raise ValueError(f"Entity {name!r} is not locally persisted")
        src_file_path = result.file_path

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

        if not dst_dir_path.exists() and "gs:/" not in str(dst_dir_path):
            dst_dir_path.mkdir(parents=True)

        dst_file_path_str = str(dst_file_path)

        if dst_file_path_str.startswith("gs:/"):
            # The path object combines // into /, so we revert it here
            copy_to_gcs(str(src_file_path), dst_file_path_str.replace("gs:/", "gs://"))
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

        return self._updating(
            lambda builder: builder.assign(name, value, values, protocol)
        )

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

        return self._updating(lambda builder: builder._set_for_last_case(name, value))

    def clearing_cases(self, *names):
        """
        Like ``FlowBuilder.clear_cases``, but returns a new copy of this flow.
        """

        return self._updating(lambda builder: builder.clear_cases(*names))

    def merging(self, flow, keep="error"):
        """
        Like ``FlowBuilder.merge``, but returns a new copy of this flow.
        """

        return self._updating(lambda builder: builder.merge(flow, keep))

    @property
    def name(self):
        """Returns the name of this flow."""

        return self.get("core__flow_name")

    def render_dag(self, include_core=False, vertical=False, curvy_lines=False):
        """
        Returns a ``FlowImage`` with a visualization of this flow's DAG. This
        object behaves similarly to a Pillow ``Image`` object.

        Will fail if Graphviz is not installed on the system.
        """

        from . import dagviz

        graph = self._deriver.export_dag(include_core)
        dot = dagviz.dot_from_graph(
            graph=graph, vertical=vertical, curvy_lines=curvy_lines, name=self.name,
        )
        return dagviz.FlowImage(dot)

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
                "from its builder and it hasn't been modified"
            )

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
            module = reload(module_registry[module_name])
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
                    oneline(
                        f"""
                    Found a matching flow, but it had been modified:
                    {self_name!r}"""
                    )
                )
            else:
                raise Exception(
                    oneline(
                        f"""
                    Couldn't find any flow named {self_name!r}
                    in modules {module_names!r}"""
                    )
                )
        if len(blessed_candidate_flows) > 1:
            raise Exception(
                oneline(
                    f"""
                Too many flows named {self_name!r}
                in modules {module_names!r};
                found {len(blessed_candidate_flows)}, wanted 1"""
                )
            )
        (flow,) = blessed_candidate_flows

        return flow

    # --- Private helpers.

    @classmethod
    def _from_state(self, state):
        return Flow(_official=True, state=state)

    def __init__(self, state, _official=False):
        if not _official:
            raise ValueError(
                "Don't construct this class directly; "
                "use one of the classmethod constructors"
            )

        self._uuid = str(uuid4())
        self._state = state
        self._deriver = EntityDeriver(state, self._uuid)

        # We replace the `get` and `setting` methods with wrapper classes
        # that have an attribute for each entity.  This allows a convenient,
        # autocompletable way to access entities. We subclass the ShortcutProxy
        # class for each method so that the docstring shows up properly in help
        # text. (We could instantiate the ShortcutProxy class and dynamically
        # set the docstring, but tools like `help` access the class definition
        # directly and won't pick up the updated docstring.)
        orig_get_method = self.get
        orig_setting_method = self.setting

        class GetMethod(ShortcutProxy):
            __doc__ = orig_get_method.__doc__

            def __init__(self):
                super(GetMethod, self).__init__(
                    orig_get_method, 'Computes the value(s) for "{name}"'
                )

        self.get = GetMethod()

        self.cache = Cache(self._deriver)

        class SettingMethod(ShortcutProxy):
            __doc__ = orig_setting_method.__doc__

            def __init__(self):
                super(SettingMethod, self).__init__(
                    orig_setting_method,
                    oneline(
                        '''
                    Returns a copy of this flow with an updated value of
                    "{name}"'''
                    ),
                )

        self.setting = SettingMethod()

    def _updating(self, builder_update_func):
        builder = FlowBuilder._from_state(self._state)
        builder_update_func(builder)
        return Flow._from_state(builder._state)


class ShortcutProxy:
    """
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
    """

    def __init__(self, wrapped_method, docstring_prefix_template):
        self._wrapped_method = wrapped_method
        self._docstring_prefix_template = docstring_prefix_template
        self._flow = wrapped_method.__self__
        assert isinstance(self._flow, Flow)

        self.__name__ = wrapped_method.__name__

    def __call__(self, *args, **kwargs):
        return self._wrapped_method(*args, **kwargs)

    def __dir__(self):
        return self._flow.all_entity_names()

    def __getattr__(self, name):
        def partial(*args, **kwargs):
            return self._wrapped_method(name, *args, **kwargs)

        partial.__name__ = name

        # Set a useful docstring for this function.
        # First, the prefix explains what the function actually does.
        doc_prefix = self._docstring_prefix_template.format(name=name)

        # If the related entity has any documentation, we include that
        # afterwards.
        entity_doc = self._flow.entity_doc(name)
        if entity_doc is None:
            main_docstring = doc_prefix + "."
        else:
            clean_entity_doc = dedent(entity_doc).strip()
            main_docstring = f"{doc_prefix}:\n{clean_entity_doc}"

        method_name = self._wrapped_method.__name__
        partial.__doc__ = (
            f"{main_docstring}"
            "\n\n"
            "This function is equivalent to "
            f"``{method_name}('{name}', *args, **kwargs)``"
        )

        return partial


# Construct a default state object.
def create_default_flow_state():
    builder = FlowBuilder._with_empty_state()

    builder.declare("core__flow_name", persist=False)

    builder.assign("core__memoize_by_default", True, persist=False)
    builder.assign("core__persistent_cache__global_dir", "bndata", persist=False)
    builder.assign("core__versioning_mode", "manual", persist=False)

    @builder
    @decorators.immediate
    def core__versioning_policy(core__versioning_mode):
        if core__versioning_mode == "manual":
            return VersioningPolicy(
                treat_bytecode_as_functional=False, check_for_bytecode_errors=False,
            )
        elif core__versioning_mode == "assist":
            return VersioningPolicy(
                treat_bytecode_as_functional=False, check_for_bytecode_errors=True,
            )
        elif core__versioning_mode == "auto":
            return VersioningPolicy(
                treat_bytecode_as_functional=True, check_for_bytecode_errors=False,
            )
        else:
            raise ValueError(
                oneline(
                    f"""
                core__versioning_mode must be one of
                ('manual', 'assist', 'auto');
                got {core__versioning_mode!r}"""
                )
            )

    @builder
    @decorators.immediate
    def core__persistent_cache__flow_dir(
        core__persistent_cache__global_dir, core__flow_name
    ):
        return os.path.join(core__persistent_cache__global_dir, core__flow_name)

    builder.assign("core__persistent_cache__gcs__bucket_name", None, persist=False)
    builder.assign("core__persistent_cache__gcs__enabled", False, persist=False)

    @builder
    @decorators.immediate
    def core__persistent_cache__gcs__object_path():
        import getpass

        return f"{getpass.getuser()}/bndata/"

    @builder
    @decorators.immediate
    def core__persistent_cache__gcs__url(
        core__persistent_cache__gcs__bucket_name,
        core__persistent_cache__gcs__object_path,
    ):
        bucket_name = core__persistent_cache__gcs__bucket_name
        object_path_str = core__persistent_cache__gcs__object_path

        if bucket_name is None:
            return None

        path = PosixPath(bucket_name) / object_path_str
        return f"gs://{path}"

    @builder
    @decorators.immediate
    def core__persistent_cache__local_store(core__persistent_cache__flow_dir,):
        local_flow_dir = core__persistent_cache__flow_dir
        return LocalStore(local_flow_dir)

    @builder
    @decorators.immediate
    def core__persistent_cache__cloud_store(
        core__persistent_cache__gcs__url, core__persistent_cache__gcs__enabled,
    ):
        gcs_url = core__persistent_cache__gcs__url
        gcs_enabled = core__persistent_cache__gcs__enabled
        if gcs_enabled:
            if gcs_url is None:
                raise AssertionError(
                    "core__persistent_cache__gcs__url is None, " "but needs a value"
                )
            return GcsCloudStore(gcs_url)
        else:
            return None

    @builder
    @decorators.immediate
    def core__persistent_cache(
        core__persistent_cache__local_store, core__persistent_cache__cloud_store,
    ):
        return PersistentCache(
            local_store=core__persistent_cache__local_store,
            cloud_store=core__persistent_cache__cloud_store,
        )

    builder.assign("core__parallel_execution__enabled", False, persist=False)
    # The executor uses max available CPUs when set to None.
    builder.assign("core__parallel_execution__worker_count", None, persist=False)

    @builder
    @decorators.immediate
    def core__executor(
        core__parallel_execution__enabled, core__parallel_execution__worker_count,
    ):
        if not core__parallel_execution__enabled:
            return None
        return Executor(core__parallel_execution__worker_count)

    return builder._state.mark_all_entities_default()
