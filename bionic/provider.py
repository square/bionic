"""
Contains the "Provider" class hierarchy.  A provider is the object that knows
how to produce a value for one or more entities.  This module includes a
BaseProvider class and various subclasses.

The whole architecture is a bit of a mess and probably needs a substantial
rethink.
"""

import inspect
from copy import copy
from collections import defaultdict
import functools
from io import BytesIO

import pandas as pd
import attr

from .datatypes import (
    Task,
    TaskKey,
    CaseKey,
    CaseKeySpace,
    CodeFingerprint,
    CodeVersion,
)
from .exception import EntityComputationError, IncompatibleEntityError
from .descriptors.parsing import entity_dnode_from_descriptor
from .bytecode import canonical_bytecode_bytes_from_func
from .util import groups_dict, hash_to_hex, oneline
from .optdep import import_optional_dependency

import logging

logger = logging.getLogger(__name__)


class ProviderAttributes:
    def __init__(
        self,
        names,
        protocols=None,
        code_version=None,
        orig_flow_name=None,
        can_persist=None,
        can_memoize=None,
        is_default_value=None,
        docs=None,
        changes_per_run=None,
    ):
        self.names = names
        self.protocols = protocols
        self.docs = docs
        self.code_version = code_version
        self.orig_flow_name = orig_flow_name
        self._can_persist = can_persist
        self._can_memoize = can_memoize
        self.changes_per_run = changes_per_run
        self.is_default_value = is_default_value

    def should_persist(self):
        return self._can_persist

    def should_memoize(self):
        return self._can_memoize


class BaseProvider:
    def __init__(self, attrs):
        self.attrs = attrs

    def get_code_fingerprint(self, case_key):
        source_func = self.get_source_func()
        bytecode_hash = (
            None
            if source_func is None
            else hash_to_hex(canonical_bytecode_bytes_from_func(source_func))
        )

        code_version = (
            CodeVersion(None, None)
            if self.attrs.code_version is None
            else self.attrs.code_version
        )

        return CodeFingerprint(
            version=code_version,
            orig_flow_name=self.attrs.orig_flow_name,
            bytecode_hash=bytecode_hash,
        )

    def get_dependency_dnodes(self):
        return []

    def get_key_space(self, dep_key_spaces_by_dnode):
        raise NotImplementedError()

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        raise NotImplementedError()

    def get_source_func(self):
        raise NotImplementedError()

    def copy(self):
        raise NotImplementedError()

    def protocol_for_name(self, name):
        name_ix = self.attrs.names.index(name)
        if name_ix < 0:
            raise ValueError(
                oneline(
                    f"""
                Attempted to look up name {name!r} from provider
                providing only {tuple(self.attrs.names)!r}"""
                )
            )
        return self.attrs.protocols[name_ix]

    def doc_for_name(self, name):
        name_ix = self.attrs.names.index(name)
        if name_ix < 0:
            raise ValueError(
                oneline(
                    f"""
                Attempted to look up name {name!r} from provider
                providing only {tuple(self.attrs.names)!r}"""
                )
            )
        return self.attrs.docs[name_ix]

    def __repr__(self):
        return f"{self.__class__.__name__}{tuple(self.attrs.names)!r}"


class ValueProvider(BaseProvider):
    @classmethod
    def from_single_value_providers(cls, value_providers):
        for provider in value_providers:
            assert isinstance(provider, ValueProvider)
            assert len(provider.attrs.names) == 1
            assert not provider._has_any_values

        names = [provider.attrs.names[0] for provider in value_providers]
        protocols = [provider.attrs.protocols[0] for provider in value_providers]
        docs = [provider.attrs.docs[0] for provider in value_providers]

        return ValueProvider(names, protocols, docs)

    def __init__(self, names, protocols, docs):
        super(ValueProvider, self).__init__(
            attrs=ProviderAttributes(
                names=names,
                protocols=protocols,
                docs=docs,
                can_persist=False,
                can_memoize=True,
                changes_per_run=False,
            ),
        )

        self.key_space = CaseKeySpace(names)
        self._has_any_values = False
        self._value_tuples_by_case_key = {}
        self._token_tuples_by_case_key = {}

    def add_case(self, case_key, values):
        provider = self._copy()
        provider._add_case_in_place(case_key, values)
        return provider

    def _copy(self):
        # TODO This copy operation is O(N) in the number of cases we already have, which
        # means that adding N cases will take O(N^2) time. This is probably not a
        # problem for any realistic value of N, but if we wanted we could fix it by
        # using Pyrsistent PMaps instead of dicts.
        provider = copy(self)
        provider._value_tuples_by_case_key = provider._value_tuples_by_case_key.copy()
        provider._token_tuples_by_case_key = provider._token_tuples_by_case_key.copy()
        return provider

    # This mutates the provider, so it should only be called on a fresh copy, as in
    # add_case().
    def _add_case_in_place(self, case_key, values):
        tokens = []
        for value, protocol in zip(values, self.attrs.protocols):
            protocol.validate(value)
            tokens.append(protocol.tokenize(value))

        if self._has_any_values:
            if case_key.space != self.key_space:
                raise IncompatibleEntityError(
                    oneline(
                        f"""
                    Can't add {case_key!r} to entities {self.attrs.names!r}:
                    key space doesn't match {self.key_space!r}"""
                    )
                )

            if case_key in self._value_tuples_by_case_key:
                raise IncompatibleEntityError(
                    oneline(
                        f"""
                    Can't add {case_key!r} to entity {self.attrs.names!r}:
                    that case key already exists"""
                    )
                )

        if not self._has_any_values:
            self.key_space = case_key.space
            self._has_any_values = True

        self._value_tuples_by_case_key[case_key] = tuple(values)
        self._token_tuples_by_case_key[case_key] = tuple(tokens)

    def has_any_cases(self):
        return self._has_any_values

    def get_code_fingerprint(self, case_key):
        if not self.has_any_cases():
            value_tokens = ""
        else:
            value_tokens = " ".join(self._token_tuples_by_case_key[case_key])

        return CodeFingerprint(
            version=CodeVersion(major=value_tokens, minor=None,),
            bytecode_hash=None,
            orig_flow_name=None,
        )

    def get_source_func(self):
        return None

    def get_key_space(self, dep_key_spaces_by_dnode):
        assert not dep_key_spaces_by_dnode
        return self.key_space

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        assert not dep_key_spaces_by_dnode
        assert not dep_task_key_lists_by_dnode

        if self.has_any_cases():
            return [
                Task(
                    keys=[
                        TaskKey(
                            dnode=entity_dnode_from_descriptor(name), case_key=case_key,
                        )
                        for name in self.attrs.names
                    ],
                    dep_keys=[],
                    compute_func=functools.partial(self._compute, case_key=case_key,),
                    is_simple_lookup=True,
                )
                for case_key in self._value_tuples_by_case_key.keys()
            ]

        # If we have no cases, we instead return a single "missing value" task.
        else:
            return [
                Task(
                    keys=[
                        TaskKey(
                            dnode=entity_dnode_from_descriptor(name),
                            case_key=CaseKey(
                                [
                                    (name, CaseKey.MISSING, "<MISSING>")
                                    for name in self.attrs.names
                                ]
                            ),
                        )
                        for name in self.attrs.names
                    ],
                    dep_keys=[],
                    compute_func=None,
                )
            ]

    def _compute(self, dep_values, case_key):
        return self._value_tuples_by_case_key[case_key]


class FunctionProvider(BaseProvider):
    def __init__(self, func):
        name = func.__name__
        super(FunctionProvider, self).__init__(
            attrs=ProviderAttributes(
                names=[name], docs=(None if func.__doc__ is None else [func.__doc__]),
            )
        )

        self._func = func
        self.name = name
        self.dnode = entity_dnode_from_descriptor(name)

        argspec = inspect.getfullargspec(func)

        if argspec.varargs:
            raise ValueError("Functions with varargs are not supported")
        if argspec.varkw:
            raise ValueError("Functions with keyword args are not supported")
        self._dep_names = list(argspec.args)
        self._dep_dnodes = [
            entity_dnode_from_descriptor(dep_name) for dep_name in self._dep_names
        ]

    def get_dependency_dnodes(self):
        return self._dep_dnodes

    def get_source_func(self):
        return self._func

    def get_key_space(self, dep_key_spaces_by_dnode):
        return CaseKeySpace.union_all(list(dep_key_spaces_by_dnode.values()))

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        dep_case_key_lists = [
            [task_key.case_key for task_key in dep_task_key_lists_by_dnode[dep_dnode]]
            for dep_dnode in self._dep_dnodes
        ]
        out_case_keys = merge_case_key_lists(dep_case_key_lists)

        return [
            Task(
                keys=[TaskKey(dnode=self.dnode, case_key=case_key)],
                dep_keys=[
                    TaskKey(
                        dnode=dep_dnode,
                        case_key=case_key.project(dep_key_spaces_by_dnode[dep_dnode]),
                    )
                    for dep_dnode in self._dep_dnodes
                ],
                compute_func=self._apply,
            )
            for case_key in out_case_keys
        ]

    def _apply(self, dep_values):
        try:
            value = self._func(*dep_values)
        except Exception as e:
            entity_description = (
                f"entity {self.attrs.names[0]!r}"
                if len(self.attrs.names) == 1
                else f"entities {self.attrs.names!r}"
            )
            raise EntityComputationError(
                oneline(
                    f"""
                An exception was thrown while computing the value of
                {entity_description}
                """
                )
            ) from e
        return [value]

    def __repr__(self):
        return f"{self.__class__.__name__}({self._func})"


class WrappingProvider(BaseProvider):
    def __init__(self, wrapped_provider):
        super(WrappingProvider, self).__init__(wrapped_provider.attrs)
        self.wrapped_provider = wrapped_provider

    def get_dependency_dnodes(self):
        return self.wrapped_provider.get_dependency_dnodes()

    def get_key_space(self, dep_key_spaces_by_dnode):
        return self.wrapped_provider.get_key_space(dep_key_spaces_by_dnode)

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        return self.wrapped_provider.get_tasks(
            dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode,
        )

    def get_source_func(self):
        return self.wrapped_provider.get_source_func()

    def __repr__(self):
        return f"{self.__class__.__name__}({self.wrapped_provider})"


class AttrUpdateProvider(WrappingProvider):
    def __init__(self, wrapped_provider, attr_name, attr_value, allow_override=False):
        super(AttrUpdateProvider, self).__init__(wrapped_provider)

        old_attr_value = getattr(wrapped_provider.attrs, attr_name)
        if old_attr_value is not None and not allow_override:
            raise ValueError(
                oneline(
                    f"""
                Attempted to set attribute {attr_name!r} twice
                on {wrapped_provider!r};
                old value was {old_attr_value!r},
                new value is {attr_value!r}"""
                )
            )

        self.attrs = copy(wrapped_provider.attrs)
        setattr(self.attrs, attr_name, attr_value)


class ProtocolUpdateProvider(WrappingProvider):
    def __init__(self, wrapped_provider, protocol=None):
        super(ProtocolUpdateProvider, self).__init__(wrapped_provider)

        protocols = [protocol for name in self.wrapped_provider.attrs.names]

        self.attrs = copy(wrapped_provider.attrs)
        self.attrs.protocols = protocols


class MultiProtocolUpdateProvider(WrappingProvider):
    def __init__(self, wrapped_provider, protocols=None):
        super(MultiProtocolUpdateProvider, self).__init__(wrapped_provider)

        self.attrs = copy(wrapped_provider.attrs)
        self.attrs.protocols = protocols


class RenamingProvider(WrappingProvider):
    def __init__(self, wrapped_provider, name):

        super(RenamingProvider, self).__init__(wrapped_provider)

        orig_names = wrapped_provider.attrs.names
        if len(orig_names) != 1:
            raise ValueError(
                oneline(
                    f"""
                Can't rename a provider that already has multiple
                names; need exactly one name but got {tuple(orig_names)!r}"""
                )
            )

        self.attrs = copy(wrapped_provider.attrs)
        self.attrs.names = [name]

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        (name,) = self.attrs.names

        def wrap_task(task):
            (task_key,) = task.keys
            return Task(
                keys=[
                    TaskKey(
                        dnode=entity_dnode_from_descriptor(name),
                        case_key=task_key.case_key,
                    )
                ],
                dep_keys=task.dep_keys,
                compute_func=task.compute,
            )

        inner_tasks = self.wrapped_provider.get_tasks(
            dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode,
        )
        return [wrap_task(task) for task in inner_tasks]


class NameSplittingProvider(WrappingProvider):
    def __init__(self, wrapped_provider, names=None):

        super(NameSplittingProvider, self).__init__(wrapped_provider)

        orig_names = wrapped_provider.attrs.names
        if len(orig_names) != 1:
            raise ValueError(
                oneline(
                    f"""
                Can't change a provider's number of names multiple times;
                need exactly one name but got {tuple(orig_names)!r}"""
                )
            )

        self.attrs = copy(wrapped_provider.attrs)
        self.attrs.names = names
        if self.attrs.protocols is not None:
            (protocol,) = self.attrs.protocols
            self.attrs.protocols = [protocol for name in names]

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        inner_tasks = self.wrapped_provider.get_tasks(
            dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode,
        )

        def wrap_task(task):
            assert len(task.keys) == 1
            (task_key,) = task.keys

            def wrapped_compute_func(dep_values):
                (value_seq,) = task.compute(dep_values)

                if len(value_seq) != len(self.attrs.names):
                    raise ValueError(
                        oneline(
                            f"""
                        Expected provider
                        {self.wrapped_provider.attrs.names[0]!r} to return
                        {len(self.attrs.names)} outputs named
                        {self.attrs.names!r}; got {len(value_seq)} outputs
                        {tuple(value_seq)!r}"""
                        )
                    )

                return tuple(value_seq)

            return Task(
                keys=[
                    TaskKey(
                        dnode=entity_dnode_from_descriptor(name),
                        case_key=task_key.case_key,
                    )
                    for name in self.attrs.names
                ],
                dep_keys=task.dep_keys,
                compute_func=wrapped_compute_func,
            )

        return [wrap_task(task) for task in inner_tasks]


class GatherProvider(WrappingProvider):
    def __init__(
        self, wrapped_provider, primary_names, secondary_names, gathered_dep_name
    ):
        # TODO This is still pretty confusing, I think.
        """
        This a very involved wrapper implementing the "gather" decorator.  It
        collects multiple dependencies into a single DataFrame argument, which
        is accessible to the wrapped provider along with its other arguments.
        Multiple instances of these dependencies may be grouped in a single
        DataFrame rather than being passed to separate instances of this
        provider.

        The gathered dependencies are provided in two groups: "primary" and
        "secondary"; putting dependencies in one group or the other will change
        how the grouping happens.  If a dependency is "primary", then any
        variation caused by it (i.e., variation from its ancestors) will be
        collapsed into a single frame.  Secondary dependencies do not affect
        the grouping; any variation due to them will result in multiple
        instances of this provider, as normal.
        """

        super(GatherProvider, self).__init__(wrapped_provider)

        self._primary_dnodes = [
            entity_dnode_from_descriptor(name) for name in primary_names
        ]
        self._secondary_dnodes = [
            entity_dnode_from_descriptor(name) for name in secondary_names
        ]
        self._inner_gathered_dep_dnode = entity_dnode_from_descriptor(gathered_dep_name)

        self._inner_dep_dnodes = self.wrapped_provider.get_dependency_dnodes()

        self._gather_dnodes = list(self._primary_dnodes) + list(self._secondary_dnodes)

        inner_gathered_dep_ix = self._inner_dep_dnodes.index(
            self._inner_gathered_dep_dnode
        )
        if inner_gathered_dep_ix < 0:
            inner_gathered_dep_descriptor = (
                self._inner_gathered_dep_dnode.to_descriptor()
            )
            inner_dep_descriptors = [
                dnode.to_descriptor() for dnode in self._inner_dep_dnodes
            ]
            raise ValueError(
                oneline(
                    f"""
                Expected wrapped {self.wrapped_provider!r}
                to have dependency name {inner_gathered_dep_descriptor!r},
                but only found names {inner_dep_descriptors!r}"""
                )
            )

        self._passthrough_dep_dnodes = list(self._inner_dep_dnodes)
        self._passthrough_dep_dnodes.pop(inner_gathered_dep_ix)
        assert self._inner_gathered_dep_dnode not in self._passthrough_dep_dnodes

        extra_dep_dnodes = [
            dnode
            for dnode in self._gather_dnodes
            if dnode not in self._passthrough_dep_dnodes
        ]
        self._outer_dep_dnodes = extra_dep_dnodes + self._passthrough_dep_dnodes

    def get_dependency_dnodes(self):
        return self._outer_dep_dnodes

    def get_key_space(self, dep_key_spaces_by_dnode):
        return self._compute_key_spaces(dep_key_spaces_by_dnode).outer

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        # These are the key spaces and task keys that the outside world sees.
        outer_key_spaces_by_dnode = dep_key_spaces_by_dnode
        outer_dtkls_by_dnode = dep_task_key_lists_by_dnode

        # We'll need to derive some useful key spaces.
        key_spaces = self._compute_key_spaces(dep_key_spaces_by_dnode)

        # Identify the full case keys that will be partitioned into
        # gathered frames.
        gather_case_key_lists = [
            [task_key.case_key for task_key in outer_dtkls_by_dnode[dnode]]
            for dnode in self._gather_dnodes
        ]
        gather_case_keys = merge_case_key_lists(gather_case_key_lists)

        # For each of these case keys, collect a "row" of the associated dependency
        # task keys.
        gather_rows = []
        for gather_case_key in gather_case_keys:
            dep_task_keys = []
            for dep_dnode in self._gather_dnodes:
                dep_key_space = dep_key_spaces_by_dnode[dep_dnode]
                dep_case_key = gather_case_key.project(dep_key_space)
                dep_task_key = TaskKey(dep_dnode, dep_case_key)
                dep_task_keys.append(dep_task_key)

            gather_row = GatherRow(
                dep_task_keys=dep_task_keys, full_case_key=gather_case_key,
            )
            gather_rows.append(gather_row)

        # Now partition these rows into tables. Each table corresponds to one gathered
        # dataframe.
        gather_row_lists_by_delta_case_key = groups_dict(
            gather_rows, lambda row: row.full_case_key.project(key_spaces.delta)
        )
        gather_tables_by_out_task_key = {}
        for (
            delta_case_key,
            all_gather_rows,
        ) in gather_row_lists_by_delta_case_key.items():
            gather_rows_without_missing_case_key_values = [
                gather_row
                for gather_row in all_gather_rows
                if not any(
                    dep_task_key.case_key.project(key_spaces.primary).has_missing_values
                    for dep_task_key in gather_row.dep_task_keys
                )
            ]
            table_task_key = TaskKey(
                dnode=self._inner_gathered_dep_dnode, case_key=delta_case_key,
            )
            gather_table = GatherTable(
                rows=gather_rows_without_missing_case_key_values,
                out_task_key=table_task_key,
            )
            gather_tables_by_out_task_key[table_task_key] = gather_table

        # Create new task keys for the gathered frame values that the inner provider
        # will consume.
        inner_gathered_key_space = key_spaces.delta
        inner_gathered_dep_task_keys = list(gather_tables_by_out_task_key.keys())

        # Now we can construct the dicts of key spaces and task keys that the
        # wrapper provider will see.
        inner_key_spaces_by_dnode = {
            dnode: (
                inner_gathered_key_space
                if dnode == self._inner_gathered_dep_dnode
                else outer_key_spaces_by_dnode[dnode]
            )
            for dnode in self._inner_dep_dnodes
        }
        inner_dtkls_by_dnode = {
            dnode: (
                inner_gathered_dep_task_keys
                if dnode == self._inner_gathered_dep_dnode
                else outer_dtkls_by_dnode[dnode]
            )
            for dnode in self._inner_dep_dnodes
        }

        # Define how we'll convert an inner task to an outer task.
        def wrap_task(task):
            # Identify the index of the inner task key that corresponds to the
            # aggregated value.  That's the one we'll be replacing.
            inner_dep_keys = task.dep_keys
            (gather_task_key_ix,) = [
                ix
                for ix, dep_key in enumerate(inner_dep_keys)
                if dep_key.dnode == self._inner_gathered_dep_dnode
            ]

            # Remove the key for the aggregated value, and remember its case
            # key.
            passthrough_dep_keys = list(inner_dep_keys)
            inner_gather_task_key = passthrough_dep_keys.pop(gather_task_key_ix)

            # Identify the table that this task corresponds to.
            gather_table = gather_tables_by_out_task_key[inner_gather_task_key]

            # NOTE prepended_keys has a non-deterministic order, because a
            # set's ordering depends on the hashes of its contents, and TaskKey
            # hashes depend on string hashes, and string hashes are randomized
            # in recent versions of Python.
            # See: https://stackoverflow.com/questions/27522626/hash-function-in-python-3-3-returns-different-results-between-sessions  # noqa: E501
            # We could fix this by sorting this list, but rather than trying
            # to eliminate every source of non-deterministic ordering, I think
            # it's better to sort our values at the point where we actually
            # need determinism.  (Viz., computing the provenance hash.)
            unique_gather_dep_keys = set(
                dep_task_key
                for gather_row in gather_table.rows
                for dep_task_key in gather_row.dep_task_keys
            )
            prepended_keys = list(unique_gather_dep_keys)

            # Combine the gathering task keys with the keys expected by the
            # wrapped task (except the one key we removed, since we'll be
            # synthesizing it ourselves).
            wrapped_dep_keys = prepended_keys + passthrough_dep_keys

            def wrapped_compute_func(dep_values):
                # Split off the extra values for the keys we prepended.
                prepended_values = dep_values[: len(prepended_keys)]
                passthrough_values = dep_values[len(prepended_keys) :]

                values_by_task_key = dict(list(zip(prepended_keys, prepended_values)))

                # Gather the prepended values into a single frame.
                gathered_df = pd.DataFrame()
                for dep_ix, dnode in enumerate(self._gather_dnodes):
                    gathered_df[dnode.to_descriptor()] = [
                        values_by_task_key.get(gather_row.dep_task_keys[dep_ix], None)
                        for gather_row in gather_table.rows
                    ]

                # Construct the final values to pass to the wrapped task.
                inner_dep_values = passthrough_values
                inner_dep_values.insert(gather_task_key_ix, gathered_df)

                return task.compute(inner_dep_values)

            # NOTE This Task object will not be picklable, because its
            # compute_func is a nested function.  If we ever want to send Tasks
            # over the network, we need to either use dill or refactor this
            # code.
            return Task(
                keys=task.keys,
                dep_keys=wrapped_dep_keys,
                compute_func=wrapped_compute_func,
            )

        orig_tasks = self.wrapped_provider.get_tasks(
            inner_key_spaces_by_dnode, inner_dtkls_by_dnode,
        )
        return [wrap_task(task) for task in orig_tasks]

    def _compute_key_spaces(self, dep_key_spaces_by_dnode):
        return self._KeySpaces(self, dep_key_spaces_by_dnode)

    class _KeySpaces:
        def __init__(self, gather_provider, dep_key_spaces_by_dnode):
            # The combined keyspace of all the non-gathered dependencies of the
            # wrapped provider.
            self.passthrough = CaseKeySpace.union_all(
                dep_key_spaces_by_dnode[dnode]
                for dnode in gather_provider._passthrough_dep_dnodes
            )

            # The combined keyspace of the all the primary gathered
            # dependencies.  This corresponds to the index of the gathered
            # frame.
            self.primary = CaseKeySpace.union_all(
                dep_key_spaces_by_dnode[dnode]
                for dnode in gather_provider._primary_dnodes
            )

            # The combined keyspace of the all the secondary gathered
            # dependencies.
            self.secondary = CaseKeySpace.union_all(
                dep_key_spaces_by_dnode[dnode]
                for dnode in gather_provider._secondary_dnodes
            )

            # The difference between the secondary and primary key spaces.
            # This is the key space of the gathered frame provider that the
            # wrapped provider sees.
            self.delta = self.secondary.difference(self.primary)

            # The combination of the passthrough and delta key spaces -- i.e.,
            # the combined key space of all the dependencies seen by the
            # wrapped provider.  This is also the key space of this provider.
            self.outer = self.passthrough.union(self.delta)


# TODO Matplotlib has global state, which means it may run differently and
# produce different output (or errors) in Jupyter vs in a script.  I think this
# may produce some annoying gotchas in the future.  Some possible solutions:
# 1. Have all tasks run in a separate process, at least by default.
# 2. Have special handling for certain tasks that need to run in a separate
#    process.
# 3. Try to make Bionic's matplotlib initialization identical to Jupyter's.
class PyplotProvider(WrappingProvider):
    def __init__(self, wrapped_provider, name="pyplot", savefig_kwargs=None):
        super(PyplotProvider, self).__init__(wrapped_provider)

        self._Image = import_optional_dependency(
            "PIL.Image", purpose="the @pyplot decorator"
        )

        self._pyplot_dnode = entity_dnode_from_descriptor(name)

        self._savefig_kwargs = {
            "format": "png",
            "bbox_inches": "tight",
        }
        if savefig_kwargs is not None:
            self._savefig_kwargs.update(savefig_kwargs)

        inner_dep_dnodes = wrapped_provider.get_dependency_dnodes()
        if self._pyplot_dnode not in inner_dep_dnodes:
            inner_dep_descriptors = [
                dnode.to_descriptor() for dnode in inner_dep_dnodes
            ]
            raise ValueError(
                oneline(
                    f"""
                When using {self.__class__.__name__},
                expected wrapped {wrapped_provider} to have a dependency
                named {name!r}; only found {inner_dep_descriptors!r}"""
                )
            )
        self._outer_dep_dnodes = list(inner_dep_dnodes)
        self._outer_dep_dnodes.remove(self._pyplot_dnode)

    def get_dependency_dnodes(self):
        return self._outer_dep_dnodes

    def get_key_space(self, dep_key_spaces_by_dnode):
        outer_dkss = dep_key_spaces_by_dnode
        inner_dkss = outer_dkss.copy()
        inner_dkss[self._pyplot_dnode] = CaseKeySpace()

        return self.wrapped_provider.get_key_space(inner_dkss)

    def get_tasks(self, dep_key_spaces_by_dnode, dep_task_key_lists_by_dnode):
        outer_dkss = dep_key_spaces_by_dnode
        inner_dkss = outer_dkss.copy()
        inner_dkss[self._pyplot_dnode] = CaseKeySpace()

        outer_dtkls = dep_task_key_lists_by_dnode
        inner_dtkls = outer_dtkls.copy()
        inner_dtkls[self._pyplot_dnode] = [
            TaskKey(dnode=self._pyplot_dnode, case_key=CaseKey([]))
        ]

        inner_tasks = self.wrapped_provider.get_tasks(inner_dkss, inner_dtkls)

        def wrap_task(task):
            inner_dep_keys = task.dep_keys
            (pyplot_dep_ix,) = [
                ix
                for ix, dep_key in enumerate(inner_dep_keys)
                if dep_key.dnode == self._pyplot_dnode
            ]

            outer_dep_keys = list(inner_dep_keys)
            outer_dep_keys.pop(pyplot_dep_ix)

            def wrapped_compute_func(dep_values):
                import_optional_dependency("matplotlib", purpose="plotting")
                from matplotlib import pyplot as plt

                outer_dep_values = dep_values

                inner_dep_values = list(outer_dep_values)
                inner_dep_values.insert(pyplot_dep_ix, plt)

                # Create a new figure so our task has a blank canvas to work
                # with.
                plt.figure()

                # Run the task, which will do the plotting.
                values = task.compute(inner_dep_values)
                if values != [None]:
                    raise ValueError(
                        oneline(
                            f"""
                        Providers wrapped by {self.__class__.__name__}
                        should not return values;
                        got values {tuple(values)!r}"""
                        )
                    )

                # Save the plot into a buffer.
                bio = BytesIO()
                plt.savefig(bio, **self._savefig_kwargs)
                plt.close()
                # Reset the buffer's position so that when we read from it, we
                # read from the beginning.
                bio.seek(0)
                # Load the buffer into an Image object.
                image = self._Image.open(bio)

                return [image]

            return Task(
                keys=task.keys,
                dep_keys=outer_dep_keys,
                compute_func=wrapped_compute_func,
            )

        outer_tasks = [wrap_task(task) for task in inner_tasks]
        return outer_tasks


# -- Helpers for managing case keys.


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
            cur_case_keys, common_key_space.select
        )

        # Likewise, group the already-merged keys.
        merged_key_lists_by_common_key = groups_dict(
            merged_case_keys, common_key_space.select
        )

        # For each distinct common key, take the Cartesian product of the
        # new and already-merged keys.
        merged_case_keys = []
        for common_key, merged_keys in merged_key_lists_by_common_key.items():
            for cur_key in cur_key_lists_by_common_key.get(common_key, []):
                for merged_key in merged_keys:
                    new_merged_key = merged_key.merge(cur_key)
                    merged_case_keys.append(new_merged_key)

        merged_key_space = merged_key_space.union(cur_key_space)

    return merged_case_keys


class HashableWrapper:
    """
    Wraps an arbitrary object along with a hashable token.

    Can be used as a hash key even if the wrapped object can't.
    """

    def __init__(self, value, token):
        self._value = value
        self._token = str(token)

    def get(self):
        return self._value

    def __hash__(self):
        return hash(self._token)

    def __eq__(self, other):
        if not isinstance(other, HashableWrapper):
            return False
        return self._token == other._token

    def __str__(self):
        return f"W({self._value})"

    def __repr__(self):
        return f"HashableWrapper({self._value!r})"


def multi_index_from_case_keys(case_keys, ordered_key_names):
    assert len(ordered_key_names) > 0
    return pd.MultiIndex.from_tuples(
        tuples=[
            tuple(
                HashableWrapper(case_key.values[name], case_key.tokens[name])
                for name in ordered_key_names
            )
            for case_key in case_keys
        ],
        names=ordered_key_names,
    )


@attr.s
class GatherRow:
    """
    A collection of task keys corresponding to one row of a gathered dataframe.
    """

    dep_task_keys = attr.ib()
    full_case_key = attr.ib()


@attr.s
class GatherTable:
    """
    Holds the task keys for one gathered dataframe. Contains the task keys required
    to compute each cell (organized by row) and one task key corresponding to the
    assembled frame itself.
    """

    rows = attr.ib()
    out_task_key = attr.ib()


# -- Helpers for working with providers.

PROVIDER_METHODS = [
    "get_code_fingerprint",
    "get_dependency_dnodes",
    "get_tasks",
    "get_source_func",
]


def is_provider(obj):
    return all(hasattr(obj, method_name) for method_name in PROVIDER_METHODS)


def is_func_or_provider(obj):
    return callable(obj) or is_provider(obj)


def as_provider(func_or_provider):
    if is_provider(func_or_provider):
        provider = func_or_provider
    elif callable(func_or_provider):
        provider = FunctionProvider(func_or_provider)
    else:
        raise ValueError("func must be either callable or a Provider")

    return provider


def provider_wrapper(wrapper_fn, *args, **kwargs):
    def decorator(func_or_provider):
        provider = as_provider(func_or_provider)
        return wrapper_fn(provider, *args, **kwargs)

    return decorator
