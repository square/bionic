"""
Contains various data structures used by Bionic's infrastructure.
"""

from collections import namedtuple

import attr

from .util import ImmutableSequence, ImmutableMapping


@attr.s(frozen=True)
class TaskKey:
    """
    A unique identifier for a Task.
    """

    dnode = attr.ib()
    case_key = attr.ib()

    def __str__(self):
        args_str = ", ".join(f"{name}={value}" for name, value in self.case_key.items())
        return f"{self.dnode.to_entity_name()}({args_str})"


@attr.s(frozen=True)
class Task:
    """
    A unit of work.  Can have dependencies, which are referred to via their
    TaskKeys.

    Attributes
    ----------
    keys: list of TaskKeys
        Keys corresponding to the output values computed by this task.
    dep_keys: list of TaskKeys
        Keys corresponding to the input values required by this task.
    compute_func: function taking a single ``dep_values`` argument
        Generates output values based on the passed input values.
    is_simple_lookup: boolean
        Whether this task consists of simply looking up the fixed value of an entity;
        used to determine what message to log when this task is computed.
    """

    keys = attr.ib(converter=tuple)
    dep_keys = attr.ib(converter=tuple)
    compute_func = attr.ib()
    is_simple_lookup = attr.ib(default=False)

    def compute(self, dep_values):
        return self.compute_func(dep_values)

    def key_for_entity_name(self, name):
        matching_keys = [
            task_key
            for task_key in self.keys
            if task_key.dnode.to_entity_name() == name
        ]
        (key,) = matching_keys
        return key

    def __repr__(self):
        return f"Task({self.keys!r}, {self.dep_keys!r})"


@attr.s(frozen=True)
class Query:
    """
    Represents a request for a specific entity value.
    """

    task_key = attr.ib()
    protocol = attr.ib()
    provenance = attr.ib()

    @property
    def dnode(self):
        return self.task_key.dnode

    @property
    def case_key(self):
        return self.task_key.case_key

    def __repr__(self):
        return f"Query({self.task_key}, {self.provenance!r})"


@attr.s(frozen=True)
class Result:
    """
    Represents one value for one entity.
    """

    query = attr.ib()
    value = attr.ib()
    file_path = attr.ib(default=None)
    value_hash = attr.ib(default=None)
    value_is_missing = attr.ib(default=False)

    def __repr__(self):
        return f"Result({self.query!r}, {self.value!r})"


@attr.s(frozen=True)
class ProvenanceDigest:
    """
    A collection of values used by Provenance for different chained hashes.
    These hashes depend on the entities and can come from either another
    provenance or the value hash of a result.
    """

    functional_hash = attr.ib()
    exact_hash = attr.ib()

    @classmethod
    def from_provenance(cls, provenance):
        return cls(provenance.functional_hash, provenance.exact_hash)

    @classmethod
    def from_value_hash(cls, value_hash):
        return cls(value_hash, value_hash)


class CaseKeySpace(ImmutableSequence):
    """
    A set of CaseKey names (without values) -- represents a space of possible
    CaseKeys.
    """

    def __init__(self, names=None):
        if names is None:
            names = []
        super(CaseKeySpace, self).__init__(sorted(names))

    def union(self, other):
        return CaseKeySpace(set(self).union(other))

    def intersection(self, other):
        return CaseKeySpace(name for name in self if name in other)

    def difference(self, other):
        return CaseKeySpace(name for name in self if name not in other)

    def select(self, case_key):
        return case_key.project(self)

    @classmethod
    def union_all(cls, spaces):
        if not spaces:
            return CaseKeySpace([])
        names = set()
        for space in spaces:
            names = names.union(space)
        return CaseKeySpace(names)

    @classmethod
    def intersection_all(cls, spaces):
        if not spaces:
            raise ValueError("Can't take the intersection of zero spaces")
        names = None
        for space in spaces:
            if names is None:
                names = set(spaces)
            else:
                names = names.intersection(space)
        return CaseKeySpace(names)

    def __repr__(self):
        return f'CaseKeySpace({", ".join(repr(name) for name in self)})'


@attr.s(frozen=True)
class MissingCaseKeyValue:
    pass


class CaseKey(ImmutableMapping):
    """
    A collection of name-value pairs that uniquely identifies a case.
    """

    # This is a sentinel value used to indicate that no value is available. We can't use
    # None because None is itself a valid value.
    # Normally I would prefer to represent missing-ness out-of-band by making the
    # `missing_names` field the source of truth here, but the relational methods like
    # `project` are cleaner when we use a sentinel value.
    MISSING = MissingCaseKeyValue()

    def __init__(self, name_value_token_triples):
        values_by_name = {
            name: value for name, value, token in name_value_token_triples
        }
        tokens_by_name = {
            name: token for name, value, token in name_value_token_triples
        }

        super(CaseKey, self).__init__(tokens_by_name)
        self._nvt_triples = name_value_token_triples
        self.values = values_by_name
        self.tokens = tokens_by_name
        self.space = CaseKeySpace(list(values_by_name.keys()))
        self.missing_names = [
            name
            for name, value, token in name_value_token_triples
            # We don't want to say `value == MISSING` because `value` might override the
            # `==` operator to mean something different (like a Pandas DataFrame does).
            if type(value) is MissingCaseKeyValue
        ]
        self.has_missing_values = len(self.missing_names) > 0

    def project(self, key_space):
        return CaseKey(
            [
                (name, value, token)
                for name, value, token in self._nvt_triples
                if name in key_space
            ]
        )

    def drop(self, key_space):
        return CaseKey(
            [
                (name, value, token)
                for name, value, token in self._nvt_triples
                if name not in key_space
            ]
        )

    def merge(self, other):
        vt_pairs_by_name = {
            name: (value, token) for name, value, token in self._nvt_triples
        }

        for name, value, token in other._nvt_triples:
            if name in vt_pairs_by_name:
                assert token == vt_pairs_by_name[name][1]
            else:
                vt_pairs_by_name[name] = value, token

        return CaseKey(
            [(name, value, token) for name, (value, token) in vt_pairs_by_name.items()]
        )

    def __repr__(self):
        args_str = ", ".join(f"{name}={token}" for name, token in self.items())
        return f"CaseKey({args_str})"


class ResultGroup(ImmutableSequence):
    """
    Represents a collection of Results, distinguished by their CaseKeys.  Each
    CaseKey should have the same set of names.
    """

    def __init__(self, results, key_space):
        super(ResultGroup, self).__init__(results)

        self.key_space = key_space

    def __repr__(self):
        return f"ResultGroup({list(self)!r})"


def str_from_version_value(value):
    if value is None:
        return "0"
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, str):
        return value
    else:
        raise ValueError(f"Version values must be str, int, or None: got {value!r}")


@attr.s(frozen=True)
class CodeVersion:
    """
    Contains the user-designated version of a piece of code, consisting of a
    major and a minor version string.  The convention is that changing the
    major version indicates a functional change, while changing the minor
    version indicates a nonfunctional change.
    """

    major = attr.ib(converter=str_from_version_value)
    minor = attr.ib(converter=str_from_version_value)


# A collection of characteristics attempting to uniquely identify a function.
CodeFingerprint = namedtuple("CodeFingerprint", "version bytecode_hash orig_flow_name")


# Encodes the versioning rules to use when computing entity values.
VersioningPolicy = namedtuple(
    "VersioningPolicy", "check_for_bytecode_errors treat_bytecode_as_functional"
)
