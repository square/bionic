"""
Contains various data structures used by Bionic's infrastructure.
"""

import attr

from .utils.misc import ImmutableSequence, ImmutableMapping


@attr.s(frozen=True)
class EntityDefinition:
    """
    Describes the immutable properties of an entity. These properties generally have
    to do with the entity's "contract": the assumptions other parts of the system can
    make about its value. However, this does *not* include the way the entity's value
    is determined; this is configured separately and can be changed more easily.

    Attributes
    ----------
    name: string
        The name of the entity.
    protocol: Protocol
        The protocol to use when serializing and deserializing entity values on disk.
    doc: string
        A human-readable description of the entity.
    optional_should_memoize: boolean or None
        Whether the entity should be memoized, or None if the global default should be
        used.
    optional_should_persist: boolean or None
        Whether the entity should be persisted, or None if the global default should be
        used
    needs_caching: boolean
        Indicates that some kind of caching needs to be enabled for this entity (either
        persistence or memoization).
    """

    name = attr.ib()
    protocol = attr.ib()
    doc = attr.ib()
    optional_should_memoize = attr.ib()
    optional_should_persist = attr.ib()
    needs_caching = attr.ib(default=False)


@attr.s(frozen=True)
class DescriptorMetadata:
    """
    Holds extra data we might need when working with a descriptor.

    Similar to an EntityDefinition, but can apply to non-entity descriptors, and also
    incorporates information from the global configuration. (For example,
    EntityDefinition has an `optional_should_memoize` field which describes the
    user's memoization preferences, if any; this class has a `should_memoize` field
    which describes what we'll actually do, based on both user preferences and the
    global configuration.)

    Attributes
    ----------
    protocol: Protocol
        The protocol to use when serializing and deserializing descriptor values on
        disk.
    doc: string
        A human-readable description of the descriptor.
    should_memoize: boolean
        Whether the value should be memoized for the lifetime of its Flow instance.
    should_memoize_for_query: boolean
        Whether the value should be memoized for the lifetime of a Flow.get() call.
        (Only relevant if ``should_memoize`` is False.)
    should_persist: boolean
        Whether the value should be persisted.
    is_composite: boolean
        Whether the value contains other descriptor values. (If so, it's desirable to
        get it out of memory quickly.)
    """

    protocol = attr.ib()
    doc = attr.ib()
    should_memoize = attr.ib(default=False)
    should_memoize_for_query = attr.ib(default=False)
    should_persist = attr.ib(default=False)
    is_composite = attr.ib(default=True)


@attr.s(frozen=True)
class TaskKey:
    """
    A unique identifier for a Task.
    """

    dnode = attr.ib()
    case_key = attr.ib()

    def evolve(self, **kwargs):
        return attr.evolve(self, **kwargs)

    def __str__(self):
        args_str = ", ".join(f"{name}={value}" for name, value in self.case_key.items())
        return f"{self.dnode.to_descriptor(near_commas=True)}({args_str})"


@attr.s(frozen=True)
class Task:
    """
    A unit of work.  Can have dependencies, which are referred to via their
    TaskKeys.

    Attributes
    ----------
    key: TaskKey
        Key corresponding to the output value computed by this task.
    dep_keys: list of TaskKeys
        Keys corresponding to the input values required by this task.
    compute_func: function taking a single ``dep_values`` argument
        Generates output values based on the passed input values.
    is_simple_lookup: boolean
        Whether this task consists of simply looking up the fixed value of an entity;
        used to determine what message to log when this task is computed.
    """

    key = attr.ib()
    dep_keys = attr.ib(converter=tuple)
    compute_func = attr.ib()
    is_simple_lookup = attr.ib(default=False)

    def compute(self, dep_values):
        return self.compute_func(dep_values)

    @property
    def can_be_serialized(self):
        return not self.is_simple_lookup

    def evolve(self, **kwargs):
        return attr.evolve(self, **kwargs)

    def __repr__(self):
        return f"Task({self.key!r}, {self.dep_keys!r})"


@attr.s(frozen=True)
class Result:
    """
    Represents one value for one entity.
    """

    task_key = attr.ib()
    value = attr.ib()
    local_artifact = attr.ib()
    value_is_missing = attr.ib(default=False)

    def __repr__(self):
        return f"Result({self.task_key!r}, {self.value!r})"


@attr.s(frozen=True)
class Artifact:
    """
    Represents a serialized, file-like artifact, either on a local filesystem or in a
    cloud object store.
    """

    url: str = attr.ib()
    content_hash: str = attr.ib()

    def evolve(self, **kwargs):
        return attr.evolve(self, **kwargs)


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


class CaseKey(ImmutableMapping):
    """
    A collection of name-token pairs that uniquely identifies a case.
    """

    def __init__(self, name_token_pairs):
        tokens_by_name = {name: token for name, token in name_token_pairs}

        super(CaseKey, self).__init__(tokens_by_name)
        self._name_token_pairs = name_token_pairs
        self.tokens = tokens_by_name
        self.space = CaseKeySpace(list(tokens_by_name.keys()))
        self.missing_names = [
            # None is a sentinel value used to indicate that no value is available.
            # Normally I would prefer to represent missing-ness out-of-band by making the
            # `missing_names` field the source of truth here, but the relational methods like
            # `project` are cleaner when we use a sentinel value.
            name
            for name, token in name_token_pairs
            if token is None
        ]
        self.has_missing_values = len(self.missing_names) > 0

    def project(self, key_space):
        return CaseKey(
            [
                (name, token)
                for name, token in self._name_token_pairs
                if name in key_space
            ]
        )

    def drop(self, key_space):
        return CaseKey(
            [
                (name, token)
                for name, token in self._name_token_pairs
                if name not in key_space
            ]
        )

    def merge(self, other):
        tokens_by_name = {name: token for name, token in self._name_token_pairs}

        for name, token in other._name_token_pairs:
            if name in tokens_by_name:
                assert token == tokens_by_name[name]
            else:
                tokens_by_name[name] = token

        return CaseKey([(name, token) for name, token in tokens_by_name.items()])

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


# The CodeVersion and CodeFingerprint classs are used (indirectly) by
# persistence.ArtifactMetadataRecord and can be serialized to YAML and stored in the
# persistent cache. That means if we add new fields to them, we also need to update
# persistence.CACHE_SCHEMA_VERSION.
# TODO Should we just move these classes to persistence.py as well?
@attr.s(frozen=True)
class CodeVersion:
    """
    Contains the user-designated version of a piece of code, consisting of a
    major and a minor version string, and a boolean that indicates whether it
    includes the bytecode.  The convention is that changing the major version
    indicates a functional change, while changing the minor version indicates a
    nonfunctional change.  If ``includes_bytecode`` is True, then the major version
    is understood to implicitly include the bytecode of the code as well.
    """

    major: str = attr.ib(converter=str_from_version_value)
    minor: str = attr.ib(converter=str_from_version_value)
    includes_bytecode: bool = attr.ib(converter=attr.converters.default_if_none(True))


@attr.s(frozen=True)
class CodeVersioningPolicy:
    """
    Contains the version of the user entity function with any additional settings
    related to the version. For now, we only have one setting that affects the
    analysis-time behavior of the version.
    """

    version: CodeVersion = attr.ib()
    suppress_bytecode_warnings: bool = attr.ib(
        converter=attr.converters.default_if_none(True)
    )


@attr.s(frozen=True)
class CodeFingerprint:
    """
    A collection of characteristics attempting to uniquely identify a function.

    Attributes
    ----------
    version: CodeVersion
        A version identifier provided by the user.
    bytecode_hash: str
        A hash of the function's Python bytecode.
    orig_flow_name: str
        The name of the flow in which this function was originally defined.
    is_identity: bool
        If True, indicates that this function is equivalent to the identity function:
        it takes one argument and returns it unchanged.
    """

    version: CodeVersion = attr.ib()
    bytecode_hash: str = attr.ib()
    orig_flow_name: str = attr.ib()
    is_identity: bool = attr.ib(default=False)


@attr.s(frozen=True)
class VersioningPolicy:
    """
    Encodes the versioning rules to use when computing entity values.
    """

    check_for_bytecode_errors = attr.ib()
    treat_bytecode_as_functional = attr.ib()
    ignore_bytecode_exceptions = attr.ib()


@attr.s(frozen=True)
class FunctionAttributes:
    """
    Describes properties of a Python function.
    """

    code_fingerprint = attr.ib()
    code_versioning_policy = attr.ib()
    changes_per_run = attr.ib()
    aip_task_config = attr.ib()
