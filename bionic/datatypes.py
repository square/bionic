'''
Contains various data structures used by Bionic's infrastructure.
'''
from __future__ import absolute_import

from builtins import object
from collections import namedtuple

import six

from .util import ImmutableSequence, ImmutableMapping


# TODO Consider using the attr library here?
class TaskKey(namedtuple('TaskKey', 'entity_name case_key')):
    '''
    A unique identifier for a Task.
    '''
    def __new__(cls, entity_name, case_key):
        return super(TaskKey, cls).__new__(cls, entity_name, case_key)

    def __repr__(self):
        return 'TaskKey(%r, %r)' % (self.entity_name, self.case_key)

    def __str__(self):
        return '%s(%s)' % (
           self.entity_name,
           ', '.join(
               '%s=%s' % (name, value)
               for name, value in self.case_key.items())
        )


class Task(object):
    '''
    A unit of work.  Can have dependencies, which are referred to via their
    TaskKeys.
    '''
    def __init__(self, keys, dep_keys, compute_func, is_simple_lookup=False):
        self.keys = tuple(keys)
        self.dep_keys = tuple(dep_keys)
        self.compute = compute_func
        self.is_simple_lookup = is_simple_lookup

    def key_for_entity_name(self, name):
        matching_keys = [
            task_key
            for task_key in self.keys
            if task_key.entity_name == name
        ]
        key, = matching_keys
        return key

    def __repr__(self):
        return 'Task(%r, %r)' % (self.keys, self.dep_keys)


class Query(object):
    '''
    Represents a request for a specific entity value.
    '''
    def __init__(
            self, task_key, protocol, provenance):
        self.task_key = task_key
        self.entity_name = task_key.entity_name
        self.case_key = task_key.case_key
        self.protocol = protocol
        self.provenance = provenance

    def to_result(self, value):
        return Result(query=self, value=value)

    def __repr__(self):
        return 'Query(%s, %r)' % (self.task_key, self.provenance)


class Result(object):
    '''
    Represents one value for one entity.
    '''
    def __init__(
            self, query, value, file_path=None):
        self.query = query
        self.value = value
        self.file_path = file_path

    def __repr__(self):
        return 'Result(%r, %r)' % (self.query, self.value)


class CaseKeySpace(ImmutableSequence):
    '''
    A set of CaseKey names (without values) -- represents a space of possible
    CaseKeys.
    '''
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
        return 'CaseKeySpace(%s)' % ', '.join(repr(name) for name in self)


class CaseKey(ImmutableMapping):
    '''
    A collection of name-value pairs that uniquely identifies a case.
    '''
    def __init__(self, name_value_token_triples):
        values_by_name = {
            name: value
            for name, value, token in name_value_token_triples
        }
        tokens_by_name = {
            name: token
            for name, value, token in name_value_token_triples
        }

        super(CaseKey, self).__init__(tokens_by_name)
        self._nvt_triples = name_value_token_triples
        self.values = values_by_name
        self.tokens = tokens_by_name
        self.space = CaseKeySpace(list(values_by_name.keys()))

    def project(self, key_space):
        return CaseKey([
            (name, value, token)
            for name, value, token in self._nvt_triples
            if name in key_space
        ])

    def drop(self, key_space):
        return CaseKey([
            (name, value, token)
            for name, value, token in self._nvt_triples
            if name not in key_space
        ])

    def merge(self, other):
        vt_pairs_by_name = {
            name: (value, token)
            for name, value, token in self._nvt_triples
        }

        for name, value, token in other._nvt_triples:
            if name in vt_pairs_by_name:
                assert token == vt_pairs_by_name[name][1]
            else:
                vt_pairs_by_name[name] = value, token

        return CaseKey([
            (name, value, token)
            for name, (value, token) in vt_pairs_by_name.items()
        ])

    def __repr__(self):
        return 'CaseKey(%s)' % ', '.join(
            name + '=' + token
            for name, token in self.items())


class ResultGroup(ImmutableSequence):
    '''
    Represents a collection of Results, distinguished by their CaseKeys.  Each
    CaseKey should have the same set of names.
    '''
    def __init__(self, results, key_space):
        super(ResultGroup, self).__init__(results)

        self.key_space = key_space

    def __repr__(self):
        return 'ResultGroup(%r)' % list(self)


class CodeVersion(object):
    '''
    Contains the user-designated version of a piece of code, consisting of a
    major and a minor version string.  The convention is that changing the
    major version indicates a functional change, while changing the minor
    version indicates a nonfunctional change.
    '''

    def __init__(self, major, minor):
        self.major = str_from_version_value(major)
        self.minor = str_from_version_value(minor)

    def __repr__(self):
        return 'CodeVersion(%r, %r)' % (self.major, self.minor)


def str_from_version_value(value):
    if value is None:
        return '0'
    elif isinstance(value, int):
        return str(value)
    elif isinstance(value, six.text_type):
        return value
    else:
        raise ValueError(
            "Version values must be str, int, or None: got %r" % value)


# Describes the code of a function.
CodeDescriptor = namedtuple(
    'CodeDescriptor', 'version bytecode_hash orig_flow_name')


# Encodes the versioning rules to use when computing entity values.
VersioningPolicy = namedtuple(
    'VersioningPolicy',
    'check_for_bytecode_errors treat_bytecode_as_functional')
