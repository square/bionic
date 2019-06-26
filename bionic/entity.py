'''
Contains various data structures used by Bionic's infrastructure.
'''
from __future__ import absolute_import

from builtins import object
from collections import namedtuple
import yaml

from .util import (
    ImmutableSequence, ImmutableMapping, check_exactly_one_present, hash_to_hex
)


# TODO Consider using the attr library here?
class TaskKey(namedtuple('TaskKey', 'resource_name case_key')):
    '''
    A unique identifier for a Task.
    '''
    def __new__(cls, resource_name, case_key):
        return super(TaskKey, cls).__new__(cls, resource_name, case_key)

    def __repr__(self):
        return 'TaskKey(%r, %r)' % (self.resource_name, self.case_key)


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

    def key_for_resource_name(self, name):
        matching_keys = [
            task_key
            for task_key in self.keys
            if task_key.resource_name == name
        ]
        key, = matching_keys
        return key

    def __repr__(self):
        return 'Task(%r, %r)' % (self.keys, self.dep_keys)


class Query(object):
    '''
    Represents a request for a specific resource value.
    '''
    def __init__(self, name, protocol, case_key, provenance):
        self.name = name
        self.protocol = protocol
        self.case_key = case_key
        self.provenance = provenance

    def to_result(self, value):
        return Result(query=self, value=value)

    def __repr__(self):
        return 'Query(%r, %r, %r, %r)' % (
            self.name, self.protocol, self.case_key, self.provenance)


class Result(object):
    '''
    Represents one value for one resource.
    '''
    def __init__(self, query, value, cache_path=None):
        self.query = query
        self.value = value
        self.cache_path = cache_path

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


class Provenance(object):
    '''
    A compact, unique hash of a (possibly yet-to-be-computed) value.
    Can be used to determine whether a value needs to be recomputed.
    '''
    @classmethod
    def from_computation(cls, code_id, case_key, dep_provenances_by_name):
        return cls(body_dict=dict(
            code_id=code_id,
            case_key=dict(case_key),
            deps={
                name: provenance.hashed_value
                for name, provenance in dep_provenances_by_name.items()
            },
        ))

    @classmethod
    def from_yaml(cls, yaml_str):
        return cls(yaml_str=yaml_str)

    def __init__(self, body_dict=None, yaml_str=None):
        check_exactly_one_present(body_dict=body_dict, yaml_str=yaml_str)

        if body_dict is not None:
            self._body_dict = body_dict
            self._yaml_str = yaml.dump(body_dict, default_flow_style=False, encoding=None)
        else:
            self._body_dict = yaml.full_load(yaml_str)
            self._yaml_str = yaml_str

        self.hashed_value = hash_to_hex(self._yaml_str.encode('utf-8'))

    def to_yaml(self):
        return self._yaml_str

    def __repr__(self):
        return 'Provenance(%s...)' % self.hashed_value[:8]
