import pytest

from .helpers import equal_when_sorted

import bionic.util as util


def test_group_pairs():
    from bionic.util import group_pairs

    assert group_pairs([]) == []
    assert group_pairs([1, 2]) == [(1, 2)]
    assert group_pairs([1, 2, 3, 4, 5, 6]) == [(1, 2), (3, 4), (5, 6)]

    with pytest.raises(ValueError):
        group_pairs([1])
    with pytest.raises(ValueError):
        group_pairs([1, 2, 3])


def test_immutable_sequence():
    class Seq(util.ImmutableSequence):
        def __init__(self, items):
            super(Seq, self).__init__(items)

    seq = Seq([1, 2, 3])

    assert seq[0] == 1
    assert seq[2] == 3
    assert seq[-2] == 2

    assert list(seq) == [1, 2, 3]
    assert len(seq) == 3

    assert 1 in seq
    assert 4 not in seq

    assert {seq: 7}[seq] == 7

    assert seq == Seq([1, 2, 3])
    assert seq != Seq([1, 3, 2])
    assert seq != [1, 2, 3]

    assert seq < Seq([1, 3, 2])
    assert seq <= Seq([1, 3, 2])
    assert Seq([1, 3, 2]) > seq
    assert Seq([1, 3, 2]) >= seq


def test_immutable_mapping():
    class Mapping(util.ImmutableMapping):
        def __init__(self, values_by_key):
            super(Mapping, self).__init__(values_by_key)

    mapping = Mapping({'a': 1, 'b': 2})

    assert mapping['a'] == 1
    assert mapping['b'] == 2
    with pytest.raises(KeyError):
        mapping['c']

    assert mapping.get('a') == 1
    assert mapping.get('c') is None

    assert {mapping: 7}[mapping] == 7

    assert equal_when_sorted(list(mapping), ['a', 'b'])
    assert dict(mapping) == {'a': 1, 'b': 2}
    assert equal_when_sorted(list(mapping.keys()), ['a', 'b'])
    assert equal_when_sorted(list(mapping.values()), [1, 2])
    assert equal_when_sorted(list(mapping.items()), [('a', 1), ('b', 2)])
    assert equal_when_sorted(list(mapping.keys()), ['a', 'b'])
    assert equal_when_sorted(list(mapping.values()), [1, 2])
    assert equal_when_sorted(list(mapping.items()), [('a', 1), ('b', 2)])

    assert mapping == Mapping({'a': 1, 'b': 2})
    assert mapping != {'a': 1, 'b': 2}
    assert mapping != Mapping({'b': 1, 'a': 2})
    assert mapping < Mapping({'b': 1, 'a': 2})
    assert mapping <= Mapping({'b': 1, 'a': 2})
    assert Mapping({'b': 1, 'a': 2}) > mapping
    assert Mapping({'b': 1, 'a': 2}) >= mapping


def test_oneline():
    from bionic.util import oneline
    assert oneline('one two') == 'one two'
    assert oneline(' one two ') == 'one two'
    assert oneline('\none\ntwo') == 'one two'
    assert oneline('''
       one
       two   three''') == 'one two   three'
    assert oneline('''
       one
       two

       three
       ''') == 'one two three'


# These functions are not in util.py but it's convenient to test them here too.
def test_longest_regex_prefix():
    from .helpers import longest_regex_prefix_match

    def longest_prefix(regex, string):
        return longest_regex_prefix_match(regex, string).re.pattern

    assert longest_prefix('test', 'test') == 'test'
    assert longest_prefix('test', 'te') == 'te'
    assert longest_prefix('test', 'text') == 'te'
    assert longest_prefix('test', 'testtest') == 'test'
    assert longest_prefix('zest', 'test') == ''
    assert longest_prefix('(test)', 'test') == '(test)'
    assert longest_prefix('(test)', 'text') == ''
    assert longest_prefix('(test)test', 'testtest') == '(test)test'
    assert longest_prefix('(test)test', 'testtext') == '(test)te'
    assert longest_prefix('x\n\n\nx', 'x\n\n\nx') == 'x\n\n\nx'
    assert longest_prefix('x\n\n\nx', 'x\n\n\ny') == 'x\n\n\n'
    assert longest_prefix('x\n\n\nx', 'x\n\ny') == 'x\n\n'
    assert longest_prefix('x\n\n\nx', 'y\n\n\nx') == ''
    assert longest_prefix('test.*test', 'testtest') == 'test.*test'
    assert longest_prefix('test.*test', 'testxxtest') == 'test.*test'
    assert longest_prefix('test.*test', 'testxxzest') == 'test.*t'
    assert longest_prefix('test.*test', 'testxxz') == 'test.*'
    assert longest_prefix('test.*test', 'texttest') == 'te'


def test_assert_re_matches():
    from .helpers import assert_re_matches

    def assert_re_nomatch(regex, string):
        with pytest.raises(AssertionError):
            assert_re_matches(regex, string)

    assert_re_matches('test', 'test')
    assert_re_matches('test', 'testxxx')
    assert_re_nomatch('test', 'tesd')

    assert_re_matches('test$', 'test')
    assert_re_nomatch('test$', 'testx')

    assert_re_matches('.*test', 'test')
    assert_re_matches('.*test', 'xxtest')
    assert_re_matches('.*test', 'testxx')
    assert_re_nomatch('.*test', 'tesd')

    assert_re_matches('(test)', 'test')
    assert_re_matches('(test)', 'testx')
    assert_re_nomatch('(test)', 'tesd')

    assert_re_matches('test.*test', 'testtest')
    assert_re_matches('test.*test', 'testxxtest')
    assert_re_nomatch('test.*test', 'test\ntest')

    assert_re_matches('(?s)test.*test', 'test\ntest')
