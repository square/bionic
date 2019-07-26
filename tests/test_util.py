import pytest

from helpers import equal_when_sorted

import bionic.util as util


def test_merge_dicts():
    from bionic.util import merge_dicts

    a1 = {'a': 1}
    a2 = {'a': 2}
    b3 = {'b': 3}
    a1_b3 = {'a': 1, 'b': 3}
    b4_c5 = {'b': 4, 'c': 5}
    a1_b4_c5 = {'a': 1, 'b': 4, 'c': 5}

    assert merge_dicts([]) == {}
    assert merge_dicts([{}]) == {}
    assert merge_dicts([{}, a1]) == a1
    assert merge_dicts([a1, {}]) == a1
    assert merge_dicts([a1, b3]) == a1_b3
    assert merge_dicts([b3, a1]) == a1_b3
    with pytest.raises(ValueError):
        merge_dicts([a1, a2])
    assert merge_dicts([a1, a2], allow_overlap=True) == a2
    assert merge_dicts([a1_b3, b4_c5], allow_overlap=True) == a1_b4_c5


def test_merge_dfs():
    import pandas as pd
    from helpers import df_from_csv_str, assert_frames_equal_when_sorted

    from bionic.util import merge_dfs

    x_df = df_from_csv_str('''
    x
    1
    2
    ''')
    y_df = df_from_csv_str('''
    y
    3
    4
    ''')
    z_df = df_from_csv_str('''
    z
    5
    6
    ''')
    xy_df = df_from_csv_str('''
    x,y
    1,3
    1,4
    2,3
    2,4
    ''')
    yz_df = df_from_csv_str('''
    y,z
    3,5
    3,6
    4,5
    4,6
    ''')
    xyz_df = df_from_csv_str('''
    x,y,z
    1,3,5
    1,3,6
    1,4,5
    1,4,6
    2,3,5
    2,3,6
    2,4,5
    2,4,6
    ''')

    assert_frames_equal_when_sorted(merge_dfs([x_df]), x_df)
    assert_frames_equal_when_sorted(merge_dfs([x_df, x_df]), x_df)
    assert_frames_equal_when_sorted(merge_dfs([x_df, y_df]), xy_df)
    assert_frames_equal_when_sorted(merge_dfs([x_df, y_df, z_df]), xyz_df)
    assert_frames_equal_when_sorted(merge_dfs([xy_df, z_df]), xyz_df)
    assert_frames_equal_when_sorted(merge_dfs([xy_df, yz_df]), xyz_df)
    assert_frames_equal_when_sorted(merge_dfs([xy_df, yz_df]), xyz_df)
    assert_frames_equal_when_sorted(merge_dfs([xyz_df, yz_df]), xyz_df)

    ab_df = df_from_csv_str('''
    a,b
    1,one
    2,two
    3,three
    ''')
    bc_df = df_from_csv_str('''
    b,c
    one,True
    one,False
    two,True
    ''')
    abc_df = df_from_csv_str('''
    a,b,c
    1,one,True
    1,one,False
    2,two,True
    ''')
    bcd_df = df_from_csv_str('''
    b,c,d
    one,True,red
    two,True,red
    two,True,blue
    ''')
    abcd_df = df_from_csv_str('''
    a,b,c,d
    1,one,True,red
    2,two,True,red
    2,two,True,blue
    ''')

    assert_frames_equal_when_sorted(merge_dfs([ab_df, bc_df]), abc_df)
    assert_frames_equal_when_sorted(merge_dfs([abc_df, bcd_df]), abcd_df)

    with pytest.raises(ValueError):
        merge_dfs([])
    assert_frames_equal_when_sorted(
        merge_dfs([], allow_empty=True),
        pd.DataFrame([[]]),
    )


def test_merge_dfs_using_inner_outer():
    from helpers import df_from_csv_str, assert_frames_equal_when_sorted

    from bionic.util import merge_dfs

    xy_df = df_from_csv_str('''
    x,y
    1,3
    2,3
    ''')
    yz_df = df_from_csv_str('''
    y,z
    3,5
    3,6
    4,7
    4,8
    ''')
    xyz_inner_df = df_from_csv_str('''
    x,y,z
    1,3,5
    1,3,6
    2,3,5
    2,3,6
    ''')
    xyz_outer_df = df_from_csv_str('''
    x,y,z
    1,3,5
    1,3,6
    2,3,5
    2,3,6
    ,4,7
    ,4,8
    ''')

    assert_frames_equal_when_sorted(
        merge_dfs([xy_df, yz_df], how='inner'), xyz_inner_df)
    assert_frames_equal_when_sorted(
        merge_dfs([xy_df, yz_df], how='outer'), xyz_outer_df)


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
