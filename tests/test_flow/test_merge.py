import pytest

import bionic as bn
from bionic.exception import (
        UndefinedEntityError, AlreadyDefinedEntityError,
        IncompatibleEntityError)

ALL_KEEP_VALUES = ['error', 'old', 'new', 'combine']


class MergeTester(object):
    """
    A utility class for testing many variations of flow merging.  This class
    is configured by creating several flows, associated them with names using
    the ``add`` method, and then calling ``test_grid`` with a string that
    visually represents flow combinations and the expected outcome from
    merging them.
    """

    def __init__(self):
        self._flows_by_key = {}

    def add(self, key, flow):
        """Associates a string ``key`` with a ``flow``."""
        self._flows_by_key[key] = flow

    def test_grid(self, keep, grid):
        """
        Parses ``grid`` as a visual representation of a grid of test
        configurations.  Each row and column corresponds to a predefined flow;
        each cell specifies an expected outcome from merging the row and
        column flows and then accessing the entity named ``"x"``.

        For example, when ``grid`` is

            OLD:NEW  NameA  NameB
              Name1      .      ^
              Name2      <      !

        that means we should run four test cases:

        - merging flow ``Name1`` with flow ``NameA``, expecting the merged
          flow to have no entity named ``x``
          be defined")
        - merging flow ``Name1`` with flow ``NameB``, expecting entity ``x``
          to have the same value as in ``NameB``
        - merging flow ``Name2`` with flow ``NameA``, expecting entity ``x``
          to have the same value as in ``Name2``
        - merging flow ``Name2`` with flow ``NameA``, expecting the merge to
          throw an ``AlreadyDefinedEntityError``

        Each merge is performed with the passed ``keep`` argument.
        """

        header_and_rows = [
            line.strip().split() for line in grid.strip().splitlines()
        ]

        header = header_and_rows[0]
        rows = header_and_rows[1:]

        new_flow_keys = header[1:]
        for row in rows:
            old_flow_key = row[0]
            outcome_keys = row[1:]

            for new_flow_key, outcome_key in zip(new_flow_keys, outcome_keys):
                self._run_test_cell(
                    old_flow_key, new_flow_key, outcome_key, keep)

    def _run_test_cell(self, old_flow_key, new_flow_key, outcome_key, keep):
        old_flow = self._flow_for_key(old_flow_key)
        new_flow = self._flow_for_key(new_flow_key)

        if outcome_key == '.':
            # Entity should not be defined.
            merged_flow = old_flow.merging(new_flow, keep)
            with pytest.raises(UndefinedEntityError):
                merged_flow.get('x')

        elif outcome_key == '-':
            # Entity should be defined, but have no value.
            merged_flow = old_flow.merging(new_flow, keep)
            assert merged_flow.get('x', set) == set()

        elif outcome_key == '<':
            # Entity should have the old values.
            merged_flow = old_flow.merging(new_flow, keep)
            assert merged_flow.get('x', set) == old_flow.get('x', set)

        elif outcome_key == '^':
            # Entity should have the new values.
            merged_flow = old_flow.merging(new_flow, keep)
            assert merged_flow.get('x', set) == new_flow.get('x', set)

        elif outcome_key == '!':
            # Merge should fail.
            with pytest.raises(AlreadyDefinedEntityError):
                old_flow.merging(new_flow, keep)

        elif outcome_key == 'X':
            # Merge should fail.
            with pytest.raises(IncompatibleEntityError):
                old_flow.merging(new_flow, keep)

        else:
            raise AssertionError(f"Unexpected outcome key: {outcome_key}")

    def _flow_for_key(self, key):
        return self._flows_by_key[key]


# TODO Once we have a proper `Flow.deriving` method, we can remove these clunky
# `Flow.to_builder`s.
@pytest.fixture(scope='function')
def merge_tester(builder):
    f = builder.build()

    tester = MergeTester()

    tester.add('Missing', f)
    tester.add('Declared', f.declaring('x'))
    tester.add('FixedSingle', f.assigning('x', 2))
    tester.add('FixedMulti', f.assigning('x', values=[3, 4]))

    fb = f.to_builder()
    fb.assign('root_x', 3)

    @fb
    def x(root_x):
        return root_x ** 2

    tester.add('DerivedSingle', fb.build())

    fb = f.to_builder()
    fb.assign('x_y', (5, 6))

    @fb  # noqa: F811
    @bn.outputs('x', 'y')
    def x(x_y):
        return x_y

    tester.add('DerivedJoint', fb.build())

    tester.add(
        'FixedJoint',
        f.declaring('x').declaring('y').adding_case('x', 7, 'y', 8))

    # This new flow will use the default cache directory, which is bad, because
    # it could pick up data from previous test runs.  Unfortunately it's tricky
    # to set another cache directory on this flow, because it will cause a
    # conflict when merging the two flows (even if the two cache directories
    # are the same).  To work around this, we disable persistence for all
    # derived entities in this flow, so the cache shouldn't be used at all.
    # Longer-term, we may want a smarter way of merging that either recognizes
    # when two values are the same, or handles "infrastructure" entities like
    # this differently.  Or a way to just run a flow without caching.
    f = bn.FlowBuilder('new_flow').build()
    tester.add('M', f)
    tester.add('D', f.declaring('x'))
    tester.add('FS', f.assigning('x', 12))
    tester.add('FM', f.assigning('x', values=[13, 14]))

    fb = f.to_builder()
    fb.assign('root_x', 3)

    @fb  # noqa: F811
    @bn.persist(False)
    def x(root_x):
        return root_x ** 2

    tester.add('DS', fb.build())

    fb = f.to_builder()
    fb.assign('x_y', (5, 6))

    @fb  # noqa: F811
    @bn.outputs('x', 'y')
    @bn.persist(False)
    def x(x_y):
        return x_y

    tester.add('DJ', fb.build())

    tester.add(
        'FJ',
        f.declaring('x').declaring('y').adding_case('x', 17, 'y', 18))

    return tester


def test_keep_error(merge_tester):
    merge_tester.test_grid(
        keep='error',
        grid='''
              OLD:NEW  M  D FS FM DS DJ FJ
              Missing  .  -  ^  ^  ^  ^  ^
             Declared  -  -  ^  ^  ^  ^  ^
          FixedSingle  <  <  !  !  !  !  !
           FixedMulti  <  <  !  !  !  !  !
        DerivedSingle  <  <  !  !  !  !  !
         DerivedJoint  <  <  !  !  !  !  !
           FixedJoint  <  <  !  !  !  !  !
        ''')


def test_keep_old(merge_tester):
    merge_tester.test_grid(
        keep='old',
        grid='''
              OLD:NEW  M  D FS FM DS DJ FJ
              Missing  .  -  ^  ^  ^  ^  ^
             Declared  -  -  ^  ^  ^  ^  ^
          FixedSingle  <  <  <  <  <  X  X
           FixedMulti  <  <  <  <  <  X  X
        DerivedSingle  <  <  <  <  <  X  X
         DerivedJoint  <  <  <  <  <  <  <
           FixedJoint  <  <  <  <  <  <  <
        ''')


def test_keep_new(merge_tester):
    merge_tester.test_grid(
        keep='new',
        grid='''
              OLD:NEW  M  D FS FM DS DJ FJ
              Missing  .  -  ^  ^  ^  ^  ^
             Declared  -  -  ^  ^  ^  ^  ^
          FixedSingle  <  <  ^  ^  ^  ^  ^
           FixedMulti  <  <  ^  ^  ^  ^  ^
        DerivedSingle  <  <  ^  ^  ^  ^  ^
         DerivedJoint  <  <  X  X  X  ^  ^
           FixedJoint  <  <  X  X  X  ^  ^
        ''')


def test_old_name_is_kept():
    old_flow = bn.FlowBuilder('old').build()
    new_flow = bn.FlowBuilder('new').build()

    for keep in ALL_KEEP_VALUES:
        assert old_flow.merging(new_flow, keep=keep).name == 'old'


def test_old_name_is_kept_even_on_explicit_rename():
    old_flow = bn.FlowBuilder('old').build()
    new_flow = bn.FlowBuilder('new').build().setting('core__flow_name', 'NEW')

    for keep in ALL_KEEP_VALUES:
        assert old_flow.merging(new_flow, keep=keep).name == 'old'


CACHE_DIR_ENT = 'core__persistent_cache__global_dir'


def test_cache_dir_not_set():
    old_flow = bn.FlowBuilder('old').build()
    new_flow = bn.FlowBuilder('new').build()

    for keep in ALL_KEEP_VALUES:
        assert old_flow.merging(new_flow, keep=keep)\
            .get(CACHE_DIR_ENT) == 'bndata'


def test_cache_dir_already_set():
    old_flow = bn.FlowBuilder('old').build()\
        .setting(CACHE_DIR_ENT, 'old_dir')
    new_flow = bn.FlowBuilder('new').build()

    for keep in ALL_KEEP_VALUES:
        assert old_flow.merging(new_flow, keep=keep)\
            .get(CACHE_DIR_ENT) == 'old_dir'


def test_cache_dir_set_on_incoming():
    old_flow = bn.FlowBuilder('old').build()
    new_flow = bn.FlowBuilder('new').build()\
        .setting(CACHE_DIR_ENT, 'new_dir')

    for keep in ALL_KEEP_VALUES:
        assert old_flow.merging(new_flow, keep=keep)\
            .get(CACHE_DIR_ENT) == 'new_dir'


def test_cache_dir_conflicts():
    old_flow = bn.FlowBuilder('old').build()\
        .setting(CACHE_DIR_ENT, 'old_dir')
    new_flow = bn.FlowBuilder('new').build()\
        .setting(CACHE_DIR_ENT, 'new_dir')

    with pytest.raises(AlreadyDefinedEntityError):
        old_flow.merging(new_flow, keep='error')
    assert old_flow.merging(new_flow, keep='old').get(CACHE_DIR_ENT) ==\
        'old_dir'
    assert old_flow.merging(new_flow, keep='new').get(CACHE_DIR_ENT) ==\
        'new_dir'


def test_protocols_conflict(builder):
    builder.declare('x', protocol=bn.protocol.type(int))

    incoming_builder = bn.FlowBuilder('new_name')
    incoming_builder.declare('x', protocol=bn.protocol.type(str))

    with pytest.raises(AlreadyDefinedEntityError):
        builder.merge(incoming_builder.build())

    builder.merge(incoming_builder.build(), keep='old')
    builder.set('x', 1)
    with pytest.raises(AssertionError):
        builder.set('x', 'blue')

    builder.merge(incoming_builder.build(), keep='new')
    builder.set('x', 'blue')
    with pytest.raises(AssertionError):
        builder.set('x', 1)


def test_protocol_is_overwritten(builder):
    builder.declare('x', protocol=bn.protocol.type(int))

    incoming_builder = bn.FlowBuilder('new_name')
    incoming_builder.assign('x', 'blue', protocol=bn.protocol.type(str))

    builder.merge(incoming_builder.build(), keep='new')

    with pytest.raises(AssertionError):
        builder.set('x', 3)

    assert builder.build().get('x') == 'blue'
