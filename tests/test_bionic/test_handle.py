import pytest

from random import Random

from helpers import count_calls

from bionic.handle import ImmutableHandle, MutableHandle


@pytest.fixture(scope='function')
def Thing():
    class Thing(object):
        def __init__(self, value):
            self.value = value

        def increment_value(self):
            self.value += 1

        def self_destruct(self):
            self.value = None
            assert False

        @count_calls
        def copy(self):
            return Thing(self.value)

        def __eq__(self, other):
            if not isinstance(other, Thing):
                return False
            return self.value == other.value

        def __ne__(self, other):
            return not (self == other)

    return Thing


def test_mutable_immutable(Thing):
    thing = Thing(1)

    ihandle = ImmutableHandle(thing)

    assert ihandle.get().value == 1
    assert Thing.copy.times_called() == 0

    mhandle = ihandle.as_mutable()

    assert ihandle.get().value == 1
    assert Thing.copy.times_called() == 0

    mhandle.get().increment_value()

    assert ihandle.get().value == 1
    assert mhandle.get().value == 2
    assert Thing.copy.times_called() == 1

    ihandle2 = mhandle.as_immutable()

    assert ihandle.get().value == 1
    assert ihandle2.get().value == 2
    assert Thing.copy.times_called() == 0

    mhandle.get().increment_value()

    assert ihandle.get().value == 1
    assert ihandle2.get().value == 2
    assert mhandle.get().value == 3
    assert Thing.copy.times_called() == 1


def test_lazy_updating(Thing):
    thing = Thing(1)
    ihandle = ImmutableHandle(thing)

    ihandle2 = ihandle.updating(Thing.increment_value, eager=False)

    assert Thing.copy.times_called() == 0
    assert ihandle.get().value == 1
    assert ihandle2.get().value == 2
    assert ihandle.get().value == 1
    assert Thing.copy.times_called() == 1


def test_eager_update_fail(Thing):
    thing = Thing(1)
    ihandle = ImmutableHandle(thing)

    with pytest.raises(AssertionError):
        ihandle.updating(Thing.self_destruct, eager=True)


def test_lazy_update_fail(Thing):
    thing = Thing(1)
    ihandle = ImmutableHandle(thing)

    ihandle.updating(Thing.self_destruct, eager=False)


def test_multiple_lazy_updates(Thing):
    thing = Thing(1)
    ihandle = ImmutableHandle(thing)

    ihandle2 = ihandle.updating(Thing.increment_value, eager=False)
    ihandle3 = ihandle2.updating(Thing.increment_value, eager=False)

    assert Thing.copy.times_called() == 0
    assert ihandle3.get().value == 3
    assert Thing.copy.times_called() == 1
    assert ihandle2.get().value == 2
    assert Thing.copy.times_called() == 1


def test_multiple_eager_updates(Thing):
    thing = Thing(1)
    ihandle = ImmutableHandle(thing)

    ihandle2 = ihandle.updating(Thing.increment_value, eager=True)
    ihandle3 = ihandle2.updating(Thing.increment_value, eager=True)

    assert Thing.copy.times_called() == 1
    assert ihandle3.get().value == 3
    assert Thing.copy.times_called() == 0
    assert ihandle2.get().value == 2
    assert Thing.copy.times_called() == 1


def test_handle_invariants_by_fuzzing(Thing):
    class HandleFuzzer(object):
        def __init__(self):
            self._random = Random(0)
            thing = Thing(1)
            self._ihandles = [ImmutableHandle(thing)]
            self._mhandles = [
                MutableHandle(immutable_value=thing),
                MutableHandle(mutable_value=thing.copy()),
            ]
            self._saved_ithings = []
            self._saved_mthings = []

        def fuzz(self):
            for _ in xrange(1000):
                self._do_random_action()

            self._save_things()

            for _ in xrange(1000):
                self._do_random_action()

            self._check_saved_things()
            self._save_things()

            for _ in xrange(5000):
                self._do_random_action()

            self._check_saved_things()

        def _save_things(self):
            self._saved_ithings = [handle.get() for handle in self._ihandles]
            self._saved_mthings = [handle.get() for handle in self._mhandles]
            for thing in self._saved_mthings:
                if self._random.randint(0, 1):
                    thing.increment_value()

        def _check_saved_things(self):
            for thing, handle in zip(self._saved_ithings, self._ihandles):
                assert thing == handle.get()
            for thing, handle in zip(self._saved_mthings, self._mhandles):
                assert thing == handle.get()

        def _do_random_action(self):
            self._random.choice([
                self._create_mutable,
                self._create_immutable,
                self._update_immutable_eager,
                self._update_immutable_lazy,
                self._get_immutable,
            ])()

        def _create_mutable(self):
            self._mhandles.append(self._random_ihandle().as_mutable())

        def _create_immutable(self):
            self._ihandles.append(self._random_mhandle().as_immutable())

        def _update_immutable_eager(self):
            self._ihandles.append(self._random_ihandle().updating(
                Thing.increment_value, eager=True))

        def _update_immutable_lazy(self):
            self._ihandles.append(self._random_ihandle().updating(
                Thing.increment_value, eager=False))

        def _get_immutable(self):
            self._random_ihandle().get()

        def _random_ihandle(self):
            return self._random.choice(self._ihandles)

        def _random_mhandle(self):
            return self._random.choice(self._mhandles)

    HandleFuzzer().fuzz()
