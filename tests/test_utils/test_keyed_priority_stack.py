import pytest

from random import Random

from bionic.utils.keyed_priority_stack import KeyedPriorityStack


def test_simple_push():
    kps = KeyedPriorityStack()

    assert len(kps) == 0

    kps.push("ONE", "1", 1)
    kps.push("TWO_A", "2a", 2)
    kps.push("THREE", "3", 3)
    kps.push("TWO_B", "2b", 2)

    assert len(kps) == 4

    assert kps.pop() == "3"
    assert kps.pop() == "2b"
    assert kps.pop() == "2a"
    assert kps.pop() == "1"

    assert len(kps) == 0


def test_pop_by_key():
    kps = KeyedPriorityStack()

    with pytest.raises(KeyError):
        kps.pop("ONE")

    kps.push("ONE", "1", 1)
    kps.push("TWO_A", "2a", 2)
    kps.push("THREE", "3", 3)
    kps.push("TWO_B", "2b", 2)

    with pytest.raises(KeyError):
        kps.pop("1")

    assert kps.pop("TWO_B") == "2b"
    assert kps.pop() == "3"
    assert kps.pop("TWO_A") == "2a"
    assert kps.pop() == "1"

    with pytest.raises(KeyError):
        kps.pop("THREE")


def test_incomparable_unhashable_values():
    class Wrapper:
        def __init__(self, value):
            self.value = value

        def __eq__(self, other):
            raise NotImplementedError("!")

        def __hash__(self, other):
            raise NotImplementedError("!")

    kps = KeyedPriorityStack()

    kps.push("ONE", Wrapper("1"), 1)
    kps.push("TWO_A", Wrapper("2a"), 2)
    kps.push("THREE", Wrapper("3"), 3)
    kps.push("TWO_B", Wrapper("2b"), 2)

    assert kps.pop().value == "3"
    assert kps.pop().value == "2b"
    assert kps.pop().value == "2a"
    assert kps.pop().value == "1"


def test_random():
    """
    Tests our data structure by applying a series of random operations and comparing
    the results to an oracle (SimpleKeyedPriorityStack).
    """

    random = Random(0)
    MAX_VALUE = 1000000

    test_kps = KeyedPriorityStack()
    ctrl_kps = SimpleKeyedPriorityStack()

    def do_push():
        value = random.randrange(MAX_VALUE)
        priority = random.randrange(MAX_VALUE)
        key = random.randrange(MAX_VALUE)

        test_kps.push(key, value, priority)
        ctrl_kps.push(key, value, priority)

    def do_and_check_pop():
        if len(test_kps) == 0:
            with pytest.raises(IndexError):
                test_kps.pop()
            return

        assert test_kps.pop() == ctrl_kps.pop()

    def do_and_check_pop_with_key():
        key = ctrl_kps._get_random_key(random)
        if key is None:
            return

        assert test_kps.pop(key) == ctrl_kps.pop(key)

    def check_pop_missing_key():
        key = random.randrange(MAX_VALUE) + MAX_VALUE

        with pytest.raises(KeyError):
            test_kps.pop(key)

    def check_push():
        key = ctrl_kps._get_random_key(random)
        if key is None:
            return
        value = random.randrange(MAX_VALUE)
        priority = random.randrange(MAX_VALUE)

        with pytest.raises(ValueError):
            test_kps.push(key, value, priority)

    def check_len():
        assert len(test_kps) == len(ctrl_kps)

    N_ITERS = 3000
    ACTIONS = [
        # We have more pushes than pops, so the size of the stack should tend to grow
        # over time.
        do_push,
        do_push,
        do_push,
        do_and_check_pop,
        do_and_check_pop_with_key,
        check_len,
        check_push,
        check_pop_missing_key,
    ]
    for i in range(N_ITERS):
        action = random.choice(ACTIONS)
        action()
    while len(test_kps) > 0:
        do_and_check_pop()
    check_len()


class SimpleKeyedPriorityStack:
    """
    An alternative implementation of KeyedPriorityStack which is simpler but less
    efficient.
    """

    def __init__(self):
        self._sorted_quads = []
        self._next_seq_id = 0

    def push(self, key, value, priority):
        seq_id = self._next_seq_id
        self._next_seq_id += 1

        self._sorted_quads.append([priority, seq_id, value, key])
        self._sorted_quads.sort()

    def pop(self, key=None):
        if key is not None:
            ix = self._quad_ix_for_key(key)
            _, _, value, _ = self._sorted_quads.pop(ix)
            return value

        else:
            _, _, value, _ = self._sorted_quads.pop()
            return value

    def __len__(self):
        return len(self._sorted_quads)

    def _quad_ix_for_key(self, key):
        (quad_ix,) = [
            quad_ix
            for (quad_ix, (_, _, _, quad_key)) in enumerate(self._sorted_quads)
            if quad_key == key
        ]
        return quad_ix

    def _get_random_key(self, random):
        if len(self) == 0:
            return None
        return random.choice(self._sorted_quads)[3]
