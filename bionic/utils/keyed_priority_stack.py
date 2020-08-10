"""
Provides an implementation of an updatable priority queue with specific ordering rules.

The other implementations I found (including `heapq` and `queue.PriorityQueue`) all have
one or more problems:

1. They're based on min-heaps, so they return the lowest value first; this is
counterintuitive when dealing with priorities.
2. Built-in tiebreaking is not provided.
3. Updating priorities is impossible, or needs to be implemented separately.

The implementation here is based on the `heapq` implementation of a binary heap, but
adds reversed ordering (highest priority first), LIFO tiebreaking, and keyed lookup.
"""

import heapq
from functools import total_ordering


class KeyedPriorityStack:
    """
    An updatable priority queue where ties are broken in last-in-first-out (LIFO) order.

    This data structure has a stack-like interface, supporting `push` and `pop`, but
    each element on the stack also has an associated key and priority. By default,
    `pop` removes returns the element with the *highest* priority (breaking ties in
    LIFO order), but it also accepts an optional key argument that specifies a specific
    element to be popped. This can be used to easily update an element's priority.
    """

    def __init__(self):
        self._heap = []
        self._next_seq_id = 0
        self._n_unremoved_entries = 0
        self._unremoved_entries_by_key = {}

    def push(self, key, value, priority):
        """
        Adds a value to the stack with associated key and priority.
        """

        if key is None:
            raise KeyError("Attempted to add None as key to priority stack")
        if key in self._unremoved_entries_by_key:
            raise ValueError(
                f"Attempted to add duplicate key to priority stack: {key!r}"
            )
        seq_id = self._next_seq_id
        self._next_seq_id += 1
        entry = PriorityEntry(priority, seq_id, key, value)
        self._unremoved_entries_by_key[key] = entry
        heapq.heappush(self._heap, entry)
        self._n_unremoved_entries += 1

    def pop(self, key=None):
        """
        Removes a value from the stack and returns it.

        If no key is provided, removes and returns the highest-priority element (or
        the last-added such element, if there is a tie).

        If a key is provided, removes and returns the element with that key.
        """

        if key is not None:
            if key not in self._unremoved_entries_by_key:
                raise KeyError(f"Key not found in priority stack: {key!r}")
            entry = self._unremoved_entries_by_key.pop(key)
            entry.is_removed = True
            self._n_unremoved_entries -= 1
            return entry.value

        else:
            while True:
                if self._n_unremoved_entries == 0:
                    raise IndexError("Attempted to get item from empty priority stack")
                entry = heapq.heappop(self._heap)
                if entry.is_removed:
                    continue
                self._n_unremoved_entries -= 1
                del self._unremoved_entries_by_key[entry.key]
                return entry.value

    def __len__(self):
        """
        Returns the number of elements on the stack.
        """

        return self._n_unremoved_entries


@total_ordering
class PriorityEntry:
    def __init__(self, priority, seq_id, key, value):
        self.priority = priority
        self.seq_id = seq_id
        self.key = key
        self.value = value
        self.is_removed = False

    def __lt__(self, other):
        assert isinstance(other, PriorityEntry)

        return (self.priority, self.seq_id) > (other.priority, other.seq_id)

    def __eq__(self, other):
        if not isinstance(other, PriorityEntry):
            return False
        return (self.priority, self.seq_id) == (other.priority, other.seq_id)
