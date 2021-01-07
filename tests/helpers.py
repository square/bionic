from io import BytesIO
from textwrap import dedent
import types
import re
import shutil

from decorator import decorate
import pandas as pd
from pandas import testing as pdt
import networkx as nx

import bionic as bn


# TODO This name is cumbersome; maybe one of these shorter names?
# - equal_unordered
# - eq_unordered
# - set_eq
def equal_when_sorted(xs, ys):
    return list(sorted(xs)) == list(sorted(ys))


def lsorted(xs):
    return list(sorted(xs))


def assert_frames_equal_when_sorted(df1, df2):
    if len(df1.columns) > 0:
        df1 = df1.sort_values(list(df1.columns)).reset_index(drop=True)
    if len(df2.columns) > 0:
        df2 = df2.sort_values(list(df2.columns)).reset_index(drop=True)
    pdt.assert_frame_equal(df1, df2)


def equal_frame_and_index_content(df1, df2):
    """
    Checks whether the passed dataframes have the same content and index values.  This ignores
    index type, so a dataframe with RangeIndex(start=0, stop=3, step=1) will be considered equal
    to Int64Index([0, 1, 2], dtype='int64', name='index')
    """
    return df1.equals(df2) and list(df1.index) == list(df2.index)


def df_from_csv_str(string):
    bytestring = dedent(string).encode("utf-8")
    return pd.read_csv(BytesIO(bytestring))


class SimpleCounter:
    """
    A class for manually counting the number of times something happens.
    """

    def __init__(self):
        self._count = 0

    def inc(self):
        self._count += 1

    def reset(self):
        self._count = 0

    def get(self):
        return self._count


class ResettingCallCounter:
    """
    A class for counting the number of times a function is called.

    Can either be used as a decorator, or triggered manually with the `mark` method.

    When this is created, a SimpleCounter can be passed as an argument; this allows
    this counter to act as a wrapper around a counter object managed by a
    ``multiprocessing`` manager. (The ResettingCallCounter class itself cannot be
    managed because its instances are callable, and managed objects are implemented
    by proxies that don't support the ``__call__`` method; accepting a wrapped
    counter object is a workaround.)
    """

    def __init__(self, counter=None):
        if counter is None:
            counter = SimpleCounter()
        self._counter = counter

    def mark(self):
        self._counter.inc()

    def times_called(self):
        count = self._counter.get()
        self._counter.reset()
        return count

    def reset(self):
        self._counter.reset()

    def __call__(self, func):
        def wrapper(func_, *args, **kwargs):
            self.mark()
            return func_(*args, **kwargs)

        wrapped = decorate(func, wrapper)
        self.reset()
        return wrapped


class RoundingProtocol(bn.protocols.BaseProtocol):
    def get_fixed_file_extension(self):
        return "round"

    def write(self, value, path):
        path.write_bytes(str(round(value)).encode("utf-8"))

    def read(self, path):
        return float(path.read_bytes())


def assert_messages_match_regexes(
    messages, regexes, allow_unmatched_messages=False, allow_unmatched_regexes=False
):
    """
    Asserts that a sequence of messages can be matched against a sequence of regexes.

    This function tries to find a 1:1 mapping from each message to a regex that it
    matches. If this is impossible, it finds the best possible mapping and then raises
    an exception listing the unmatched values.

    If `allow_unmatched_messages` is set, then mappings with some unmatched messages
    won't raise an exception; likewise for `allow_unmatched_regexes`.

    The runtime of this function is roughly `O(len(messages) * len(regexes))`, so it
    will be expensive for large inputs.
    """

    matched_regexes_by_message = find_best_message_regex_pairing(messages, regexes)
    matched_regexes = set(matched_regexes_by_message.values())
    unmatched_regexes = [regex for regex in regexes if regex not in matched_regexes]
    unmatched_messages = [
        message for message in messages if message not in matched_regexes_by_message
    ]

    if (unmatched_messages and not allow_unmatched_messages) or (
        unmatched_regexes and not allow_unmatched_regexes
    ):
        failure_message = f"""
        Failed to match messages against regexes.
        Unmatched messages: {unmatched_messages!r}
        Unmatched regexes: {unmatched_regexes!r}
        """
        assert False, dedent(failure_message).strip()


def find_best_message_regex_pairing(messages, regexes):
    """
    Finds a maximal matching between a sequence of messages and a sequence of regexes.

    Returns the largest possible dictionary where each key is a message and each value
    is a regex that matches its key.

    The runtime of this function should be roughly `O(len(messages) * len(regexes))`.
    """

    messages = set(messages)
    compiled_regexes = [re.compile(regex, flags=re.DOTALL) for regex in regexes]

    # We'll reduce this to a bipartite matching problem: treat each message and regex
    # as a node, and each possible message-regex match as an edge, then find the largest
    # set of edges that don't share any nodes.
    graph = nx.Graph()
    graph.add_nodes_from(messages)
    graph.add_nodes_from(compiled_regexes)
    graph.add_edges_from(
        (message, compiled_regex)
        for message in messages
        for compiled_regex in compiled_regexes
        if compiled_regex.fullmatch(message)
    )

    # This uses the Hopcroft-Karp algorithm to find the maximum matching. The algorithm
    # is worst-case O(num_edges * sqrt(num_vertices)), or O(num_vertices ** 2.5), so
    # "roughly" quadratic.
    nodes_by_matched_node = nx.algorithms.bipartite.maximum_matching(
        graph,
        # NetworkX doesn't know which nodes are messages and which are regexes, so we
        # need to tell it which side of the graph is which.
        top_nodes=messages,
    )

    # The matching algorithm returns every pairing in both directions; we only want to
    # keep the matches going from message to regex (not vice versa).
    matched_regexes_by_message = {
        left_node: right_node.pattern
        for left_node, right_node in nodes_by_matched_node.items()
        if left_node in messages
    }

    return matched_regexes_by_message


def assert_re_matches(regex, string, flags=0):
    """
    Equivalent to `assert re.match(regex, string, flags)` but with a nicer
    error message that shows how how much of the regex and string matched.
    """

    # Check if the full regex matches.
    match = longest_regex_prefix_match(regex, string, flags)
    matched_str = match.group(0)
    matched_rgx = match.re.pattern
    if len(matched_rgx) == len(regex):
        return

    # Otherwise, identify the parts that didn't match.
    assert string.startswith(matched_str)
    assert regex.startswith(matched_rgx)
    unmatched_str = string[len(matched_str) :]
    unmatched_rgx = regex[len(matched_rgx) :]

    # We'll display the results in two columns: first the matching parts, then
    # the non-matching parts.
    def fmt(s):
        s = repr(s)
        if len(s) > 20:
            s = s[:8] + "<...>" + s[-7:]
        return s

    matched_parts = [fmt(matched_rgx), fmt(matched_str), "(match)"]
    unmatched_parts = [fmt(unmatched_rgx), fmt(unmatched_str), "(MISMATCH)"]

    # Align each column so its elements each have the same width.
    max_matched_len = max(len(s) for s in matched_parts)
    matched_parts = [s.rjust(max_matched_len) for s in matched_parts]
    max_unmatched_len = max(len(s) for s in unmatched_parts)
    unmatched_parts = [s.ljust(max_unmatched_len) for s in unmatched_parts]

    raise AssertionError(
        f"assert re.match({regex!r}, {string!r})\n"
        f"   regex: {matched_parts[0]} + {unmatched_parts[0]}\n"
        f"  string: {matched_parts[1]} + {unmatched_parts[1]}\n"
        f"          {matched_parts[2]}   {unmatched_parts[2]}"
    )


def longest_regex_prefix_match(regex, string, flags=0):
    """
    Returns the longest prefix of `regex` that matches `string`. (Note this
    uses re.match, which means the regex needs to match the beginning of the
    string, but not the entire string. This also means that the empty prefix
    will always match, so this function will always return a value.)
    """

    # If the full regex matches, then we'll just return that.
    match = re.match(regex, string, flags=flags)
    if match:
        return match

    assert len(regex) > 0

    # Otherwise, we'll find the longest matching prefix using binary search.
    # We know a lower bound to get a match (the empty regex always matches)
    # and an upper bound for failure (the full regex didn't match).
    max_succ_ix = 0
    min_fail_ix = len(regex)

    while max_succ_ix + 1 < min_fail_ix:
        # We'll test the prefix that ends at this index.
        mid_ix = (max_succ_ix + min_fail_ix + 1) // 2

        # We don't know if this prefix actually forms a valid regex -- it may be
        # cut off in the middle of an escape code or parenthetical.
        # If this exact index doesn't work, we'll find the nearest working index
        # (if any) to either side.
        found_valid_pattern = False

        # Test the current index, and walk to the right until we find a working
        # index.
        hi_ix = mid_ix
        while hi_ix < min_fail_ix:
            try:
                pattern = re.compile(regex[:hi_ix], flags=flags)
                match = pattern.match(string)
                if match:
                    max_succ_ix = hi_ix
                else:
                    min_fail_ix = hi_ix
                found_valid_pattern = True
                break
            except re.error:
                hi_ix += 1

        # If the original index didn't work, we'll also walk to the left until
        # we find a working index.
        if hi_ix != mid_ix:
            lo_ix = mid_ix - 1
            while lo_ix > max_succ_ix:
                try:
                    pattern = re.compile(regex[:lo_ix], flags=flags)
                    match = pattern.match(string)
                    if match:
                        max_succ_ix = lo_ix
                    else:
                        min_fail_ix = lo_ix
                    break
                    found_valid_pattern = True
                except re.error:
                    lo_ix -= 1

        # If we didn't find any valid regexs between our bounds, then we're done
        # searching.
        if not found_valid_pattern:
            break

    match = re.search(regex[:max_succ_ix], string, flags=flags)
    assert match is not None
    return match


def local_wipe_path(path_str):
    assert "BNTESTDATA" in path_str
    shutil.rmtree(path_str)


def import_code(code, vars_dict={}, is_module_internal=False):
    # Create a blank module.
    if is_module_internal:
        module = types.ModuleType("bionic.my_test_mod")
        module.__file__ = "bionic/tests.py"
    else:
        module = types.ModuleType("my_test_mod")
        module.__file__ = "my_file.py"
    # Populate the module with code.
    # exec(code, {**vars_dict, **module.__dict__})
    module.__dict__.update(vars_dict)
    exec(code, module.__dict__)
    return module
