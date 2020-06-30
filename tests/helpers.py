from io import BytesIO
from textwrap import dedent
import re
import subprocess
import shutil

import pandas as pd
from pandas import testing as pdt
from decorator import decorate

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


def count_calls(counter):
    """
    A decorator which counts the number of times the decorated function is
    called using the counter argument.
    """

    def call_counts_inner(func):
        def wrapper(func, *args, **kwargs):
            counter.mark()
            return func(*args, **kwargs)

        wrapped = decorate(func, wrapper)
        counter.reset()
        return wrapped

    return call_counts_inner


class ResettingCounter:
    """
    A class for manually counting the number of times something happens.
    Used as an argument to ``count_calls`` and directly used in situations
    where ``count_calls`` can't be used.
    """

    def __init__(self):
        self._n_calls_total = 0
        self._n_calls_since_last_check = 0

    def mark(self):
        self._n_calls_total += 1
        self._n_calls_since_last_check += 1

    def times_called(self):
        count = self._n_calls_since_last_check
        self._n_calls_since_last_check = 0
        return count

    def total_times_called(self):
        return self._n_calls_total

    def reset(self):
        self._n_calls_total = 0
        self._n_calls_since_last_check = 0


class RoundingProtocol(bn.protocols.BaseProtocol):
    def get_fixed_file_extension(self):
        return "round"

    def write(self, value, path):
        path.write_bytes(str(round(value)).encode("utf-8"))

    def read(self, path):
        return float(path.read_bytes())


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


def gsutil_wipe_path(url):
    assert "BNTESTDATA" in url
    subprocess.check_call(["gsutil", "-q", "-m", "rm", "-rf", url])


def gsutil_path_exists(url):
    return subprocess.call(["gsutil", "ls", url]) == 0


def local_wipe_path(path_str):
    assert "BNTESTDATA" in path_str
    shutil.rmtree(path_str)
