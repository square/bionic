import pytest

from io import BytesIO
import os
from textwrap import dedent

import pandas as pd
from pandas import testing as pdt
from decorator import decorate

import bionic as bn

GCS_TEST_BUCKET = os.environ.get('BIONIC_GCS_TEST_BUCKET', None)
skip_unless_gcs = pytest.mark.skipif(
    GCS_TEST_BUCKET is None,
    reason='the BIONIC_GCS_TEST_BUCKET env variable was not set'
)


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
    '''
    Checks whether the passed dataframes have the same content and index values.  This ignores
    index type, so a dataframe with RangeIndex(start=0, stop=3, step=1) will be considered equal
    to Int64Index([0, 1, 2], dtype='int64', name='index')
    '''
    return df1.equals(df2) and list(df1.index) == list(df2.index)


def df_from_csv_str(string):
    bytestring = dedent(string).encode('utf-8')
    return pd.read_csv(BytesIO(bytestring))


def count_calls(func):
    '''
    A decorator which counts the number of times the decorated function is
    called.  The decorated function will have two methods attached:

    - times_called(): returns the number of calls since the last time
      times_called() was invoked
    - times_called_total(): returns the total number of calls ever
    '''
    container = []

    def wrapper(func, *args, **kwargs):
        wrapped_func = container[0]
        wrapped_func._n_calls_total += 1
        wrapped_func._n_calls_since_last_check += 1
        return func(*args, **kwargs)

    wrapped = decorate(func, wrapper)
    wrapped._n_calls_since_last_check = 0
    wrapped._n_calls_total = 0

    def times_called():
        n = wrapped._n_calls_since_last_check
        wrapped._n_calls_since_last_check = 0
        return n
    wrapped.times_called = times_called

    def total_times_called():
        return wrapped._n_calls_total
    wrapped.total_times_called = total_times_called

    container.append(wrapped)

    return wrapped


class ResettingCounter(object):
    """
    A class for manually counting the number of times something happens.
    Used mainly in situations whre ``count_calls`` can't be used.
    """

    def __init__(self):
        self._count = 0

    def mark(self):
        self._count += 1

    def times_called(self):
        count = self._count
        self._count = 0
        return count


class RoundingProtocol(bn.protocols.BaseProtocol):
    def get_fixed_file_extension(self):
        return 'round'

    def write(self, value, path):
        path.write_bytes(str(round(value)).encode('utf-8'))

    def read(self, path, extension):
        return float(path.read_bytes())
