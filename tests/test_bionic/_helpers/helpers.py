from future import standard_library
standard_library.install_aliases() # NOQA
from io import StringIO
from textwrap import dedent

import pandas as pd
from pandas import testing as pdt
from decorator import decorate


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


def df_from_csv_str(string):
    return pd.read_csv(StringIO(dedent(string)))


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
