'''
Miscellaneous data-science-specific utility functions.
'''

import math

import pandas as pd

from IPython.display import display


def render_entire_frame(
        df, max_rows=None, max_columns=None, max_colwidth=10000):
    with pd.option_context(
            'display.max_rows', max_rows,
            'display.max_columns', max_columns,
            'display.max_colwidth', max_colwidth):
        display(df)


def pd_compose(values_by_intermediate, intermediates_by_key):
    '''
    Accepts two DataFrames and/or Serieses.
    Return an object with the type and contents of values_by_intermediate,
    mapped to the index of intermediates_by_key.
    '''
    if isinstance(values_by_intermediate, pd.DataFrame):
        vbi_frame = values_by_intermediate
        vbi_cols = list(values_by_intermediate.columns)
    elif isinstance(values_by_intermediate, pd.Series):
        vbi_frame = values_by_intermediate.to_frame()
        vbi_cols = values_by_intermediate.name

    if isinstance(intermediates_by_key, pd.DataFrame):
        ibk_frame = intermediates_by_key
        # We convert this to a list because for some reason merge gets
        # confused when we pass the raw columns object.
        ibk_cols = list(intermediates_by_key.columns)
    elif isinstance(intermediates_by_key, pd.Series):
        ibk_frame = intermediates_by_key.to_frame()
        ibk_cols = intermediates_by_key.name

    return pd.merge(
        left=vbi_frame,
        right=ibk_frame,
        how='right',
        left_index=True,
        right_on=ibk_cols,
    )[vbi_cols]


def drop_col_level(df):
    df = df.copy()
    df.columns = df.columns.droplevel()
    return df


# TODO Should we use np.log instead?
def log10(x):
    return math.log(x, 10)


def exp10(x):
    return 10 ** x


def norm_series(xs):
    return xs / float(xs.sum())


def norm_rows(df):
    return df.div(df.sum(axis=1), axis=0)


def norm_cols(df):
    return df.div(df.sum(axis=0), axis=1)
