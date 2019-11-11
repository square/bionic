import pytest

from binascii import hexlify

import numpy as np
import pandas as pd
import pandas.testing as pdt
from PIL import Image

from ..helpers import (
    count_calls, df_from_csv_str, equal_frame_and_index_content)

import bionic as bn
import dask.dataframe as dd
from bionic.exception import UnsupportedSerializedValueError
from bionic.protocols import CombinedProtocol, PicklableProtocol


PICKLABLE_VALUES = [
    1,
    'string',
    True,
    [1, 2, 3],
    {'a': 1, 'b': 2},
]


@pytest.mark.parametrize('value', PICKLABLE_VALUES)
def test_picklable_value(builder, value):
    @builder
    @bn.protocol.picklable
    @count_calls
    def picklable_value():
        return value

    assert builder.build().get('picklable_value') == value
    assert builder.build().get('picklable_value') == value
    assert picklable_value.times_called() == 1


def test_picklable_with_parens(builder):
    @builder
    @bn.protocol.picklable()
    @count_calls
    def picklable_value():
        return 1

    assert builder.build().get('picklable_value') == 1
    assert builder.build().get('picklable_value') == 1
    assert picklable_value.times_called() == 1


@pytest.mark.parametrize('value', PICKLABLE_VALUES)
def test_picklable_value_is_also_dillable(builder, value):
    @builder
    @bn.protocol.dillable
    @count_calls
    def dillable_value():
        return value

    assert builder.build().get('dillable_value') == value
    assert builder.build().get('dillable_value') == value
    assert dillable_value.times_called() == 1


def test_dillable(builder):
    # Pickle doesn't support nested functions (at least in Python 2.7), but
    # dill should.
    def make_adder(x):
        def adder(y):
            return x + y
        return adder

    assert make_adder(3)(2) == 5

    @builder
    @bn.protocol.dillable
    @count_calls
    def add_two():
        return make_adder(2)

    assert builder.build().get('add_two')(3) == 5
    assert builder.build().get('add_two')(3) == 5
    assert add_two.times_called() == 1


def test_simple_dataframe(builder):
    df_value = df_from_csv_str('''
    color,number
    red,1
    blue,2
    green,3
    ''')

    @builder
    @bn.protocol.frame
    @count_calls
    def df():
        return df_value

    pdt.assert_frame_equal(builder.build().get('df'), df_value)
    pdt.assert_frame_equal(builder.build().get('df'), df_value)
    assert df.times_called() == 1


def test_typed_dataframe(builder):
    df_value = pd.DataFrame()
    df_value['int'] = [1, 2, 3]
    df_value['float'] = [1.0, 1.5, float('nan')]
    df_value['str'] = ['red', 'blue', None]
    df_value['time'] = pd.to_datetime([
        '2011-02-07',
        '2011-03-17',
        '2011-04-27',
    ])

    @builder
    @bn.protocol.frame
    def df():
        return df_value

    pdt.assert_frame_equal(builder.build().get('df'), df_value)
    assert builder.build().get('df').dtypes.to_dict() ==\
        df_value.dtypes.to_dict()


def test_dataframe_index_cols(builder):
    @builder
    @bn.protocol.frame
    def raw_df():
        return df_from_csv_str('''
        city,country,continent,metro_pop_mil
        Tokyo,Japan,Asia,38
        Delhi,India,Asia,26
        Shanghai,China,Asia,24
        Sao Paulo,Brazil,South America,21
        Mumbai,India,21
        Mexico City,Mexico,North America,21
        Beijing,China,Asia,20
        Osaka,Japan,Asia,20
        Cairo,Egypt,Africa,19
        New York,USA,North America,19
        ''')

    @builder
    @bn.protocol.frame
    def counts_df(raw_df):
        return raw_df.groupby(['continent', 'country']).size()\
            .to_frame('count')

    df = builder.build().get('counts_df')
    assert df.loc['Asia'].loc['Japan']['count'] == 2


def test_dataframe_with_categoricals_fails(builder):
    df_value = pd.DataFrame()
    df_value['cat'] = pd.Categorical(
        ['red', 'blue', 'red'],
        categories=['blue', 'red'],
        ordered=True)

    @builder
    def df():
        return df_value

    with pytest.raises(ValueError):
        builder.build().get('df')


def test_dataframe_with_categoricals_ignored(builder):
    df_value = pd.DataFrame()
    df_value['cat'] = pd.Categorical(
        ['red', 'blue', 'red'],
        categories=['blue', 'red'],
        ordered=True)

    @builder
    @bn.protocol.frame(check_dtypes=False)
    def df():
        return df_value

    pdt.assert_series_equal(
        # Whether or not the deserialized column has the Categorical Dtype can
        # depend on the version of pyarrow being used, so we'll just convert
        # both columns to the same type here.
        builder.build().get('df')['cat'].astype(object),
        df_value['cat'].astype(object))


def test_dataframe_with_duplicate_columns_fails(builder):
    df_value = pd.DataFrame(columns=['a', 'b', 'a'], data=[[1, 2, 3]])

    @builder
    def df():
        return df_value

    with pytest.raises(ValueError):
        builder.build().get('df')


def test_dataframe_with_categorical_works_with_feather(builder):
    df_value = pd.DataFrame()
    df_value['cat'] = pd.Categorical(
        ['red', 'blue', 'red'],
        categories=['blue', 'red'],
        ordered=True)

    @builder
    @bn.protocol.frame(file_format='feather')
    def df():
        return df_value

    pdt.assert_frame_equal(builder.build().get('df'), df_value)


def test_simple_dask_dataframe(builder):
    df_value = df_from_csv_str('''
    color,number
    red,1
    blue,2
    green,3
    ''')
    dask_df = dd.from_pandas(df_value, npartitions=1)

    @builder
    @bn.protocol.dask
    @count_calls
    def df():
        return dask_df

    assert equal_frame_and_index_content(builder.build().get('df').compute(), dask_df.compute())
    assert equal_frame_and_index_content(builder.build().get('df').compute(), dask_df.compute())
    assert df.times_called() == 1


def test_multiple_partitions_dask_dataframe(builder):
    df_value = df_from_csv_str('''
    color,number
    red,1
    blue,2
    green,3
    ''')
    dask_df = dd.from_pandas(df_value, npartitions=3)

    @builder
    @bn.protocol.dask
    @count_calls
    def df():
        return dask_df

    assert equal_frame_and_index_content(builder.build().get('df').compute(), dask_df.compute())
    assert equal_frame_and_index_content(builder.build().get('df').compute(), dask_df.compute())
    assert df.times_called() == 1


def test_typed_dask_dataframe(builder):
    df_value = pd.DataFrame()
    df_value['int'] = [1, 2, 3]
    df_value['float'] = [1.0, 1.5, float('nan')]
    df_value['str'] = ['red', 'blue', None]
    df_value['time'] = pd.to_datetime([
        '2011-02-07',
        '2011-03-17',
        '2011-04-27',
    ])
    dask_df = dd.from_pandas(df_value, npartitions=1)

    @builder
    @bn.protocol.dask
    def df():
        return dask_df

    assert equal_frame_and_index_content(builder.build().get('df').compute(), dask_df.compute())
    assert builder.build().get('df').compute().dtypes.to_dict() ==\
        dask_df.compute().dtypes.to_dict()


def test_dask_dataframe_index_cols(builder):
    df_value = df_from_csv_str('''
        city,country,continent,metro_pop_mil
        Tokyo,Japan,Asia,38
        Delhi,India,Asia,26
        Shanghai,China,Asia,24
        Sao Paulo,Brazil,South America,21
        Mumbai,India,21
        Mexico City,Mexico,North America,21
        Beijing,China,Asia,20
        Osaka,Japan,Asia,20
        Cairo,Egypt,Africa,19
        New York,USA,North America,19
        ''')
    dask_df = dd.from_pandas(df_value, npartitions=1)

    @builder
    @bn.protocol.dask
    def raw_df():
        return dask_df

    @builder
    @bn.protocol.dask
    def counts_df(raw_df):
        return raw_df.groupby(['continent', 'country']).size()\
            .to_frame('count')

    with pytest.raises(UnsupportedSerializedValueError):
        builder.build().get('counts_df')


def test_image_protocol(builder):
    @builder
    @bn.protocol.image
    def blue_rect():
        return Image.new('RGB', (4, 2), color='blue')

    image = builder.build().get('blue_rect')
    assert image.width == 4
    assert image.height == 2
    assert hexlify(image.tobytes()) == (b'0000ff' * 8)


def test_numpy_protocol_1d(builder):
    @builder
    @bn.protocol.numpy
    def np_array_1d():
        return np.arange(100)

    arr = builder.build().get('np_array_1d')
    desired = np.arange(100)
    assert(np.array_equal(arr, desired))


def test_numpy_protocol_2d(builder):
    @builder
    @bn.protocol.numpy
    def np_array_2d():
        return np.array([[i + j for i in range(3)] for j in range(3)])

    arr = builder.build().get('np_array_2d')
    desired = np.array([[i + j for i in range(3)] for j in range(3)])
    assert(np.array_equal(arr, desired))


def test_numpy_protocol_3d(builder):
    @builder
    @bn.protocol.numpy
    def np_array_3d():
        return np.array([[[i + j + k for i in range(3)] for j in range(3)] for k in range(3)])

    arr = builder.build().get('np_array_3d')
    desired = np.array([[[i + j + k for i in range(3)] for j in range(3)] for k in range(3)])
    assert(np.array_equal(arr, desired))


def test_numpy_protocol_high_d(builder):
    @builder
    @bn.protocol.numpy
    def np_array_high_d():
        return np.ones(shape=(5, 5, 5, 5, 5))

    arr = builder.build().get('np_array_high_d')
    desired = np.ones(shape=(5, 5, 5, 5, 5))
    assert(np.array_equal(arr, desired))


def test_yaml_protocol_list(builder):
    @builder
    @bn.protocol.yaml
    def alist():
        return ['red', 'blue']

    assert builder.build().get('alist') == ['red', 'blue']


def test_yaml_protocol_dump_kwargs(builder, tmpdir):
    @builder
    @bn.protocol.yaml(default_flow_style=False)
    def alist():
        return ['red', 'blue']

    flow = builder.build()
    expected = "- red\n- blue\n"

    assert flow.get('alist', mode='path').read_text() == expected


def test_yaml_protocol_dict(builder):
    record = {'record': {'name': 'Oscar', 'favorite_things': ['trash', 'green']}}
    @builder
    @bn.protocol.yaml
    def adict():
        return record

    assert builder.build().get('adict') == record


def test_type_protocol(builder):
    builder.declare('int_val', bn.protocol.type(int))
    builder.declare('float_val', bn.protocol.type(float))
    builder.declare('str_val', bn.protocol.type(str))

    builder.set('int_val', 1)
    builder.set('float_val', 1.0)
    builder.set('str_val', 'one')

    with pytest.raises(AssertionError):
        builder.set('int_val', 'one')
    with pytest.raises(AssertionError):
        builder.set('float_val', 1)
    with pytest.raises(AssertionError):
        builder.set('str_val', 1.0)

    flow = builder.build()

    assert flow.get('int_val') == 1
    assert flow.get('float_val') == 1.0
    assert flow.get('str_val') == 'one'


def test_enum_protocol(builder):
    builder.declare('color', bn.protocol.enum('red', 'blue'))

    builder.set('color', 'red')
    assert builder.build().get('color') == 'red'

    builder.set('color', 'blue')
    assert builder.build().get('color') == 'blue'

    with pytest.raises(AssertionError):
        builder.set('color', 'green')


def test_combined_protocol(builder):
    class WriteValueAsStringProtocol(PicklableProtocol):
        def __init__(self, value, string_value):
            super(WriteValueAsStringProtocol, self).__init__()

            self._value = value
            self._string_value = string_value

        def validate(self, value):
            assert value == self._value

        def get_fixed_file_extension(self):
            return self._string_value

        def write(self, value, file_):
            super(WriteValueAsStringProtocol, self).write(
                self._string_value, file_)

    one_protocol = WriteValueAsStringProtocol(1, 'one')
    two_protocol = WriteValueAsStringProtocol(2, 'two')

    builder.declare('value')

    @builder
    @one_protocol
    def must_be_one(value):
        return value

    @builder
    @two_protocol
    def must_be_two(value):
        return value

    @builder
    @CombinedProtocol(one_protocol, two_protocol)
    def must_be_one_or_two(value):
        return value

    flow = builder.build().setting('value', 1)

    assert flow.get('must_be_one') == 'one'
    with pytest.raises(AssertionError):
        flow.get('must_be_two')
    assert flow.get('must_be_one_or_two') == 'one'

    flow = flow.setting('value', 2)
    with pytest.raises(AssertionError):
        flow.get('must_be_one')
    assert flow.get('must_be_two') == 'two'
    assert flow.get('must_be_one_or_two') == 'two'

    flow = flow.setting('value', 3)
    with pytest.raises(AssertionError):
        flow.get('must_be_one_or_two')
