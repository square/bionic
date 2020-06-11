import pytest

from binascii import hexlify
import tempfile
from pathlib import Path

import numpy as np
import geopandas
import pandas as pd
import pandas.testing as pdt
from PIL import Image
import dask.dataframe as dd

from ..helpers import count_calls, df_from_csv_str, equal_frame_and_index_content

import bionic as bn
from bionic.exception import (
    EntitySerializationError,
    UnsupportedSerializedValueError,
)
from bionic.protocols import CombinedProtocol, PicklableProtocol
from bionic.util import recursively_delete_path


PICKLABLE_VALUES = [
    1,
    "string",
    True,
    [1, 2, 3],
    {"a": 1, "b": 2},
]


# A passthrough decorator that can be used in place of a protocol
# decorator to allow testing default protocol. This decorator is just a
# convenience wrapper to use pytest parameterization.
def passthrough(f):
    return f


@pytest.mark.parametrize("value", PICKLABLE_VALUES)
@pytest.mark.parametrize("protocol", [bn.protocol.picklable, passthrough])
def test_picklable_value(builder, make_counter, protocol, value):
    counter = make_counter()

    @builder
    @protocol
    @count_calls(counter)
    def picklable_value():
        return value

    assert builder.build().get("picklable_value") == value
    assert builder.build().get("picklable_value") == value
    assert counter.times_called() == 1


def test_picklable_with_parens(builder, make_counter):
    counter = make_counter()

    @builder
    @bn.protocol.picklable()
    @count_calls(counter)
    def picklable_value():
        return 1

    assert builder.build().get("picklable_value") == 1
    assert builder.build().get("picklable_value") == 1
    assert counter.times_called() == 1


@pytest.mark.parametrize("value", PICKLABLE_VALUES)
def test_picklable_value_is_also_dillable(builder, make_counter, value):
    counter = make_counter()

    @builder
    @bn.protocol.dillable
    @count_calls(counter)
    def dillable_value():
        return value

    assert builder.build().get("dillable_value") == value
    assert builder.build().get("dillable_value") == value
    assert counter.times_called() == 1


def test_dillable(builder, make_counter):
    # Pickle doesn't support nested functions (at least in Python 2.7), but
    # dill should.
    def make_adder(x):
        def adder(y):
            return x + y

        return adder

    assert make_adder(3)(2) == 5

    counter = make_counter()

    @builder
    @bn.protocol.dillable
    @count_calls(counter)
    def add_two():
        return make_adder(2)

    assert builder.build().get("add_two")(3) == 5
    assert builder.build().get("add_two")(3) == 5
    assert counter.times_called() == 1


def test_simple_dataframe(builder, make_counter):
    df_value = df_from_csv_str(
        """
    color,number
    red,1
    blue,2
    green,3
    """
    )

    counter = make_counter()

    @builder
    @bn.protocol.frame
    @count_calls(counter)
    def df():
        return df_value

    pdt.assert_frame_equal(builder.build().get("df"), df_value)
    pdt.assert_frame_equal(builder.build().get("df"), df_value)
    assert counter.times_called() == 1


def test_typed_dataframe(builder):
    df_value = pd.DataFrame()
    df_value["int"] = [1, 2, 3]
    df_value["float"] = [1.0, 1.5, float("nan")]
    df_value["str"] = ["red", "blue", None]
    df_value["time"] = pd.to_datetime(["2011-02-07", "2011-03-17", "2011-04-27"])

    @builder
    @bn.protocol.frame
    def df():
        return df_value

    pdt.assert_frame_equal(builder.build().get("df"), df_value)
    assert builder.build().get("df").dtypes.to_dict() == df_value.dtypes.to_dict()


def test_dataframe_index_cols(builder):
    @builder
    @bn.protocol.frame
    def raw_df():
        return df_from_csv_str(
            """
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
        """
        )

    @builder
    @bn.protocol.frame
    def counts_df(raw_df):
        return raw_df.groupby(["continent", "country"]).size().to_frame("count")

    df = builder.build().get("counts_df")
    assert df.loc["Asia"].loc["Japan"]["count"] == 2


def test_dataframe_with_categoricals_fails(builder):
    df_value = pd.DataFrame()
    df_value["cat"] = pd.Categorical(
        ["red", "blue", "red"], categories=["blue", "red"], ordered=True
    )

    @builder
    def df():
        return df_value

    with pytest.raises(EntitySerializationError):
        builder.build().get("df")


def test_dataframe_with_categoricals_ignored(builder):
    df_value = pd.DataFrame()
    df_value["cat"] = pd.Categorical(
        ["red", "blue", "red"], categories=["blue", "red"], ordered=True
    )

    @builder
    @bn.protocol.frame(check_dtypes=False)
    def df():
        return df_value

    pdt.assert_series_equal(
        # Whether or not the deserialized column has the Categorical Dtype can
        # depend on the version of pyarrow being used, so we'll just convert
        # both columns to the same type here.
        builder.build().get("df")["cat"].astype(object),
        df_value["cat"].astype(object),
    )


def test_dataframe_with_duplicate_columns_fails(builder):
    df_value = pd.DataFrame(columns=["a", "b", "a"], data=[[1, 2, 3]])

    @builder
    def df():
        return df_value

    with pytest.raises(EntitySerializationError):
        builder.build().get("df")


def test_dataframe_with_categorical_works_with_feather(builder):
    df_value = pd.DataFrame()
    df_value["cat"] = pd.Categorical(
        ["red", "blue", "red"], categories=["blue", "red"], ordered=True
    )

    @builder
    @bn.protocol.frame(file_format="feather")
    def df():
        return df_value

    pdt.assert_frame_equal(builder.build().get("df"), df_value)


@pytest.mark.parametrize("protocol", [bn.protocol.dask, passthrough])
def test_simple_dask_dataframe(builder, make_counter, protocol):
    df_value = df_from_csv_str(
        """
    color,number
    red,1
    blue,2
    green,3
    """
    )
    dask_df = dd.from_pandas(df_value, npartitions=1)

    counter = make_counter()

    @builder
    @protocol
    @count_calls(counter)
    def df():
        return dask_df

    assert equal_frame_and_index_content(
        builder.build().get("df").compute(), dask_df.compute()
    )
    assert equal_frame_and_index_content(
        builder.build().get("df").compute(), dask_df.compute()
    )
    assert counter.times_called() == 1


def test_multiple_partitions_dask_dataframe(builder, make_counter):
    df_value = df_from_csv_str(
        """
    color,number
    red,1
    blue,2
    green,3
    """
    )
    dask_df = dd.from_pandas(df_value, npartitions=3)

    counter = make_counter()

    @builder
    @bn.protocol.dask
    @count_calls(counter)
    def df():
        return dask_df

    assert equal_frame_and_index_content(
        builder.build().get("df").compute(), dask_df.compute()
    )
    assert equal_frame_and_index_content(
        builder.build().get("df").compute(), dask_df.compute()
    )
    assert counter.times_called() == 1


def test_typed_dask_dataframe(builder):
    df_value = pd.DataFrame()
    df_value["int"] = [1, 2, 3]
    df_value["float"] = [1.0, 1.5, float("nan")]
    df_value["str"] = ["red", "blue", None]
    df_value["time"] = pd.to_datetime(["2011-02-07", "2011-03-17", "2011-04-27"])
    dask_df = dd.from_pandas(df_value, npartitions=1)

    @builder
    @bn.protocol.dask
    def df():
        return dask_df

    assert equal_frame_and_index_content(
        builder.build().get("df").compute(), dask_df.compute()
    )
    assert (
        builder.build().get("df").compute().dtypes.to_dict()
        == dask_df.compute().dtypes.to_dict()
    )


def test_dask_dataframe_index_cols(builder):
    df_value = df_from_csv_str(
        """
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
        """
    )
    dask_df = dd.from_pandas(df_value, npartitions=1)

    @builder
    @bn.protocol.dask
    def raw_df():
        return dask_df

    @builder
    @bn.protocol.dask
    def counts_df(raw_df):
        return raw_df.groupby(["continent", "country"]).size().to_frame("count")

    with pytest.raises(UnsupportedSerializedValueError):
        builder.build().get("counts_df")


@pytest.mark.parametrize("protocol", [bn.protocol.image, passthrough])
def test_image_protocol(builder, protocol):
    @builder
    @protocol
    def blue_rect():
        return Image.new("RGB", (4, 2), color="blue")

    image = builder.build().get("blue_rect")
    assert image.width == 4
    assert image.height == 2
    assert hexlify(image.tobytes()) == (b"0000ff" * 8)


@pytest.mark.parametrize("protocol", [bn.protocol.numpy, passthrough])
def test_numpy_protocol_1d(builder, protocol):
    @builder
    @protocol
    def np_array_1d():
        return np.arange(100)

    arr = builder.build().get("np_array_1d")
    desired = np.arange(100)
    assert np.array_equal(arr, desired)


def test_numpy_protocol_2d(builder):
    @builder
    @bn.protocol.numpy
    def np_array_2d():
        return np.array([[i + j for i in range(3)] for j in range(3)])

    arr = builder.build().get("np_array_2d")
    desired = np.array([[i + j for i in range(3)] for j in range(3)])
    assert np.array_equal(arr, desired)


def test_numpy_protocol_3d(builder):
    @builder
    @bn.protocol.numpy
    def np_array_3d():
        return np.array(
            [[[i + j + k for i in range(3)] for j in range(3)] for k in range(3)]
        )

    arr = builder.build().get("np_array_3d")
    desired = np.array(
        [[[i + j + k for i in range(3)] for j in range(3)] for k in range(3)]
    )
    assert np.array_equal(arr, desired)


def test_numpy_protocol_high_d(builder):
    @builder
    @bn.protocol.numpy
    def np_array_high_d():
        return np.ones(shape=(5, 5, 5, 5, 5))

    arr = builder.build().get("np_array_high_d")
    desired = np.ones(shape=(5, 5, 5, 5, 5))
    assert np.array_equal(arr, desired)


def test_yaml_protocol_list(builder):
    @builder
    @bn.protocol.yaml
    def alist():
        return ["red", "blue"]

    assert builder.build().get("alist") == ["red", "blue"]


def test_yaml_protocol_dump_kwargs(builder, tmpdir):
    @builder
    @bn.protocol.yaml(default_flow_style=False)
    def alist():
        return ["red", "blue"]

    flow = builder.build()
    expected = "- red\n- blue\n"

    assert flow.get("alist", mode="path").read_text() == expected


def test_yaml_protocol_dict(builder):
    record = {"record": {"name": "Oscar", "favorite_things": ["trash", "green"]}}

    @builder
    @bn.protocol.yaml
    def adict():
        return record

    assert builder.build().get("adict") == record


def test_type_protocol(builder):
    builder.declare("int_val", bn.protocol.type(int))
    builder.declare("float_val", bn.protocol.type(float))
    builder.declare("str_val", bn.protocol.type(str))

    builder.set("int_val", 1)
    builder.set("float_val", 1.0)
    builder.set("str_val", "one")

    with pytest.raises(AssertionError):
        builder.set("int_val", "one")
    with pytest.raises(AssertionError):
        builder.set("float_val", 1)
    with pytest.raises(AssertionError):
        builder.set("str_val", 1.0)

    flow = builder.build()

    assert flow.get("int_val") == 1
    assert flow.get("float_val") == 1.0
    assert flow.get("str_val") == "one"


def test_enum_protocol(builder):
    builder.declare("color", bn.protocol.enum("red", "blue"))

    builder.set("color", "red")
    assert builder.build().get("color") == "red"

    builder.set("color", "blue")
    assert builder.build().get("color") == "blue"

    with pytest.raises(AssertionError):
        builder.set("color", "green")


@pytest.mark.parametrize("operation", ["move", "copy"])
def test_path_protocol(builder, make_counter, tmp_path, operation):
    # Our entities will return paths in this directory.
    working_dir_path = Path(tempfile.mkdtemp(dir=tmp_path))
    output_phrase_file_path = working_dir_path / "phrase"
    output_colors_dir_path = working_dir_path / "colors"

    phrase_str = "hello world"
    colors = ["red", "blue"]

    phrase_file_path_counter = make_counter()

    @builder
    @bn.protocol.path(operation=operation)
    @count_calls(phrase_file_path_counter)
    def phrase_file_path():
        "A path to a file containing a phrase."
        output_phrase_file_path.write_text(phrase_str)
        return output_phrase_file_path

    colors_dir_path_counter = make_counter()

    @builder
    @bn.protocol.path(operation=operation)
    @count_calls(colors_dir_path_counter)
    def colors_dir_path():
        "A path to a directory containing some files named after colors."
        output_colors_dir_path.mkdir()
        for color in colors:
            (output_colors_dir_path / color).write_text(color)
        return output_colors_dir_path

    def check_file_contents(file_path):
        assert file_path.read_text() == phrase_str

    def check_dir_contents(dir_path):
        file_paths = list(dir_path.iterdir())
        assert set(path.name for path in file_paths) == set(colors)
        for file_path in file_paths:
            assert file_path.read_text() == file_path.name

    check_file_contents(builder.build().get("phrase_file_path"))
    check_dir_contents(builder.build().get("colors_dir_path"))
    assert phrase_file_path_counter.times_called() == 1
    assert colors_dir_path_counter.times_called() == 1

    if operation == "copy":
        # Check that the original files are still there.
        check_file_contents(output_phrase_file_path)
        check_dir_contents(output_colors_dir_path)

        # Check that the cached versions are still accessible if we delete the
        # originals.
        for sub_path in working_dir_path.iterdir():
            recursively_delete_path(sub_path)

        check_file_contents(builder.build().get("phrase_file_path"))
        check_dir_contents(builder.build().get("colors_dir_path"))
        assert phrase_file_path_counter.times_called() == 0
        assert colors_dir_path_counter.times_called() == 0

    elif operation == "move":
        # Check that the original files have been removed.
        assert list(working_dir_path.iterdir()) == []


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
            super(WriteValueAsStringProtocol, self).write(self._string_value, file_)

    one_protocol = WriteValueAsStringProtocol(1, "one")
    two_protocol = WriteValueAsStringProtocol(2, "two")

    builder.declare("value")

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

    flow = builder.build().setting("value", 1)

    assert flow.get("must_be_one") == "one"
    with pytest.raises(AssertionError):
        flow.get("must_be_two")
    assert flow.get("must_be_one_or_two") == "one"

    flow = flow.setting("value", 2)
    with pytest.raises(AssertionError):
        flow.get("must_be_one")
    assert flow.get("must_be_two") == "two"
    assert flow.get("must_be_one_or_two") == "two"

    flow = flow.setting("value", 3)
    with pytest.raises(AssertionError):
        flow.get("must_be_one_or_two")


@pytest.mark.parametrize("protocol", [bn.protocol.geodataframe, passthrough])
def test_geodataframe_protocol(builder, protocol):
    @builder
    @protocol
    def basic_geopandas_df():
        return geopandas.read_file(geopandas.datasets.get_path("nybb"))

    gdf1 = builder.build().get("basic_geopandas_df")
    gdf2 = geopandas.read_file(geopandas.datasets.get_path("nybb"))
    pdt.assert_frame_equal(gdf1, gdf2)


def test_geodataframe_protocol_fails_with_long_column_name(builder):
    @builder
    @bn.protocol.geodataframe
    def longname_geopandas_df():
        df = geopandas.read_file(geopandas.datasets.get_path("nybb"))
        columns = list(df.columns)
        columns[0] = "extra_long_column_name"
        df.columns = columns
        return df

    with pytest.raises(EntitySerializationError):
        builder.build().get("longname_geopandas_df")


@pytest.mark.parametrize("protocol1", [bn.protocol.picklable, bn.protocol.frame])
@pytest.mark.parametrize("protocol2", [bn.protocol.picklable, bn.protocol.image])
def test_redundant_protocols(builder, protocol1, protocol2):
    with pytest.warns(Warning):

        @builder
        @protocol1
        @protocol2
        def problem():
            return None

    actual_protocol = builder.build().entity_protocol("problem")
    assert isinstance(actual_protocol, protocol1().__class__)
