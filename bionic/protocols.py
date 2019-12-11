'''
Contains bionic's concept of a "protocol": an object that specifies how to
handle certain types of data.  A protocol is similar to a Python type, but
might be more or less specific.  For example, PicklableProtocol can be used
for any type that can be safely pickled.  On the other hand, DataFrameProtocol
takes additional arguments that constrain the structure of the DataFrames it
handles.  Protocols specify how to serialize, deserialize, and validate data.

This module contains a BaseProtocol class and various subclasses.
'''

from collections import Counter
import pickle
import sys
import tempfile
import shutil
import warnings
from pathlib import Path

import yaml
import numpy as np
from pyarrow import parquet, Table
import pandas as pd

from .exception import UnsupportedSerializedValueError
from .provider import provider_wrapper, ProtocolUpdateProvider
from .optdep import import_optional_dependency
from .util import read_hashable_bytes_from_file_or_dir, oneline
from . import tokenization


def check_is_like_protocol(obj):
    for method_name in 'validate', 'read', 'write':
        if not hasattr(obj, method_name):
            raise ValueError(oneline(f'''
                Expected {obj!r} to be a kind of Protocol, but didn't find
                expected method {method_name!r}'''))


class BaseProtocol(object):
    def validate(self, value):
        pass

    def value_is_valid(self, value):
        try:
            self.validate(value)
        except Exception:
            return False
        return True

    def get_fixed_file_extension(self):
        raise NotImplementedError()

    def file_extension_for_value(self, value):
        return self.get_fixed_file_extension()

    def can_read_file_extension(self, extension):
        return extension == self.get_fixed_file_extension()

    def supports_value(self, value):
        return any(
            isinstance(value, type_)
            for type_ in self.get_all_supported_types()
        )

    def write(self, value, path):
        raise NotImplementedError()

    def read(self, path):
        raise NotImplementedError()

    SIMPLE_TYPES = {
        bool,
        str, bytes, str,
        int, int, float,
    }

    def tokenize(self, value):
        if type(value) in self.SIMPLE_TYPES:
            return tokenization.tokenize(value)
        else:
            return tokenization.tokenize(value, self._write_to_bytes)

    def _write_to_bytes(self, value):
        file_name = 'temp_file'
        temp_dir = Path(tempfile.mkdtemp())
        try:
            temp_file_path = temp_dir / file_name
            self.write(value, temp_file_path)
            return read_hashable_bytes_from_file_or_dir(temp_file_path)
        finally:
            shutil.rmtree(str(temp_dir))

    # This lets a protocol object be used in two different ways:
    # - called with a function or provider and used as a decorator
    # - called with keyword arguments to construct a new instance
    # The advantage is that later we can do something like:
    #
    #    frame = DataFrameProtocol()
    #
    # and then do either
    #
    #    @builder
    #    @frame
    #    def func(...):
    #        ...
    #
    # OR
    #
    #    @builder
    #    @frame(arg1=..., arg2=...)
    #    def func(...):
    #        ...
    #
    def __call__(self, func_or_provider=None, **kwargs):
        if func_or_provider is not None:
            if len(kwargs) > 0:
                raise ValueError(
                    f"{self} can't be called with both a function and keywords")

            wrapper = provider_wrapper(ProtocolUpdateProvider, self)
            return wrapper(func_or_provider)
        else:
            return self.__class__(**kwargs)

    def __repr__(self):
        return f'{self.__class__.__name__}(...)'


class PicklableProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values can be serialized using the
    ``pickle`` library.
    """

    def get_fixed_file_extension(self):
        return 'pkl'

    def write(self, value, path):
        with path.open('wb') as file_:
            pickle.dump(value, file_)

    def read(self, path, extension):
        with path.open('rb') as file_:
            return pickle.load(file_)


class DillableProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values can be serialized using the
    ``dill`` library.

    This is useful for objects that can't be pickled for some reason.
    """

    def get_fixed_file_extension(self):
        return 'dill'

    def __init__(self, suppress_dill_side_effects=True):
        super(DillableProtocol, self).__init__()

        self._suppress_dill_side_effects = suppress_dill_side_effects
        self._dill = None

    def _get_dill_module(self):
        if self._dill is not None:
            return self._dill

        dill_already_imported = 'dill' in sys.modules
        self._dill = import_optional_dependency(
            'dill', purpose='the @dillable protocol')
        if not dill_already_imported and self._suppress_dill_side_effects:
            self._dill.extend(False)

        return self._dill

    def write(self, value, path):
        with path.open('wb') as file_:
            self._get_dill_module().dump(value, file_)

    def read(self, path, extension):
        with path.open('rb') as file_:
            return self._get_dill_module().load(file_)


class ParquetDataFrameProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values always have the
    ``pandas.DataFrame`` type.

    These values will be serialized to Parquet files.

    Parameters:
        check_dtypes: boolean (default: True)
            Attempt to check for column types not supported by the file format.
    """

    def __init__(self, check_dtypes=True):
        super(ParquetDataFrameProtocol, self).__init__()

        self._check_dtypes = check_dtypes

    def get_fixed_file_extension(self):
        return 'pq'

    def validate(self, value):
        assert isinstance(value, pd.DataFrame)

    def read(self, path, extension):
        with path.open('rb') as file_:
            return parquet.read_table(file_).to_pandas()

    def write(self, df, path):
        self._check_no_duplicate_cols(df)
        if self._check_dtypes:
            self._check_no_categorical_cols(df)
        with path.open('wb') as file_:
            parquet.write_table(Table.from_pandas(df), file_)

    def _check_no_duplicate_cols(self, df):
        duplicate_cols = {elem: count for elem, count in Counter(df.columns).items() if count > 1}
        if duplicate_cols:
            raise ValueError(
                oneline(f'''
                    Attempted to serialize to Parquet a dataframe which has
                    duplicate columns with the following counts:
                    {duplicate_cols}. You can fix this by dropping duplicate
                    columns with something like: ''')
                + '\n' +
                "df = df.loc[:, ~df.columns.duplicated()]")

    def _check_no_categorical_cols(self, df):
        categorical_cols = [
            col for col in df.columns
            if df[col].dtype.name == 'category'
        ]

        if categorical_cols:
            raise ValueError(oneline(f'''
                Attempted to serialize to Parquet a dataframe which has
                categorical columns: {categorical_cols!r} --
                these columns may be transformed to another type and/or
                lose some information.
                You can fix this by using
                (a) ``@frame(file_format='feather')`` to use the Feather
                format instead, or
                (b) ``@frame(check_dtypes=False)`` to ignore this check.'''))


class FeatherDataFrameProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values always have the
    ``pandas.DataFrame`` type.

    These values will be serialized to Feather files.
    """

    def get_fixed_file_extension(self):
        return 'feather'

    def validate(self, value):
        assert isinstance(value, pd.DataFrame)

    def read(self, path, extension):
        with path.open('rb') as file_:
            return pd.read_feather(file_)

    def write(self, df, path):
        with path.open('wb') as file_:
            df.to_feather(file_)


Image = import_optional_dependency('PIL.Image', raise_on_missing=False)


class ImageProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values always have the
    ``Pillow.Image`` type.

    These values will be serialized to PNG files.
    """

    def get_fixed_file_extension(self):
        return 'png'

    def validate(self, value):
        # If Image is None, then the PIL library is not present, which
        # presumably means this value is not a PIL image, and hence should fail
        # validation.
        assert Image is not None
        assert isinstance(value, Image.Image)

    def read(self, path, extension):
        with path.open('rb') as file_:
            Image = import_optional_dependency(
                'PIL.Image', purpose='the @image decorator')
            image = Image.open(file_)
            # Image.open() is lazy; if we don't call load() now, the file can
            # be closed or possibly invalidated before it actually gets read.
            image.load()
            return image

    def write(self, image, path):
        with path.open('wb') as file_:
            image.save(file_, format='png')


class NumPyProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values always have the
    ``numpy.ndarray`` type.

    These values will be serialized to .npy files.
    """

    def get_fixed_file_extension(self):
        return '.npy'

    def validate(self, value):
        assert isinstance(value, np.ndarray)

    def read(self, path, extension):
        with path.open('rb') as file_:
            return np.load(file_)

    def write(self, array, path):
        with path.open('wb') as file_:
            np.save(file_, array)


dd = import_optional_dependency('dask.dataframe', raise_on_missing=False)


class DaskProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values always have the
    ``dask.dataframe.DataFrame`` type.

    These values will be serialized a .dask.pq directory.
    """

    def get_fixed_file_extension(self):
        return 'pq.dask'

    def validate(self, value):
        # If dd is None, then dask with dataframe (i.e. dask[dataframe]) is not present,
        # which presumably means this value is not a dask dataframe, and hence should fail
        # validation.
        assert dd is not None
        assert isinstance(value, dd.DataFrame)

    def read(self, path, extension):
        dd = import_optional_dependency('dask.dataframe', purpose='the @dask decorator')
        with warnings.catch_warnings():
            warnings.filterwarnings('error', message=r".*cannot\s+autodetect\s+index.*")
            try:
                return dd.read_parquet(path)
            except RuntimeWarning as e:
                raise UnsupportedSerializedValueError(
                    f"Reading dataframe failed due to present MultiIndex: {e}")

    def write(self, df, path):
        dd.to_parquet(df, path, write_index=True)


class YamlProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values can be serialized using the
    ``PyYAML`` library.

    Parameters
    ----------
    **kwargs: keyword args for ``yaml.dump``
        E.g. ``default_flow_style``, ``encoding``, etc.
    """
    def __init__(self, **kwargs):
        super(YamlProtocol, self).__init__()
        self._kwargs = kwargs

    def get_fixed_file_extension(self):
        return 'yaml'

    def write(self, value, path):
        with path.open('w') as file_:
            yaml.dump(value, file_, **self._kwargs)

    def read(self, path, extension):
        with path.open('r') as file_:
            return yaml.safe_load(file_)


class CombinedProtocol(BaseProtocol):
    """
    Decorator generator indicating that an entity's values should be handled
    differently depending on their types.

    This protocol combines multiple protocols, and handles each value according
    to the first protocol that supports it.

    Parameters
    ----------
    subprotocols: Sequence of Protocols
        An ordered sequence of protocols to try for each value.

    Returns
    -------
    Function:
        An entity decorator.
    """

    def __init__(self, *subprotocols):
        super(CombinedProtocol, self).__init__()

        self._subprotocols = subprotocols

    def _protocol_for_value(self, value):
        for protocol in self._subprotocols:
            if protocol.value_is_valid(value):
                return protocol
        return None

    def _protocol_for_extension(self, extension):
        for protocol in self._subprotocols:
            if protocol.can_read_file_extension(extension):
                return protocol
        return None

    def can_read_file_extension(self, extension):
        return self._protocol_for_extension(extension) is not None

    def file_extension_for_value(self, value):
        return self._protocol_for_value(value).file_extension_for_value(value)

    def validate(self, value):
        if self._protocol_for_value(value) is not None:
            return

        if not self._subprotocols:
            raise AssertionError('No subprotocols defined')
        else:
            self._subprotocols[-1].validate(value)

    def read(self, path, extension):
        if not self.can_read_file_extension(extension):
            raise ValueError(
                "This protocol doesn't know how to read a file with "
                f"extension {extension!r}")
        return self._protocol_for_extension(extension).read(path, extension)

    def write(self, value, path):
        self._protocol_for_value(value).write(value, path)

    def __repr__(self):
        return f'CombinedProtocol{tuple(self._subprotocols)!r}'


class TypeProtocol(PicklableProtocol):
    """
    Indicates that an entity's values will always have a specific type.

    Parameters
    ----------
    type_: Type
        The expected type for this entity.

    Returns
    -------
    Function:
        A entity decorator.
    """

    def __init__(self, type_):
        super(TypeProtocol, self).__init__()

        self._type = type_

    def validate(self, value):
        assert isinstance(value, self._type)

    def __repr__(self):
        return f'TypeProtocol({self._type.__name__})'


class EnumProtocol(PicklableProtocol):
    """
    Indicates that an entity will only have one of a specific set of values.

    Parameters
    ----------
    allowed_values: Sequence of objects
        The expected possible values for this entity.

    Returns
    -------
    Function:
        An entity decorator.
    """

    def __init__(self, *allowed_values):
        super(EnumProtocol, self).__init__()

        self._allowed_values = allowed_values

    def validate(self, value):
        assert value in self._allowed_values

    def __repr__(self):
        return f'EnumProtocol{tuple(self._allowed_values)!r}'
