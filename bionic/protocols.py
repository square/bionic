'''
Contains bionic's concept of a "protocol": an object that specifies how to
handle certain types of data.  A protocol is similar to a Python type, but
might be more or less specific.  For example, PicklableProtocol can be used
for any type that can be safely pickled.  On the other hand, DataFrameProtocol
takes additional arguments that constrain the structure of the DataFrames it
handles.  Protocols specify how to serialize, deserialize, and validate data.

This module contains a BaseProtocol class and various subclasses.
'''
from __future__ import absolute_import

from builtins import object
import pickle
import sys

import numpy as np
from pyarrow import parquet, Table
import pandas as pd

from .provider import provider_wrapper, ProtocolUpdateProvider
from .optdep import import_optional_dependency
from . import tokenization


def check_is_like_protocol(obj):
    for method_name in 'validate', 'read', 'write':
        if not hasattr(obj, method_name):
            raise ValueError(
                "Expected %r to be a kind of Protocol, but didn't find "
                "expected method %s " % (obj, method_name))


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

    def write(self, value, file_):
        raise NotImplementedError()

    def read(self, file_):
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
            return tokenization.tokenize(value, self.write)

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
                    "%s can't be called with both a function and keywords" %
                    self)

            wrapper = provider_wrapper(ProtocolUpdateProvider, self)
            return wrapper(func_or_provider)
        else:
            return self.__class__(**kwargs)

    def __repr__(self):
        return '%s(...)' % (self.__class__.__name__)


class PicklableProtocol(BaseProtocol):
    """
    Decorator indicating that an entity's values can be serialized using the
    ``pickle`` library.
    """

    def get_fixed_file_extension(self):
        return 'pkl'

    def write(self, value, file_):
        pickle.dump(value, file_)

    def read(self, file_, extension):
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

    def write(self, value, file_):
        self._get_dill_module().dump(value, file_)

    def read(self, file_, extension):
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

    def read(self, file_, extension):
        return parquet.read_table(file_).to_pandas()

    def write(self, df, file_):
        if self._check_dtypes:
            self._check_no_categorical_cols(df)
        parquet.write_table(Table.from_pandas(df), file_)

    def _check_no_categorical_cols(self, df):
        categorical_cols = [
            col for col in df.columns
            if df[col].dtype.name == 'category'
        ]

        if categorical_cols:
            raise ValueError(
                "Attempted to serialize to Parquet a dataframe which has "
                "categorical columns: %r -- these columns may be transformed "
                "to another type and/or lose some information.  You can fix "
                "this by using (a) ``@frame(file_format='feather')`` to use "
                "the Feather format instead, or (b) "
                "``@frame(check_dtypes=False)`` to ignore this check." % (
                    categorical_cols))


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

    def read(self, file_, extension):
        return pd.read_feather(file_)

    def write(self, df, file_):
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

    def read(self, file_, extension):
        Image = import_optional_dependency(
            'PIL.Image', purpose='the @image decorator')
        image = Image.open(file_)
        # Image.open() is lazy; if we don't call load() now, the file can
        # be closed or possibly invalidated before it actually gets read.
        image.load()
        return image

    def write(self, image, file_):
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

    def read(self, file_, extension):
        return np.load(file_)

    def write(self, array, file_):
        np.save(file_, array)


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

    def read(self, file_, extension):
        if not self.can_read_file_extension(extension):
            raise ValueError(
                "This protocol doesn't know how to read a file with "
                "extension %r" % extension)
        return self._protocol_for_extension(extension).read(file_, extension)

    def write(self, value, file_):
        self._protocol_for_value(value).write(value, file_)

    def __repr__(self):
        return 'CombinedProtocol%r' % (tuple(self._subprotocols),)


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
        return 'TypeProtocol(%s)' % self._type.__name__


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
        return 'EnumProtocol%r' % (tuple(self._allowed_values),)
