'''
Contains bionic's concept of a "protocol": an object that specifies how to
handle certain types of data.  A protocol is similar to a Python type, but
might be more or less specific.  For example, PicklableProtocol can be used
for any type that can be safely pickled.  On the other hand, DataFrameProtocol
takes additional arguments that constrain the structure of the DataFrames it
handles.  Protocols specify how to serialize, deserialize, and validate data.

This module contains a BaseProtocol class and various subclasses.
'''

import pickle
import sys

from pyarrow import parquet, Table
import pandas as pd

from resource import resource_wrapper, AttrUpdateResource
import tokenization


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
        str, bytes, unicode,
        int, long, float,
    }

    def tokenize(self, value):
        if type(value) in self.SIMPLE_TYPES:
            return tokenization.tokenize(value)
        else:
            return tokenization.tokenize(value, self.write)

    def __call__(self, func_or_resource):
        wrapper = resource_wrapper(AttrUpdateResource, 'protocol', self)
        return wrapper(func_or_resource)

    def __repr__(self):
        return '%s(...)' % (self.__class__.__name__)


class PicklableProtocol(BaseProtocol):
    def get_fixed_file_extension(self):
        return 'pkl'

    def write(self, value, file_):
        pickle.dump(value, file_)

    def read(self, file_, extension):
        return pickle.load(file_)


class DillableProtocol(BaseProtocol):
    def get_fixed_file_extension(self):
        return 'dill'

    def __init__(self, suppress_dill_side_effects=True, **base_kwargs):
        super(DillableProtocol, self).__init__(**base_kwargs)

        dill_already_imported = 'dill'in sys.modules
        import dill
        if not dill_already_imported and suppress_dill_side_effects:
            dill.extend(False)

        self._dill = dill

    def write(self, value, file_):
        self._dill.dump(value, file_)

    def read(self, file_, extension):
        return self._dill.load(file_)


class DataFrameProtocol(BaseProtocol):
    def __init__(self):
        super(DataFrameProtocol, self).__init__()

    def get_fixed_file_extension(self):
        return 'pq'

    def validate(self, value):
        assert isinstance(value, pd.DataFrame)

    def read(self, file_, extension):
        return parquet.read_table(file_).to_pandas()

    def write(self, df, file_):
        parquet.write_table(Table.from_pandas(df), file_)


class CombinedProtocol(BaseProtocol):
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
        return self._protocol_for_extension(extension).read(file_, extension)

    def write(self, value, file_):
        self._protocol_for_value(value).write(value, file_)

    def __repr__(self):
        return 'CombinedProtocol%r' % (tuple(self._subprotocols),)


class TypeProtocol(PicklableProtocol):
    def __init__(self, type_):
        super(TypeProtocol, self).__init__()

        self._type = type_

    def validate(self, value):
        assert isinstance(value, self._type)

    def __repr__(self):
        return 'TypeProtocol(%s)' % self.type.__name__


class EnumProtocol(PicklableProtocol):
    def __init__(self, *allowed_values):
        super(EnumProtocol, self).__init__()

        self._allowed_values = allowed_values

    def validate(self, value):
        assert value in self._allowed_values

    def __repr__(self):
        return 'EnumProtocol%r' % (tuple(self._allowed_values),)
