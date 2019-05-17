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

    def is_valid(self, value):
        try:
            self.validate(value)
        except Exception:
            return False
        return True

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
    file_suffix = '.pkl'

    def write(self, value, file_):
        pickle.dump(value, file_)

    def read(self, file_):
        return pickle.load(file_)


class DillableProtocol(BaseProtocol):
    file_suffix = '.dill'

    def __init__(self, suppress_dill_side_effects=True, **base_kwargs):
        super(DillableProtocol, self).__init__(**base_kwargs)

        dill_already_imported = 'dill'in sys.modules
        import dill
        if not dill_already_imported and suppress_dill_side_effects:
            dill.extend(False)

        self._dill = dill

    def write(self, value, file_):
        self._dill.dump(value, file_)

    def read(self, file_):
        return self._dill.load(file_)


class DataFrameProtocol(BaseProtocol):
    def __init__(self):
        super(DataFrameProtocol, self).__init__()

        self.file_suffix = '.pq'

    def validate(self, df):
        assert isinstance(df, pd.DataFrame), type(df)
        # TODO If there are any kinds of dataframe that can't be written to
        # Parquet, this might be a good place to check them.

    def read(self, file_):
        return parquet.read_table(file_).to_pandas()

    def write(self, df, file_):
        parquet.write_table(Table.from_pandas(df), file_)
