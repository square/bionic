'''
Miscellaneous utility functions.
'''
from __future__ import division

import logging
from builtins import zip, object
from collections import defaultdict
from hashlib import sha256
from binascii import hexlify
import warnings

from .optdep import import_optional_dependency


def n_present(*items):
    "Returns the number of non-None arguments."
    return sum(item is not None for item in items)


def check_exactly_one_present(**kwargs):
    if not n_present(list(kwargs.values())) == 1:
        raise ValueError("Exactly one of %s should be present; got %s" % (
            tuple(kwargs.keys()),
            ', '.join(
                '%s=%r' % (name, value)
                for name, value in kwargs.items())
        ))


def group_pairs(items):
    "Groups an even-sized list into contiguous pairs."
    if len(items) % 2 != 0:
        raise ValueError("Expected even number of items, got %r" % (items,))
    return list(zip(items[::2], items[1::2]))


def groups_dict(values, keyfunc):
    '''
    Like itertools.groupby, but doesn't require the values to be already
    sorted.
    '''
    valuelists_by_key = defaultdict(list)
    for value in values:
        valuelists_by_key[keyfunc(value)].append(value)
    return dict(valuelists_by_key)


def color(code, text):
    return '\033[%sm%s\033[0m' % (code, text)


def hash_to_hex(bytestring, n_bytes=None):
    hash_ = sha256()
    hash_.update(bytestring)
    hex_str = hexlify(hash_.digest()).decode('utf-8')

    if n_bytes is not None:
        n_chars = n_bytes * 2
        available_chars = len(hex_str)
        if n_chars > available_chars:
            raise ValueError("Can't keep %d bytes; we only have %d" % (
                n_bytes, available_chars // 2))
        hex_str = hex_str[:n_chars]

    return hex_str


def get_gcs_client_without_warnings():
    gcs = import_optional_dependency(
        'google.cloud.storage', purpose='caching to GCS')

    with warnings.catch_warnings():
        # Google's SDK warns if you use end user credentials instead of a
        # service account.  I think this warning is intended for production
        # server code, where you don't want GCP access to be tied to a
        # particular user.  However, this code is intended to be run by
        # individuals, so using end user credentials seems appropriate.
        # Hence, we'll suppress this warning.
        warnings.filterwarnings(
            'ignore',
            'Your application has authenticated using end user credentials'
        )
        return gcs.Client()


def copy_to_gcs(src, dst):
    """ Copy a local file at src to GCS at dst
    """
    bucket = dst.replace('gs://', '').split('/')[0]
    prefix = "gs://{}".format(bucket)
    path = dst[len(prefix) + 1:]

    client = get_gcs_client_without_warnings()
    blob = client.get_bucket(bucket).blob(path)
    blob.upload_from_filename(src)


class ImmutableSequence(object):
    def __init__(self, items):
        self.__items = tuple(items)

    def __getitem__(self, key):
        return self.__items.__getitem__(key)

    def __iter__(self):
        return self.__items.__iter__()

    def __len__(self):
        return self.__items.__len__()

    def __contains__(self, value):
        return self.__items.__contains__(value)

    def __hash__(self):
        return self.__items.__hash__()

    def __eq__(self, seq):
        if not isinstance(seq, ImmutableSequence):
            return False
        return self.__items.__eq__(seq.__items)

    def __ne__(self, seq):
        if not isinstance(seq, ImmutableSequence):
            return True
        return self.__items.__ne__(seq.__items)

    def __lt__(self, seq):
        return self.__items.__lt__(seq.__items)

    def __gt__(self, seq):
        return self.__items.__gt__(seq.__items)

    def __le__(self, seq):
        return self.__items.__le__(seq.__items)

    def __ge__(self, seq):
        return self.__items.__ge__(seq.__items)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.__items)


class ImmutableMapping(ImmutableSequence):
    def __init__(self, values_by_key):
        super(ImmutableMapping, self).__init__(
            tuple(sorted(values_by_key.items())))
        self.__values_by_key = dict(values_by_key)

    def __getitem__(self, key):
        return self.__values_by_key.__getitem__(key)

    def __iter__(self):
        return self.__values_by_key.__iter__()

    def __contains__(self, value):
        return self.__values_by_key.__contains__(value)

    def get(self, key):
        return self.__values_by_key.get(key)

    def keys(self):
        return list(self.__values_by_key.keys())

    def values(self):
        return list(self.__values_by_key.values())

    def items(self):
        return list(self.__values_by_key.items())

    def iterkeys(self):
        return iter(self.__values_by_key.keys())

    def itervalues(self):
        return iter(self.__values_by_key.values())

    def iteritems(self):
        return iter(self.__values_by_key.items())

    def __hash__(self):
        return super(ImmutableMapping, self).__hash__()

    def __eq__(self, seq):
        if not isinstance(seq, ImmutableMapping):
            return False
        return self.__values_by_key.__eq__(seq.__values_by_key)

    def __ne__(self, seq):
        if not isinstance(seq, ImmutableMapping):
            return True
        return self.__values_by_key.__ne__(seq.__values_by_key)

    def __repr__(self):
        return '%s(%r)' % (self.__class__.__name__, self.__values_by_key)


# TODO I'm not sure if we'll end up needing this or not.
class ExtensibleLogger(object):
    def __init__(self, logger):
        self._logger = logger

    # Subclasses should call this.
    def base_log(self, level, msg, *args, **kwargs):
        self._logger.log(level, msg, *args, **kwargs)

    # Subclasses should override this.
    def custom_log(self, level, msg, *args, **kwargs):
        self.base_log(level, msg, *args, **kwargs)

    # Internal methods.
    def _custom_log_if_enabled(self, level, msg, *args, **kwargs):
        if not self.isEnabledFor(level):
            return
        self.custom_log(level, msg, *args, **kwargs)

    # User-facing methods.
    def isEnabledFor(self, level):
        return self._logger.isEnabledFor(level)

    def log(self, level, msg, *args, **kwargs):
        self._custom_log_if_enabled(level, msg, *args, **kwargs)

    def debug(self, msg, *args, **kwargs):
        self._custom_log_if_enabled(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg, *args, **kwargs):
        self._custom_log_if_enabled(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg, *args, **kwargs):
        self._custom_log_if_enabled(logging.WARNING, msg, *args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        self.warning(msg, *args, **kwargs)

    def error(self, msg, *args, **kwargs):
        self._custom_log_if_enabled(logging.ERROR, msg, *args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        self._custom_log_if_enabled(logging.CRITICAL, msg, *args, **kwargs)

    def exception(self, msg, *args, **kwargs):
        self._custom_log_if_enabled(
            logging.ERROR, msg, *args, exc_info=True, **kwargs)


def init_basic_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)16s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )


def init_matplotlib():
    '''
    Attempts to safely set up an appropriate matplotlib backend.  (The default
    backend on OS X will crash on Python 2 if you aren't using the system
    Python.)

    This only has an effect if you call it before matplotlib.pyplot is
    imported.
    '''
    matplotlib = import_optional_dependency('matplotlib', purpose='plotting')

    if matplotlib.get_backend() == 'MacOSX':
        matplotlib.use('TkAgg', warn=False)
    else:
        matplotlib.use('Agg', warn=False)
