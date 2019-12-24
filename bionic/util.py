'''
Miscellaneous utility functions.
'''

import logging
from collections import defaultdict
from hashlib import sha256
from binascii import hexlify
import subprocess
import warnings

from .optdep import import_optional_dependency, oneline


def n_present(*items):
    "Returns the number of non-None arguments."
    return sum(item is not None for item in items)


def check_exactly_one_present(**kwargs):
    if not n_present(list(kwargs.values())) == 1:
        args_str = ', '.join(
            f'{name}={value!r}'
            for name, value in kwargs.items())
        raise ValueError(oneline(f'''
            Exactly one of {tuple(kwargs.keys())} should be present;
            got {args_str}'''))


def check_at_most_one_present(**kwargs):
    if n_present(list(kwargs.values())) > 1:
        args_str = ', '.join(
            f'{name}={value!r}'
            for name, value in kwargs.items())
        raise ValueError(oneline(f'''
            At most one of {tuple(kwargs.keys())} should be present;
            got {args_str}'''))


def group_pairs(items):
    "Groups an even-sized list into contiguous pairs."
    if len(items) % 2 != 0:
        raise ValueError(f"Expected even number of items, got {items!r}")
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


def hash_to_hex(bytestring, n_bytes=None):
    hash_ = sha256()
    hash_.update(bytestring)
    hex_str = hexlify(hash_.digest()).decode('utf-8')

    if n_bytes is not None:
        n_chars = n_bytes * 2
        available_chars = len(hex_str)
        if n_chars > available_chars:
            raise ValueError(oneline(f'''
                Can't keep {n_bytes} bytes;
                we only have {available_chars // 2}'''))
        hex_str = hex_str[:n_chars]

    return hex_str


_cached_gcs_client = None


def get_gcs_client_without_warnings(cache_value=True):
    # TODO This caching saves a lot of time, especially in tests.  But it would
    # be better if Bionic were able to re-use its in-memory cache when creating
    # new flows, instead of resetting the cache each time.
    if cache_value:
        global _cached_gcs_client
        if _cached_gcs_client is None:
            _cached_gcs_client =\
                get_gcs_client_without_warnings(cache_value=False)
        return _cached_gcs_client

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
    prefix = f"gs://{bucket}"
    path = dst[len(prefix) + 1:]

    client = get_gcs_client_without_warnings()
    blob = client.get_bucket(bucket).blob(path)
    blob.upload_from_filename(src)


def num_as_bytes(n):
    """
    Encodes an integer in UTF-8 bytes.
    """
    return str(n).encode('utf-8')


def read_hashable_bytes_from_file_or_dir(path):
    """
    Reads the contents of a file or directory (and all nested
    files/directories) into a single byte array.  This function is intended to
    generate the input to a hash function, so it includes some extra metadata
    to reduce the chance of collisions.
    """
    if not path.exists():
        raise ValueError(f"{path!r} doesn't exist")
    elif path.is_file():
        return b'F' + num_as_bytes(path.stat().st_size) + b':' +\
            path.read_bytes()
    elif path.is_dir():
        # We could just concatenate all the file byte strings together, but
        # since we expect to hash this, it'd be nice to avoid returning the
        # same value for different file structures.  For example, if a
        # directory contains one file with contents 'AB', we'd like that to
        # look different from two files with contents 'A' and 'B'.  To
        # accomplish this, we prefix each type of data with a letter and the
        # length of the data.
        sub_paths = list(sorted(path.iterdir()))
        return b'D' + num_as_bytes(len(sub_paths)) + b':' + b':'.join(
            b'N' + num_as_bytes(len(sub_path.name)) + b':' +
            sub_path.name + b':' +
            read_hashable_bytes_from_file_or_dir(sub_path)
            for sub_path in sub_paths
        )
    else:
        raise ValueError(oneline(f'''
            {path!r} is neither a file nor a directory;
            not sure what to do with it!'''))


def ensure_parent_dir_exists(path):
    path.parent.mkdir(parents=True, exist_ok=True)


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
        return f'{self.__class__.__name__}({self.__items!r})'


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
        return f'{self.__class__.__name__}({self.__values_by_key!r})'


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


class FileCopier(object):
    """
    A wrapper for a Path object, exposing a `copy` method that will copy the underlying
    file to a local or cloud destination


    Parameters
    ----------
    src_file_path: Path
        A path to a file.
    """
    def __init__(self, src_file_path):
        self.src_file_path = src_file_path

    def copy(self, destination):
        """
        Copies file that FileCopier represents to `destination`

        This supports both local and GCS destinations.  For the former, we follow cp's conventions
        and for the latter we follow gsutil cp's conventions.  For example, trying to copy a
        file locally to a non-existent directory will fail.

        Parameters
        ----------

        destination: Path or str
            Where to copy the underlying file
        """

        #  handle gcs
        if str(destination).startswith('gs://'):
            subprocess.check_call(
                ['gsutil', '-mq', 'cp', '-R', str(self.src_file_path), str(destination)]
            )
        else:
            subprocess.check_call(['cp', '-R', str(self.src_file_path), str(destination)])


def init_basic_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format='%(asctime)s %(levelname)s %(name)16s: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )
