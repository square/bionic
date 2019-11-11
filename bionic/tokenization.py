'''
Contains a tokenize() function which can be used to convert arbitrary values
into nice strings, suitable for use as filenames.
'''

from .util import hash_to_hex


def char_range(first, last):
    "Return a list of all the characters from first to last, inclusive."
    return [chr(i) for i in range(ord(first), ord(last) + 1)]


CLEAN_CHARS = set(
    char_range('a', 'z') + char_range('A', 'Z') + char_range('0', '9') +
    ['_', '-', '.'])
MAX_CLEAN_STR_LEN = 32


def clean_str(string):
    "Converts an arbitary string to one that could be used as a filename."
    cleaned = ''.join((c if c in CLEAN_CHARS else '.') for c in string)
    # Some filesystems are case insensitive, so we don't want uppercase
    # letters.
    cleaned = cleaned.lower()
    # Some filesystems treat files differently if they start with a period, so
    # let's avoid that.
    if cleaned.startswith('.'):
        cleaned = '_' + cleaned
    if len(cleaned) > MAX_CLEAN_STR_LEN:
        head_len = (MAX_CLEAN_STR_LEN // 2) - 1
        tail_len = MAX_CLEAN_STR_LEN - (head_len + 3)
        cleaned = cleaned[:head_len] + '...' + cleaned[-tail_len:]
    return cleaned


# When hashing values for tokens, we'll hash down to 5 bytes (10 hex chars).
# The reasoning is:
# - we want to support up to 1e6 distinct values
# - to avoid collisions, we need a hash space of 1e6 squared, or 1e12
# - that's 36 bits
# - rounding up, that's 5 bytes
# I picked 1e6 arbitrarily; the hash is only used when two value have the same
# "clean string" value OR when they can't be converted to strings at all, but
# that will include things like dicts of hyperparameter values.
HASH_LEN = 5


# TODO: add optional directory parameter for where to write/read from
def tokenize(value, serialize_func=None):
    '''
    Convert an arbitrary value to a nice, unique string that could be used as a
    filename.  If a serialization function is provided, the value will be
    serialized and hashed.  Otherwise it will be converted to a string; if that
    string is not suitable for a filename, it will be cleaned and a hash will
    be appended.
    '''

    if serialize_func is not None:
        bytestring = serialize_func(value)
        token = hash_to_hex(bytestring, HASH_LEN)
    else:
        value_str = str(value)
        token = clean_str(value_str)
        if token != value_str:
            token += '_' + hash_to_hex(value_str.encode('utf-8'), HASH_LEN)

    return token
