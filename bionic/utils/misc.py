"""
Miscellaneous utility functions.
"""

from collections import defaultdict
from hashlib import sha256
from binascii import hexlify
import re
import threading

import logging

logger = logging.getLogger(__name__)


def oneline(string):
    """
    Shorten a multiline string into a single line, by replacing all newlines
    (and their surrounding whitespace) into single spaces. Convenient for
    rendering error messages, which are most easily written as multiline
    string literals but more readable as single-line strings.
    """
    return " ".join(
        substring.strip() for substring in string.split("\n") if substring.split()
    )


def n_present(*items):
    "Returns the number of non-None arguments."
    return sum(item is not None for item in items)


def check_exactly_one_present(**kwargs):
    if not n_present(list(kwargs.values())) == 1:
        args_str = ", ".join(f"{name}={value!r}" for name, value in kwargs.items())
        raise ValueError(
            oneline(
                f"""
            Exactly one of {tuple(kwargs.keys())} should be present;
            got {args_str}"""
            )
        )


def check_at_most_one_present(**kwargs):
    if n_present(list(kwargs.values())) > 1:
        args_str = ", ".join(f"{name}={value!r}" for name, value in kwargs.items())
        raise ValueError(
            oneline(
                f"""
            At most one of {tuple(kwargs.keys())} should be present;
            got {args_str}"""
            )
        )


def group_pairs(items):
    "Groups an even-sized list into contiguous pairs."
    if len(items) % 2 != 0:
        raise ValueError(f"Expected even number of items, got {items!r}")
    return list(zip(items[::2], items[1::2]))


def groups_dict(values, keyfunc):
    """
    Like itertools.groupby, but doesn't require the values to be already
    sorted.
    """
    valuelists_by_key = defaultdict(list)
    for value in values:
        valuelists_by_key[keyfunc(value)].append(value)
    return dict(valuelists_by_key)


def single_element(iterable):
    "Takes an iterable with a single element and returns that element."
    items = list(iterable)
    if len(items) != 1:
        raise ValueError(
            oneline(
                f"""
            Expected a sequence with exactly one item;
            got {len(items)} items."""
            )
        )
    return items[0]


def single_unique_element(iterable):
    """
    Takes an iterable with exactly one unique element and returns that element.

    Equivalent to `single_element(set(iterable))`.
    """
    return single_element(set(iterable))


def hash_to_hex(bytestring, n_bytes=None):
    hash_ = sha256()
    hash_.update(bytestring)
    hex_str = hexlify(hash_.digest()).decode("utf-8")

    if n_bytes is not None:
        n_chars = n_bytes * 2
        available_chars = len(hex_str)
        if n_chars > available_chars:
            raise ValueError(
                oneline(
                    f"""
                Can't keep {n_bytes} bytes;
                we only have {available_chars // 2}"""
                )
            )
        hex_str = hex_str[:n_chars]

    return hex_str


def num_as_bytes(n):
    """
    Encodes an integer in UTF-8 bytes.
    """
    return str(n).encode("utf-8")


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
        # TODO This file could be large, so it would be better not to read the whole
        # thing into memory. We could probably implement this whole function as a
        # streaming algorithm instead.
        return b"F" + num_as_bytes(path.stat().st_size) + b":" + path.read_bytes()
    elif path.is_dir():
        # We could just concatenate all the file byte strings together, but
        # since we expect to hash this, it'd be nice to avoid returning the
        # same value for different file structures.  For example, if a
        # directory contains one file with contents 'AB', we'd like that to
        # look different from two files with contents 'A' and 'B'.  To
        # accomplish this, we prefix each type of data with a letter and the
        # length of the data.
        sub_paths = list(sorted(path.iterdir()))
        return (
            b"D"
            + num_as_bytes(len(sub_paths))
            + b":"
            + b":".join(
                b"N"
                + num_as_bytes(len(sub_path.name))
                + b":"
                + sub_path.name.encode("UTF-8")
                + b":"
                + read_hashable_bytes_from_file_or_dir(sub_path)
                for sub_path in sub_paths
            )
        )
    else:
        raise ValueError(
            oneline(
                f"""
            {path!r} is neither a file nor a directory;
            not sure what to do with it!"""
            )
        )


def hash_simple_obj_to_hex(obj):
    """
    Generates a hash digest of an object, as a hex string.  The object must
    be a "simple" type, i.e., of one of the following types: None, text, bytes,
    int, or a list or dict of simple types.
    """

    hash_ = sha256()
    try:
        update_hash(hash_, obj)
    except ValueError as e:
        raise ValueError(f"{e} (full object was {obj!r})")
    return hash_.hexdigest()


def update_hash(hash_, obj):
    if obj is None:
        hash_.update(b"N")
    elif isinstance(obj, str):
        hash_.update(b"S")
        hash_.update(obj.encode("utf8"))
    elif isinstance(obj, bytes):
        hash_.update(b"B")
        hash_.update(obj.encode("utf8"))
    elif isinstance(obj, int):
        hash_.update(b"I")
        hash_.update(str(obj).encode("utf8"))
    elif isinstance(obj, list):
        hash_.update(b"L")
        for item in obj:
            update_hash(hash_, item)
    elif isinstance(obj, dict):
        hash_.update(b"D")
        for key, value in obj.items():
            update_hash(hash_, key)
            update_hash(hash_, value)
    else:
        raise ValueError(f"Unable to hash object {obj!r} of type {type(obj)!r}")


# This default chunk size is just a guess; I haven't benchmarked anything.
def hexdigest_from_path(path, hash_=None, chunk_size=4192):
    if hash_ is None:
        hash_ = sha256()
    with path.open("rb") as file_:
        while True:
            chunk_bytes = file_.read(chunk_size)
            if len(chunk_bytes) == 0:
                break
            hash_.update(chunk_bytes)
    return hash_.hexdigest()


# Matches a line that looks like the start of a new paragraph.
NEW_PARAGRAPH_PATTERN = re.compile(
    r"$"  # An empty line.
    r"|[\-\+\*]"  # A bullet point in an unordered list.
    r"|(\d+[\.\)] )"  # A numbered list item.
    r"|([a-z][\.\)] )"  # A letter list item.
)


def rewrap_docstring(docstring):
    """
    Reformats a Python docstring to read more nicely as freeform text (for example, in
    an SVG tooltip).

    Docstrings are generally written with a fixed line width, so they have
    line breaks that look weird in contexts that expect continuous lines of
    text. This function replaces collapses the line breaks into regular
    spaces, except when it identifies text that looks like the start of a new
    paragraph:
    - An empty line.
    - A line starting with bullet point ("-", "+", "*").
    - A line starting with an ordered list field (a lowercase letter or
    number followed by "." or ")").
    """
    docstring = docstring.strip()
    if not docstring:
        return ""

    lines = docstring.split("\n")
    grouped_line_lists = [[]]
    for line in lines:
        line = line.strip()
        if NEW_PARAGRAPH_PATTERN.match(line):
            grouped_line_lists.append([])

        if line:
            grouped_line_lists[-1].append(line)

    paragraphs = [" ".join(line_list) for line_list in grouped_line_lists]
    return "\n".join(paragraphs)


class ImmutableSequence:
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
        return f"{self.__class__.__name__}({self.__items!r})"


class ImmutableMapping(ImmutableSequence):
    def __init__(self, values_by_key):
        super(ImmutableMapping, self).__init__(tuple(sorted(values_by_key.items())))
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
        return f"{self.__class__.__name__}({self.__values_by_key!r})"


def init_basic_logging(level=logging.INFO):
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)16s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


class SynchronizedSet:
    """
    A set-like class that only exposes add() and contains() method.
    The add() operation returns whether a new value was added and is
    atomic.
    """

    def __init__(self):
        self.values = set()
        self.lock = threading.Lock()

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state["lock"]
        return state

    def __setstate__(self, state):
        # Restore instance attributes.
        self.__dict__.update(state)
        # Restore the lock.
        self.lock = threading.Lock()

    def add(self, value):
        """
        Adds the value and returns True if the value isn't present in the
        set. Returns False if the set already contains the value.
        """
        with self.lock:
            if self.contains(value):
                return False
            self.values.add(value)
            return True

    def contains(self, value):
        return value in self.values
