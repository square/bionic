"""
This module contains the CodeHasher class, which hashes code objects
into values that uniquely identify those objects.

Bionic uses the CodeHasher class to detect any changes to entity
functions. When an entity function changes, the entity value may also
change, which requires Bionic to invalidate the cache and compute the
entity again.

This class is only used when versioning mode is set to "auto" or
"assisted". Since the logic doesn't cover every type of code change,
like changes to referenced classes, these versioning modes are still
experimental. When we can detect all kinds of code changes, we will
make "auto" mode the default behavior.

Since Bionic uses these encodings to detect changes, any changes to
encoding will make caching backwards-incompatible. If you make any
changes to this module that can change the encoding, update
CACHE_SCHEMA_VERSION to update cache scheme.
"""

from enum import Enum
import hashlib
import inspect
import warnings

from .code_references import (
    get_code_context,
    get_referenced_objects,
    ReferenceProxy,
)
from .utils.misc import oneline
from .utils.reload import is_internal_file


PREFIX_SEPARATOR = b"$"


# List of things we should do before releasing Smart Caching:
# - dedup references
# - caching individual object hashes for a CodeHasher run
# - hash classes
# - Throw an exception for unhandled bytecode instruction type
# - investigate if we can hash module or package versions
# - verify that we hash all Python constant types
# - version.suppress_bytecode_warnings TODO
# - skip and warn for referenced code objects


class CodeHasher:
    """
    This class hashes code objects into values that uniquely identify
    those objects. If two code objects have different behavior, they
    should ideally have different hashes. However, for now, we only
    examine the function bytecode and associated constants. We will add
    more functionality like checking function references and any
    changes to class objects.

    Since this class doesn't examine the references and class objects,
    it doesn't actually achieve the ideal behavior we described above.
    """

    def __init__(self, suppress_warnings):
        self._suppress_warnings = suppress_warnings
        # This is used to detect circular references.
        self._object_depths_by_id = {}
        # This caches the hashed values for objects we have already
        # hashed.
        self._obj_hash_digests_by_id = {}
        # This stores the objects that we have hashed. If we don't
        # store the objects, the objects can get garbage-collected,
        # and new objects can reuse the same object id as the
        # garbage-collected objects. Doing so ensures that the ids
        # for the hashes we cache are not reused.
        # See this stack overflow post for more details:
        # https://stackoverflow.com/questions/35173479/why-do-different-methods-of-same-object-have-the-same-id
        self._encountered_objects_by_id = {}

    @classmethod
    def hash(cls, obj, suppress_warnings=False):
        hasher = cls(suppress_warnings)
        return hasher.hash_for_obj(obj)

    def hash_for_obj(self, obj):
        return str(self._check_and_hash(obj))

    def _check_and_hash(self, obj, code_context=None):
        """
        Checks for circular references and cached hashes before calling
        the _just_hash method, which does the actual encoding and
        returns the hashed value of the `obj`.
        """
        obj_id = id(obj)
        self._encountered_objects_by_id[obj_id] = obj

        # If the hash is already cached, return the cached value.
        if obj_id in self._obj_hash_digests_by_id:
            return self._obj_hash_digests_by_id[obj_id]

        # If the obj is already being hashed, break the circular ref by
        # analyzing the depth of the value instead.
        elif obj_id in self._object_depths_by_id:
            hash_accumulator = hashlib.new("md5")
            obj_depth = self._object_depths_by_id[obj_id]
            add_to_hash(
                hash_accumulator=hash_accumulator,
                type_prefix=TypePrefix.CIRCULAR_REF,
                obj_bytes=str(obj_depth).encode(),
            )
            obj_hash_digest_in_bytes = hash_accumulator.digest()

        # Compute the hash otherwise.
        else:
            self._object_depths_by_id[obj_id] = len(self._object_depths_by_id)
            obj_hash_digest_in_bytes = self._just_hash(obj, code_context)
            del self._object_depths_by_id[obj_id]

        self._obj_hash_digests_by_id[obj_id] = obj_hash_digest_in_bytes
        return obj_hash_digest_in_bytes

    def _just_hash(self, obj, code_context):
        hash_accumulator = hashlib.new("md5")
        self._update_hash(hash_accumulator, obj, code_context)
        return hash_accumulator.digest()

    def _update_hash(self, hash_accumulator, obj, code_context):
        """
        Contains the logic that analyzes the objects and encodes them
        into bytes that are added to the hash_accumulator.
        """
        if isinstance(obj, bytes):
            add_to_hash(hash_accumulator, type_prefix=TypePrefix.BYTES, obj_bytes=obj)

        elif isinstance(obj, bytearray):
            add_to_hash(
                hash_accumulator, type_prefix=TypePrefix.BYTEARRAY, obj_bytes=obj
            )

        elif obj is None:
            add_to_hash(hash_accumulator, type_prefix=TypePrefix.NONE)

        elif isinstance(obj, int):
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.INT,
                obj_bytes=str(obj).encode(),
            )

        elif isinstance(obj, float):
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.FLOAT,
                obj_bytes=str(obj).encode(),
            )

        elif isinstance(obj, str):
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.STRING,
                obj_bytes=obj.encode(),
            )

        elif isinstance(obj, bool):
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.BOOL,
                obj_bytes=str(obj).encode(),
            )

        elif isinstance(obj, (list, set, tuple)):
            if isinstance(obj, list):
                type_prefix = TypePrefix.LIST
            elif isinstance(obj, set):
                type_prefix = TypePrefix.SET
            else:
                type_prefix = TypePrefix.TUPLE
            obj_bytes = str(len(obj)).encode()
            add_to_hash(
                hash_accumulator,
                type_prefix=type_prefix,
                obj_bytes=obj_bytes,
            )
            for elem in obj:
                add_to_hash(
                    hash_accumulator,
                    type_prefix=TypePrefix.HASH,
                    obj_bytes=self._check_and_hash(elem, code_context),
                )

        elif isinstance(obj, dict):
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.DICT,
                obj_bytes=str(len(obj)).encode(),
            )
            for key, elem in obj.items():
                add_to_hash(
                    hash_accumulator,
                    type_prefix=TypePrefix.HASH,
                    obj_bytes=self._check_and_hash(key, code_context),
                )
                add_to_hash(
                    hash_accumulator,
                    type_prefix=TypePrefix.HASH,
                    obj_bytes=self._check_and_hash(elem, code_context),
                )

        elif isinstance(obj, ReferenceProxy):
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.REF_PROXY,
                obj_bytes=obj.val.encode(),
            )

        elif inspect.isbuiltin(obj):
            add_to_hash(hash_accumulator, type_prefix=TypePrefix.BUILTIN)
            builtin_name = "%s.%s" % (obj.__module__, obj.__name__)
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.HASH,
                obj_bytes=self._check_and_hash(builtin_name, code_context),
            )

        elif inspect.isroutine(obj):
            # TODO: See if we can get the version of the module and
            # hash the version as well.
            if (
                obj.__module__ is not None and obj.__module__.startswith("bionic")
            ) or is_internal_file(obj.__code__.co_filename):
                add_to_hash(hash_accumulator, type_prefix=TypePrefix.INTERNAL_ROUTINE)
                routine_name = "%s.%s" % (obj.__module__, obj.__name__)
                add_to_hash(
                    hash_accumulator,
                    type_prefix=TypePrefix.HASH,
                    obj_bytes=self._check_and_hash(routine_name),
                )
            else:
                add_to_hash(hash_accumulator, type_prefix=TypePrefix.ROUTINE)
                code_context = get_code_context(obj)
                add_to_hash(
                    hash_accumulator,
                    type_prefix=TypePrefix.HASH,
                    obj_bytes=self._check_and_hash(obj.__defaults__, code_context),
                )
                self._update_hash_for_code(hash_accumulator, obj.__code__, code_context)

        elif inspect.iscode(obj):
            add_to_hash(hash_accumulator, type_prefix=TypePrefix.CODE)
            self._update_hash_for_code(hash_accumulator, obj, code_context)

        elif inspect.isclass(obj):
            # TODO: Hashing classes is next on our roadmap. Let's hash
            # the name for now.
            add_to_hash(
                hash_accumulator,
                type_prefix=TypePrefix.CLASS,
                obj_bytes=obj.__name__.encode(),
            )

        else:
            # TODO: Verify that we hash all Python constant types.
            add_to_hash(hash_accumulator, type_prefix=TypePrefix.DEFAULT)
            if not self._suppress_warnings:
                # TODO: What else can we tell about this object to the user?
                # Can we add line number, filename and object name?
                message = oneline(
                    f"""
                    Found a complex object {obj!r} of type {type(obj)!r}
                    while analyzing code for caching. Any changes to its
                    value won't be detected by Bionic, which may result in
                    Bionic using stale cache values. Consider making this
                    value a Bionic entity instead.

                    See https://bionic.readthedocs.io/en/stable/warnings.html#avoid-global-state
                    for more information.

                    You can also suppress this warning by removing the
                    `suppress_bytecode_warnings` override from the
                    `@version` decorator on the corresponding function.
                    """
                )
                warnings.warn(message)

    def _update_hash_for_code(self, hash_accumulator, code, code_context):
        assert code_context is not None

        add_to_hash(
            hash_accumulator,
            type_prefix=TypePrefix.HASH,
            obj_bytes=self._check_and_hash(code.co_code),
        )

        # TODO: Maybe there is a way using which we can differentiate
        # between lambda variable names and string constants that end
        # with `.<lambda>`.
        consts = [
            const
            for const in code.co_consts
            if not (isinstance(const, str) and const.endswith(".<lambda>"))
        ]
        add_to_hash(
            hash_accumulator,
            type_prefix=TypePrefix.HASH,
            obj_bytes=self._check_and_hash(consts, code_context),
        )

        references = get_referenced_objects(code, code_context)
        # TODO: We should skip any referenced code objects since
        # they can't be analyzed with the current code's
        # code_context.
        add_to_hash(
            hash_accumulator,
            type_prefix=TypePrefix.HASH,
            obj_bytes=self._check_and_hash(references, code_context),
        )


class TypePrefix(Enum):
    """
    Represents a unique value for each type that CodeHasher hashes that
    is prefixed to avoid collision between same encoded values.

    If you change the prefix of any type, or add a new prefix, it can
    change the encoding of objects. Since Bionic uses these encodings
    to detect changes, any changes to encoding will make caching
    backwards-incompatible. Update CACHE_SCHEMA_VERSION to update cache
    scheme if you change the encoding.
    """

    BYTES = b"AA"
    BYTEARRAY = b"AB"
    NONE = b"AC"
    STRING = b"AD"
    INT = b"AE"
    FLOAT = b"AF"
    BOOL = b"AG"
    LIST = b"AH"
    SET = b"AI"
    TUPLE = b"AJ"
    DICT = b"AK"
    ROUTINE = b"AL"
    CODE = b"AM"
    CIRCULAR_REF = b"AN"
    INTERNAL_ROUTINE = b"AO"
    BUILTIN = b"AP"
    CLASS = b"AQ"
    REF_PROXY = b"AR"
    HASH = b"AS"
    DEFAULT = b"ZZ"


def get_size_as_bytes(obj_bytes):
    assert isinstance(obj_bytes, (bytes, bytearray))
    return str(len(obj_bytes)).encode()


def add_to_hash(hash_accumulator, type_prefix, obj_bytes=b""):
    hash_accumulator.update(type_prefix.value)
    hash_accumulator.update(get_size_as_bytes(obj_bytes))
    hash_accumulator.update(PREFIX_SEPARATOR)
    hash_accumulator.update(obj_bytes)
