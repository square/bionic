"""
This module contains the CodeHasher class, which hashes code objects
into values that uniquely identify those objects.

Bionic uses the CodeHasher class to detect any changes to entity
functions. When an entity function changes, the entity value may also
change, which requires Bionic to invalidate the cache and compute the
entity again.

This class is only used when versioning mode is set to "auto" or
"assisted". Since the logic doesn't cover every type of code change,
like changes to references or classes, these versioning modes are still
experimental. When we can detect all kinds of code changes, we will
make "auto" mode the default behavior.

Since Bionic uses these encodings to detect changes, any changes to
encoding will make caching backwards-incompatible. If you make any
changes to this module that can change the encoding, update
CACHE_SCHEMA_VERSION to update cache scheme.
"""

import hashlib
import inspect
import json
import warnings

from .utils.misc import oneline


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

    def __init__(self):
        self._hash = hashlib.new("md5")
        # This is used to detect circular references.
        self._object_depths_by_id = {}

    @classmethod
    def hash(cls, obj):
        hasher = cls()
        hasher._check_and_ingest(obj=obj)
        return hasher._hash.hexdigest()

    def hexdigest(self):
        return self._hash.hexdigest()

    def update(self, obj):
        return self._check_and_ingest(obj)

    def _check_and_ingest(self, obj):
        """
        Checks for circular references before calling the _ingest
        method, which does the actual encoding.
        """
        obj_id = id(obj)

        # If the obj is already being hashed, break the circular ref by
        # analyzing the depth of the value instead.
        if obj_id in self._object_depths_by_id:
            obj = self._object_depths_by_id[obj_id]
            obj_id = id(obj)

        self._object_depths_by_id[obj_id] = len(self._object_depths_by_id)
        self._ingest(obj)
        del self._object_depths_by_id[obj_id]

    def _ingest(self, obj):
        """
        Contains the logic that analyzes the objects and encodes them
        into bytes that are added to the hash.
        """
        if isinstance(obj, bytes):
            self._hash.update(obj)

        elif isinstance(obj, bytearray):
            self._hash.update(obj)

        elif obj is None or isinstance(obj, (int, float, str, bool)):
            json_bytes = json.dumps(obj).encode()
            self._hash.update(obj, json_bytes)

        elif isinstance(obj, (list, tuple)):
            for elem in obj:
                self._check_and_ingest(elem)

        elif isinstance(obj, dict):
            for key, elem in obj.items():
                self._check_and_ingest(key)
                self._check_and_ingest(elem)

        elif inspect.isroutine(obj):
            self._check_and_ingest(obj.__defaults__)
            self._ingest_code(obj.__code__)

        elif inspect.iscode(obj):
            self._ingest_code(obj)

        else:
            # TODO: Verify that we hash all Python constant types.
            message = oneline(
                f"""
                Found a constant {obj!r} of type {type(obj)!r} that
                Bionic doesn't know how to hash. This is most likely a
                bug in Bionic. Please raise a new issue at
                https://github.com/square/bionic/issues to let us know.
                """
            )
            warnings.warn(message)

    def _ingest_code(self, code):
        # TODO: Find references for the code and analyze references.
        self._check_and_ingest(code.co_code)

        # TODO: Maybe there is a way using which we can differentiate
        # between lambda variable names and string constants that end
        # with `.<lambda>`.
        consts = [
            const
            for const in code.co_consts
            if not (isinstance(const, str) and const.endswith(".<lambda>"))
        ]
        self._check_and_ingest(consts)
