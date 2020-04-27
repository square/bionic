"""
Bionic-specific exception classes.
"""


class UndefinedEntityError(KeyError):
    @classmethod
    def for_name(cls, name):
        return cls(f"Entity {name!r} is not defined")


class AlreadyDefinedEntityError(ValueError):
    @classmethod
    def for_name(cls, name):
        return cls(f"Entity {name!r} is already defined")


class IncompatibleEntityError(ValueError):
    pass


class UnsupportedSerializedValueError(Exception):
    pass


class CodeVersioningError(Exception):
    pass


class EntitySerializationError(Exception):
    pass


class EntityComputationError(Exception):
    pass


class AttributeValidationError(Exception):
    pass


class MalformedDescriptorError(Exception):
    pass
