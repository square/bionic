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


class UnsetEntityError(ValueError):
    pass


class IncompatibleEntityError(ValueError):
    pass


class UnsupportedSerializedValueError(Exception):
    pass


class CodeVersioningError(Exception):
    def __init__(self, message, bad_descriptor):
        super(CodeVersioningError, self).__init__(message)
        self.bad_descriptor = bad_descriptor


class EntitySerializationError(Exception):
    pass


class EntityComputationError(Exception):
    pass


class EntityValueError(ValueError):
    pass


class AttributeValidationError(Exception):
    pass


class MalformedDescriptorError(Exception):
    pass
