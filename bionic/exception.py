"""
Bionic-specific exception classes.
"""

from .utils.misc import oneline


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


class UnavailableArtifactError(Exception):
    def __init__(self, artifact_dnode):
        source_desc = artifact_dnode.assume_generic().child.to_descriptor()
        message = f"""
        Descriptor {source_desc!r} is not configured to have a persisted artifact.
        """
        super(UnavailableArtifactError, self).__init__(oneline(message))
        self.artifact_dnode = artifact_dnode
