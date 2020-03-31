"""
This module defines functions and data structures relating to descriptors.
A descriptor is intended to be a generalized entity name; it's a short string
expression that can represent the input or output of a function in Bionic. For
example, instead of referring only to atomic entities like `raw_data` or
`model`, we will eventually be able to use complex expressions like `model,
report`, `list of model`, and `file_path of model`. However, for the time
being we still only support the simplest type of descriptor: a single entity
name.

A descriptor string can be parsed into a DescriptorNode object, which
represents the abstract syntax tree (AST) of the expression. Bionic's
internals use these "dnodes" to represent values to represent the inputs and
outputs of tasks. DescriptorNodes can also be converted back into descriptor
strings if they need to be serialized or presented to a human.
"""

from abc import ABC, abstractmethod
import re

import attr


ENTITY_NAME_PATTERN = re.compile("[a-zA-Z_][a-zA-Z0-9_]*")


class DescriptorNode(ABC):
    """
    Abstract base class representing a parsed descriptor.
    """

    @classmethod
    def from_descriptor(self, descriptor):
        """
        Parses a descriptor string and returns a DescriptorNode corresponding to
        the descriptor's abstract syntax tree.
        """
        if ENTITY_NAME_PATTERN.fullmatch(descriptor):
            return EntityNode(descriptor)
        else:
            # For now we only support the simplest kind of descriptors.
            raise ValueError(f"Invalid entity descriptor: {descriptor!r}")

    @abstractmethod
    def to_descriptor(self):
        """
        Returns a descriptor string corresponding to this node.
        """
        pass

    def to_entity_name(self):
        """
        If this descriptor is a simple entity name, returns that name; otherwise
        throws a TypeError.
        """
        raise TypeError(f"Descriptor {self.to_descriptor()!r} is not an entity name")


@attr.s(frozen=True)
class EntityNode(DescriptorNode):
    """
    A descriptor node corresponding to a simple entity name.
    """

    name = attr.ib()

    def to_entity_name(self):
        return self.name

    def to_descriptor(self):
        return self.name
