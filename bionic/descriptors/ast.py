"""
This module defines functions and data structures relating to descriptors.
A descriptor is intended to be a generalized entity name; it's a short string
expression that can represent the input or output of a function in Bionic. For
example, instead of referring only to atomic entities like `raw_data` or
`model`, we will eventually be able to use complex expressions like `model,
report`, `list of model`, and `file_path of model`. However, for the time
being we only support descriptors belonging to one of two types: a single entity name
(EntityNode) or a tuple of other descriptors (TupleNode).

A descriptor string can be parsed into a DescriptorNode object, which
represents the abstract syntax tree (AST) of the expression.  (The parsing code is in
`bionic.descriptors.parsing`.) Bionic's internals use these "dnodes" to
represent values to represent the inputs and outputs of tasks.
DescriptorNodes can also be converted back into descriptor strings if they
need to be serialized or presented to a human.
"""

from abc import ABC, abstractmethod
from functools import total_ordering

import attr


@total_ordering
class DescriptorNode(ABC):
    """
    Abstract base class representing a parsed descriptor.
    """

    @abstractmethod
    def to_descriptor(self, near_commas=False):
        """
        Returns a descriptor string corresponding to this node.

        Parameters
        ----------

        near_commas: boolean (default: False)
            Indicates whether the descriptor string will be inserted into a context next
            to other commas, such as within a tuple expression. If so, this method may
            add extra enclosing parentheses to separate it from the surrounding context.
        """
        pass

    def to_entity_name(self):
        """
        If this descriptor is a simple entity name, returns that name; otherwise
        throws a TypeError.
        """
        raise TypeError(f"Descriptor {self.to_descriptor()!r} is not an entity name")

    @abstractmethod
    def all_entity_names(self):
        """
        Returns a list of every entity name appearing in this descriptor, in order,
        include duplicates.
        """
        pass

    # In order to allow different node types to be compared, we define equality and
    # ordering based on the string value of the descriptor. (It should always be the
    # case that different descriptors have different string values.)

    def __eq__(self, other):
        return self.to_descriptor() == other.to_descriptor()

    def __hash__(self):
        return hash(self.to_descriptor())

    def __lt__(self, other):
        return self.to_descriptor() < other.to_descriptor()


# Since equality, hashing, and comparison methods are implemented in the base
# DescriptorNode class, we don't want `attrs` to implement them for us. (Setting
# `eq=False` is sufficient to avoid both the equality and hashing methods.)
node_attrs = attr.s(frozen=True, eq=False, order=False)


@node_attrs
class EntityNode(DescriptorNode):
    """
    A descriptor node corresponding to a simple entity name.
    """

    name = attr.ib()

    def to_entity_name(self):
        return self.name

    def to_descriptor(self, near_commas=False):
        return self.name

    def all_entity_names(self):
        return [self.name]


@node_attrs
class TupleNode(DescriptorNode):
    """
    A descriptor node corresponding a tuple of descriptors.
    """

    children = attr.ib(converter=tuple)

    def to_descriptor(self, near_commas=False):
        if len(self.children) == 0:
            return "()"
        elif len(self.children) == 1:
            desc = f"{self.children[0].to_descriptor(near_commas=True)},"
        else:
            desc = ", ".join(
                child.to_descriptor(near_commas=True) for child in self.children
            )

        if near_commas:
            desc = f"({desc})"
        return desc

    def all_entity_names(self):
        return [name for child in self.children for name in child.all_entity_names()]
