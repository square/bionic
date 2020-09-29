"""
This module defines functions and data structures relating to descriptors.
A descriptor is intended to be a generalized entity name; it's a short string
expression that can represent the input or output of a function in Bionic. For
example, instead of referring only to atomic entities like `raw_data` or
`model`, we will eventually be able to use complex expressions like `model,
report`, `list of model`, and `file_path of model`. However, for the time
being we only support descriptors belonging to one of three types: a single entity name
(EntityNode), a tuple of other descriptors (TupleNode), or a "draft" of another
descriptor (DraftNode).

A descriptor string can be parsed into a DescriptorNode object, which
represents the abstract syntax tree (AST) of the expression.  (The parsing code is in
`bionic.descriptors.parsing`.) Bionic's internals use these "dnodes" to
represent values to represent the inputs and outputs of tasks.
DescriptorNodes can also be converted back into descriptor strings if they
need to be serialized or presented to a human.
"""

from abc import ABC, abstractmethod
from enum import Enum
from functools import total_ordering

import attr

from ..utils.misc import oneline


class NodeTypeEnum(Enum):
    """
    Distinguishes between different subclasses of DescriptorNode.
    """

    ENTITY = "entity"
    TUPLE = "tuple"
    DRAFT = "draft"


@total_ordering
class DescriptorNode(ABC):
    """
    Abstract base class representing a parsed descriptor.
    """

    @property
    @abstractmethod
    def type_enum(self):
        """An enum indicating what subclass of DescriptorNode this is."""
        pass

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

    @abstractmethod
    def all_entity_names(self):
        """
        Returns a list of every entity name appearing in this descriptor, in order,
        include duplicates.
        """
        pass

    @abstractmethod
    def edit(self, func):
        """
        Recursively transforms this node and each of its children.

        Returns an "edited" version of this node, where the node and each of its
        children have been transformed by the function ``func``. ``func`` should
        accept a descriptor node and return a new node. The "edited" node is the
        result of the following operation:

        1. Recursively apply this editing operation to each of this node's children.
        2. Make a modified copy of this node where each child is replaced by the edited
           version.
        3. Apply ``func`` to this modified node and return the result.
        """
        pass

    def is_entity(self):
        """Indicates whether this node is an EntityNode."""
        return self.type_enum == NodeTypeEnum.ENTITY

    def is_tuple(self):
        """Indicates whether this node is a TupleNode."""
        return self.type_enum == NodeTypeEnum.TUPLE

    def is_draft(self):
        """Indicates whether this node is a DraftNode."""
        return self.type_enum == NodeTypeEnum.DRAFT

    # I considered adding some way to pattern-match on node type, like
    #
    #        value = dnode.match(
    #            entity=lambda entity_node: ...,
    #            tuple=lambda tuple_node: ...,
    #            draft=lambda draft_node: ...,
    #        )
    #
    # or
    #
    #        class MyMatcher(NodeMatcher):
    #            def match_entity(self, entity_node):
    #                return ...
    #            def match_tuple(self, tuple_node):
    #                return ...
    #            ...
    #
    # But after trying these out, neither seemed to actually improve the code; I think
    # things are clearest when we use a series of if/else statements and end with
    # `fail_match`. We now also have a Flake8 plugin whihch verifies that `fail_match`
    # is used safely.
    def fail_match(self):
        """
        Raises an AssertionError in an impossible-to-reach matching branch.

        Call this when you've exhaustively tested this node for every type and it
        hasn't matched any of them (which should be impossible). For example:

            if dnode.is_entity():
                ...

            elif dnode.is_tuple():
                ...

            ...

            else:
                dnode.fail_match()
        """

        message = f"""
        Encountered a descriptor {self.to_descriptor()!r} of type
        {self.__class__.__name__};
        this should be impossible and is probably a bug in Bionic
        """
        raise AssertionError(oneline(message))

    def assume_entity(self):
        """Returns this node if it is an EntityNode; otherwise throws TypeError."""
        return self._assume_type_enum(NodeTypeEnum.ENTITY)

    def assume_tuple(self):
        """Returns this node if it is a TupleNode; otherwise throws TypeError."""
        return self._assume_type_enum(NodeTypeEnum.TUPLE)

    def assume_draft(self):
        """Returns this node if it is a DraftNode; otherwise throws TypeError."""
        return self._assume_type_enum(NodeTypeEnum.DRAFT)

    def _assume_type_enum(self, type_enum):
        if self.type_enum != type_enum:
            message = f"""
            Descriptor node for {self.to_descriptor()!r} is assumed to have type_enum
            {type_enum.value!r}, but actually has type_enum {self.type_enum.value!r}
            """
            raise TypeError(oneline(message))
        return self

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

    type_enum = NodeTypeEnum.ENTITY

    def to_descriptor(self, near_commas=False):
        return self.name

    def all_entity_names(self):
        return [self.name]

    def edit(self, func):
        return func(self)


@node_attrs
class TupleNode(DescriptorNode):
    """
    A descriptor node corresponding a tuple of descriptors.
    """

    children = attr.ib(converter=tuple)

    type_enum = NodeTypeEnum.TUPLE

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

    def edit(self, func):
        return func(TupleNode([child.edit(func) for child in self.children]))


@node_attrs
class DraftNode(DescriptorNode):
    """
    A descriptor node corresponding to a "draft" of another descriptor.

    When a descriptor ``D`` is associated with a user-provided value, that value often
    needs to be normalized in some way. This normalized value becomes the official value
    of the descriptor, while the original user-provided value is referred to as the
    "draft" value. To distinguish the draft values from official values, we use the
    draft descriptor ``<D>``.

    For example, consider the following code:

        @builder
        def raw_data(...):
            return pandas.DataFrame(...)

        @builder
        def clean_data(raw_data):
            ...

    Here the ``raw_data`` entity is defined as a Pandas dataframe, and is consumed by
    the ``clean_data`` entity. However, the object received by the ``clean_data``
    function is not exactly the same one returned by ``raw_data``; it has been
    serialized, saved to disk , and then deserialized back into memory, which may
    have subtly changed the contents of the frame. Internally, we use ``<raw_data>``
    to refer to the original unnormalized dataframe, and ``raw_data`` to refer to the
    normalized version. Thus, whenever the user assigns a value to a descriptor, we
    actually assign that value to the draft version of the descriptor, and then derive
    the official value from it later.

    We don't allow users to directly reference draft descriptors. When they're providing
    data, there's no point because all descriptors are implicitly drafts; and when
    they're requesting data, we may not be able to produce the original value without
    doing extra computation.

    Nesting multiple draft descriptors (as in ``<X, <Y>>``) is not permitted. (TODO
    I'm not sure if this requirement is strictly necessary; it might be feasible to
    allow redundant layers of drafts but have them not do anything.)

    Currently the only form of normalization we perform is the process of
    serialization, persistence, and deserialization. (This means that for
    non-persisted values, the official value is identical to the draft.) However, in
    the future there may be other kinds of normalization; this is part of the motivation
    for allowing drafts of any kind of descriptor, rather than just entity descriptors.
    """

    child = attr.ib()

    type_enum = NodeTypeEnum.DRAFT

    def to_descriptor(self, near_commas=False):
        return f"<{self.child.to_descriptor()}>"

    def all_entity_names(self):
        return self.child.all_entity_names()

    def edit(self, func):
        return func(DraftNode(self.child.edit(func)))
