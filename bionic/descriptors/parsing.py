"""
This module contains functions for parsing descriptor strings into descriptor AST nodes.

Currently we use a hand-written parser instead a parser generator or other
generic parsing tool. I looked through several parsing libraries but didn't find one
with all the following properties:

- It's clearly maintained, documented, and robust.
- It has no major API issues (like global state).
- It has no big dependencies (like a JVM).
- I was able to figure out how to parse the full descriptor language. (The current
syntax is quite simple, but in the future the syntax will support prefix modifiers
(e.g., "big linear model") and I'm not sure if it can be parsed with a standard LR
parser. I also had trouble getting PEG-style and combinator-based parsers to work for
the full language. However, I'm not an expert and didn't give a lot of attention to
every library.)
- I was sure the parsing would behave as I expected. (In particular, I was able to
get the Lark library to parse the full planned language, but the grammar was
ambiguous and I had trouble understanding the way Lark resolved the ambiguities.)
- It's easy to provide clear, precise error messages when parsing fails.

That said, I'm open to switching to a library if we can achieve the above goals.
"""

import re
from typing import Optional, List

import attr

from .ast import DescriptorNode, DraftNode, EntityNode, TupleNode
from ..exception import MalformedDescriptorError
from ..utils.misc import oneline


def dnode_from_descriptor(descriptor):
    """
    Given a descriptor string, returns the parsed descriptor node.
    """

    return DescriptorParser().parse(descriptor)


def nondraft_dnode_from_descriptor(descriptor):
    """
    Given a descriptor string containing no drafts, returns the parsed descriptor node.

    This is useful for parsing user-submitted descriptors, which should never contain
    drafts. (Allowing users to submit draft descriptors would cause problems: using
    them as inputs may fail because draft values can be garbage-collected early; and
    using them as outputs will fail because every output is wrapped in a draft, and
    drafts can't be nested.)
    """

    return DescriptorParser().parse(descriptor, allow_drafts=False)


def entity_dnode_from_descriptor(descriptor):
    """
    Given an entity descriptor string, returns the parsed entity descriptor node.
    """

    dnode = dnode_from_descriptor(descriptor)
    if not dnode.is_entity():
        raise ValueError(f"Expected a valid entity name, but got {descriptor!r}")
    return dnode


TOKEN_PATTERN = re.compile(
    r"(?P<whitespace>\s+)"
    r"|(?P<name>[a-zA-Z_][a-zA-Z0-9_]*)"
    r"|(?P<lparen>\()"
    r"|(?P<rparen>\))"
    r"|(?P<comma>,)"
    r"|(?P<langle><)"
    r"|(?P<rangle>>)"
    r"|(?P<end>$)"
)


@attr.s
class AugmentedToken:
    """
    A parsed token, along with some extra metadata.
    """

    token = attr.ib()
    token_type = attr.ib()
    start_pos = attr.ib()

    def __str__(self):
        if self.token_type == "end":
            token_description = "end of string"
        else:
            token_description = repr(self.token)
        return f"{token_description} (at position {self.start_pos})"


class DescriptorParser:
    "Parses descriptor strings into AST nodes."

    def __init__(self):
        # All initialization happens in parse().
        pass

    def parse(self, descriptor, allow_drafts=True):
        """
        Parses a descriptor string into a descriptor node.

        Not thread-safe (because it uses member fields to manage parsing state).

        This is a sketch of the grammar:

            descriptor: expr

            expr: commaless_expr | tuple
            commaless_expr: parenthentical | draft | ENTITY
            tuple: commaless_expr ','
                | commaless_expr (',' commaless_expr)+ ','?
            parenthetical: '(' expr ')'
            draft: '<' expr '>'

            ENTITY: /[a-zA-Z_][a-zA-Z0-9_]/
        """

        # Initialize parser state.
        self._descriptor = descriptor
        self._allow_drafts = allow_drafts
        self._cur_aug_token = None
        self._prev_aug_token = None
        self._expr_stack = [ExprParseState()]

        # Parse each token.
        for aug_token in self._gen_aug_tokens():
            self._cur_aug_token = aug_token
            self._parse_cur_aug_token()
            self._prev_aug_token = aug_token

        # The current (and only) expression on the stack should now have the parsed
        # node.
        assert self._cur_expr.parsed_dnode is not None
        return self._cur_expr.parsed_dnode

    @property
    def _cur_expr(self):
        return self._expr_stack[-1]

    def _gen_aug_tokens(self):
        descriptor = self._descriptor
        pos = 0
        while True:
            match = TOKEN_PATTERN.match(descriptor, pos=pos)
            if match is None:
                self._fail(f"illegal character {descriptor[pos]!r} (at position {pos})")
            token_type = match.lastgroup
            assert token_type is not None
            if token_type != "whitespace":
                yield AugmentedToken(
                    token=match.group(),
                    token_type=token_type,
                    start_pos=pos,
                )
            if token_type == "end":
                break
            pos = match.end()

    def _parse_cur_aug_token(self):
        token_type = self._cur_aug_token.token_type

        if token_type == "name":
            self._parse_entity_name()

        elif token_type == "comma":
            self._open_or_extend_tuple_expr()

        elif token_type == "lparen":
            self._open_paren()

        elif token_type == "rparen":
            self._finish_parsing_cur_expr_if_tuple()
            self._close_paren()

        elif token_type == "langle":
            self._open_angle()

        elif token_type == "rangle":
            self._finish_parsing_cur_expr_if_tuple()
            self._close_angle()

        elif token_type == "end":
            self._finish_parsing_cur_expr_if_tuple()
            self._finish_parsing()

    def _parse_entity_name(self):
        name = self._cur_aug_token.token
        if self._cur_expr.parsed_dnode is not None:
            self._fail(
                f"""
                found unexpected name {name}
                following an already-complete expression
                {self._cur_expr.parsed_dnode.to_descriptor()!r}
                """
            )
        self._cur_expr.parsed_dnode = EntityNode(name)

    def _open_or_extend_tuple_expr(self):
        if self._cur_expr.parsed_dnode is None:
            if self._cur_expr.active_tuple_dnodes is None:
                self._fail(
                    f"""
                    found unexpected {self._cur_aug_token}
                    with no preceding expression
                    """
                )
            else:
                assert self._prev_aug_token.token_type == "comma", self._prev_aug_token
                self._fail(
                    f"""
                    found unexpected {self._cur_aug_token}
                    immediately following another ','
                    """
                )

        parsed_dnode = self._cur_expr.parsed_dnode
        self._cur_expr.parsed_dnode = None

        if self._cur_expr.active_tuple_dnodes is None:
            self._cur_expr.active_tuple_dnodes = []
        self._cur_expr.active_tuple_dnodes.append(parsed_dnode)

    def _finish_parsing_cur_expr_if_tuple(self):
        if self._cur_expr.active_tuple_dnodes is not None:
            if self._cur_expr.parsed_dnode is not None:
                self._cur_expr.active_tuple_dnodes.append(self._cur_expr.parsed_dnode)
            self._cur_expr.parsed_dnode = TupleNode(self._cur_expr.active_tuple_dnodes)
            self._cur_expr.active_tuple_dnodes = None

    def _open_paren(self):
        if self._cur_expr.parsed_dnode is not None:
            self._fail(
                f"""
                found unexpected {self._cur_aug_token}
                following an already-complete expression
                {self._cur_expr.parsed_dnode.to_descriptor()!r}
                """
            )
        self._expr_stack.append(
            ExprParseState(start_lparen_aug_token=self._cur_aug_token)
        )

    def _close_paren(self):
        if self._cur_expr.start_lparen_aug_token is None:
            if self._cur_expr.start_langle_aug_token is not None:
                self._fail(
                    f"""
                    found unexpected {self._cur_aug_token};
                    expected '>' instead to match
                    {self._cur_expr.start_langle_aug_token}
                    """
                )
            self._fail(
                f"""
                found unexpected {self._cur_aug_token}
                with no matching '('
                """
            )

        if self._cur_expr.parsed_dnode is None:
            self._cur_expr.parsed_dnode = TupleNode([])

        expr = self._expr_stack.pop()
        self._cur_expr.parsed_dnode = expr.parsed_dnode

    def _open_angle(self):
        if not self._allow_drafts:
            self._fail(
                f"""
                found illegal {self._cur_aug_token};
                draft expressions are not allowed in user-provided descriptors
                """
            )
        # This requires a linear search through the stack, but I don't anticipate the
        # stack ever being deep enough for this to be a performance issue.
        for expr in self._expr_stack:
            if expr.start_langle_aug_token is not None:
                self._fail(
                    f"""
                    found illegal {self._cur_aug_token}
                    nested within another {expr.start_langle_aug_token}
                    """
                )
        if self._cur_expr.parsed_dnode is not None:
            self._fail(
                f"""
                found unexpected {self._cur_aug_token}
                following an already-complete expression
                {self._cur_expr.parsed_dnode.to_descriptor()!r}
                """
            )
        self._expr_stack.append(
            ExprParseState(start_langle_aug_token=self._cur_aug_token)
        )

    def _close_angle(self):
        if self._cur_expr.start_langle_aug_token is None:
            if self._cur_expr.start_lparen_aug_token is not None:
                self._fail(
                    f"""
                    found unexpected {self._cur_aug_token};
                    expected ')' instead to match
                    {self._cur_expr.start_lparen_aug_token}
                    """
                )
            self._fail(
                f"""
                found unexpected {self._cur_aug_token}
                with no matching '<'
                """
            )
        if self._cur_expr.parsed_dnode is None:
            self._fail(
                f"""
                found no expression between {self._cur_expr.start_langle_aug_token}
                and {self._cur_aug_token}
                """
            )

        expr = self._expr_stack.pop()
        self._cur_expr.parsed_dnode = DraftNode(expr.parsed_dnode)

    def _finish_parsing(self):
        if self._cur_expr.start_lparen_aug_token is not None:
            lparen_aug_token = self._cur_expr.start_lparen_aug_token
            self._fail(
                f"""
                {lparen_aug_token} has no matching ')'
                """
            )

        if self._cur_expr.start_langle_aug_token is not None:
            langle_aug_token = self._cur_expr.start_langle_aug_token
            self._fail(
                f"""
                {langle_aug_token} has no matching '>'
                """
            )

        if self._cur_expr.parsed_dnode is None:
            assert self._descriptor.strip() == ""
            self._fail("descriptor is empty")

        assert len(self._expr_stack) == 1

    def _fail(self, message):
        raise MalformedDescriptorError(
            f"Unable to parse descriptor {self._descriptor!r}: " + oneline(message)
        )


@attr.s
class ExprParseState:
    """
    The state of a parser as it processes a particular expression.
    """

    start_lparen_aug_token: Optional[AugmentedToken] = attr.ib(default=None)
    start_langle_aug_token: Optional[AugmentedToken] = attr.ib(default=None)

    active_tuple_dnodes: Optional[List[DescriptorNode]] = None
    parsed_dnode: Optional[DescriptorNode] = None
