"""
This module contains a Flake8 plugin validating our use of `DescriptorNode.fail_match`.

The goal is to check that we only call `fail_match` after exhaustively checking all
cases, as in the following:

    if dnode.is_entity():
        ...

    elif dnode.is_tuple():
        ...

    ...

    else:
        dnode.fail_match()

The plugin requires this exact structure, although it doesn't care about what order the
cases are handled in.

The plugin's entry point is the Checker class; Flake8 loads this class based on the
`flake8:local-plugins` config in `setup.cfg`, and then instantiates it with a parsed
syntax tree for each file in our project.

"""

import ast
from enum import Enum

import attr

from ..descriptors.ast import NodeTypeEnum
from ..utils.misc import oneline


EXPECTED_TESTED_METHOD_NAMES = {f"is_{type_enum.value}" for type_enum in NodeTypeEnum}
N_EXPECTED_METHODS = len(EXPECTED_TESTED_METHOD_NAMES)


class AstExpectationError(Exception):
    pass


class ErrorCode(Enum):
    COMPLEX_CALL = "DNM100"
    TOO_FEW_IFS = "DNM101"
    COMPLEX_TEST = "DNM102"
    MISSING_METHOD = "DNM103"


@attr.s
class Problem:
    node = attr.ib()
    error_code = attr.ib()
    message = attr.ib()

    def to_flake8_tuple(self):
        return (
            self.node.lineno,
            self.node.col_offset,
            f"{self.error_code.value} {self.message}",
            None,
        )


class NodeCursor:
    """
    A wrapper for a Python AST node.

    Allows you to walk up and down the AST while asserting what type of node you expect
    at each step.
    """

    def __init__(self, node, parent):
        self.node = node
        self.parent = parent

    def expect_parent(self, cls):
        self._check(self.parent.node, cls)
        return self.parent

    def expect_field(self, name, cls):
        value = getattr(self.node, name)
        self._check(value, cls)
        return NodeCursor(value, self)

    def _check(self, value, cls):
        if not isinstance(value, cls):
            raise AstExpectationError()


class MatchVisitor(ast.NodeVisitor):
    """
    Walks a Python AST and records any problems.

    The superclass, ``ast.NodeVisitor``, recursively walks the AST; we just override two
    methods:
    - ``visit``, to keep our ``NodeCursor`` up to date,
    - ``visit_Attribute``, to check for Attribute nodes corresponding to method calls
    """

    def __init__(self):
        super(MatchVisitor, self).__init__()
        self._cursor = None
        self.problems = []

    def visit(self, node):
        self._cursor = NodeCursor(node, self._cursor)
        super(MatchVisitor, self).visit(node)
        self._cursor = self._cursor.parent

    def visit_Attribute(self, node):
        if node.attr == "fail_match":
            self._check_fail_match_attribute_node(node)
        self.generic_visit(node)

    def _check_fail_match_attribute_node(self, node):
        """
        Checks the surrounding structure of an attribute node to make sure it looks
        something like this:

            # `If` node
            - test: # `Call` node
                func: # `Attribute` node
                  attr: "is_entity"

              orelse: # list of nodes
                # `If` node
                - test: # `Call` node
                    func: # `Attribute` node
                      attr: "is_tuple"

                  orelse: # list of nodes

                    ... # ... chain of If nodes ...

                      orelse: # list of nodes
                        # `Expr` node
                        - value: # `Call` node
                            func: # `Attribute` node
                              attr: "fail_match"

        Note that we start at the innermost `attr: "fail_match"` node and walk upwards
        to check the surrounding code.
        """

        try:
            cursor = self._cursor.expect_parent(ast.Call).expect_parent(ast.Expr)
        except AstExpectationError:
            self._log_problem(
                ErrorCode.COMPLEX_CALL,
                "`fail_match` should only appear in simple method call",
            )
            return

        tested_method_names = set()
        while True:
            # We'll keep walking up the tree until we hit something other than an if
            # statement.
            try:
                cursor = cursor.expect_parent(ast.If)
            except AstExpectationError:
                break

            # If we do have an if statement, let's make sure it's a simple method call.
            try:
                method_name = (
                    cursor.expect_field("test", ast.Call)
                    .expect_field("func", ast.Attribute)
                    .node.attr
                )
            except AstExpectationError:
                message = f"""
                expected `fail_match` after {N_EXPECTED_METHODS} simple if statements,
                but line {cursor.node.lineno} is not a simple test
                """
                self._log_problem(ErrorCode.COMPLEX_TEST, oneline(message))
                return

            tested_method_names.add(method_name)

        for missing_method_name in EXPECTED_TESTED_METHOD_NAMES.difference(
            tested_method_names
        ):
            message = f"""
            expected `fail_match` only after exhaustive tests, but case
            {missing_method_name} was not tested
            """
            self._log_problem(ErrorCode.MISSING_METHOD, oneline(message))

    def _log_problem(self, error_code, message):
        self.problems.append(Problem(self._cursor.node, error_code, message))


class Checker:
    name = "flake8-bionic-dnode-match"
    version = "0.1"

    def __init__(self, tree):
        self._tree = tree

    def run(self):
        visitor = MatchVisitor()
        visitor.visit(self._tree)
        for problem in visitor.problems:
            yield problem.to_flake8_tuple()


if __name__ == "__main__":
    import sys
    from pathlib import Path

    filenames = sys.argv[1:]

    for filename in filenames:
        code = Path(filename).read_text()
        tree = ast.parse(code)
        for message in Checker(tree).run():
            print(message)  # noqa: T001
