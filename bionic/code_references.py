# Copyright 2018-2020 Streamlit Inc.
# Author: Dominik Moritz

# The code in this module is copied from Streamlit with modifications.
# Unlike Streamlit, we don't import modules to avoid runtime
# computation and use newer libraries to make the code cleaner.
# https://github.com/streamlit/streamlit

"""
This module contains functions which are used to find references in
your code objects. `get_referenced_objects` function finds those
references for you given a code object and an additional `CodeContext`
input, which can be extracted using the `get_code_context` function.

CodeHasher uses these methods as part of hashing an entity function for
Bionic cache invalidation. Using these methods, CodeHasher can hash the
references of entity functions (and even their references), so that any
change in the references are also detected by Bionic to invalidate
cache.
"""

import attr
import dis
import inspect
import sys
import warnings

from .utils.misc import oneline


@attr.s
class CodeContext:
    """
    Holds variable information from function and code object
    attributes.

    Attributes
    ----------
    globals: dict
        A reference to the dictionary that holds the function’s global
        variables — the global namespace of the module in which the
        function was defined.
    cells: dict
        A dictionary that tracks all free variables by name and cell
        variable names by themselves.
    varnames: dict
        A dictionary that is used to track local variables by name.
    """

    globals = attr.ib()
    cells = attr.ib()
    varnames = attr.ib()

    def copy(self):
        return CodeContext(
            globals=self.globals.copy(),
            cells=self.cells.copy(),
            varnames=self.varnames.copy(),
        )


def get_code_context(func) -> CodeContext:
    code = func.__code__

    # Mapping from variable name to the value if we can resolve it.
    # Otherwise map to the name.
    cells = {}

    for var in code.co_cellvars:
        # Streamlit uses the names of the variable instead of the value
        # for cell variables. I suspect this is so because it is hard
        # to find the value of a cell variable. It doesn't mean we
        # don't hash these values for Smart Caching. These cell
        # variables are also present in ``code.co_consts``` which we
        # hash separately.
        cells[var] = var

    if code.co_freevars:
        assert len(code.co_freevars) == len(func.__closure__)
        for freevar, cell in zip(code.co_freevars, func.__closure__):
            cells[freevar] = cell.cell_contents

    varnames = {}
    if inspect.ismethod(func):
        varnames = {"self": func.__self__}

    return CodeContext(globals=func.__globals__, cells=cells, varnames=varnames)


def get_referenced_objects(code, context, suppress_warnings=False):
    """
    Attempts to return all objects referenced externally by a code object. In
    some cases, these objects are:
    - replaced with ReferenceProxy class: when we can't find a variable or when
      the reference is an attribute of the result of a function call, or
    - omitted: when the reference is accessed using exec or is the result of a
      function call.

    An externally referenced object is any object used by a piece of code but not
    defined in that code. These objects can include classes, functions, modules,
    or any other variables. They can be referenced from various scopes: global,
    "free" (when the variable is used in a code block, but is not defined there
    and is not global), etc.; or as attributes of other referenced objects.

    Note that this function does not actually run any code. This includes
    calling any functions used by the input code, because doing so can be
    expensive and have unintended consequences. Due to this, any references that
    are attributes of the result of a function call won’t be detected. This means
    that if an inner function returns a module, any attributes of the module
    won't be detected. In this case, we return the name of the attribute as a
    proxy for the object itself. So for the function below, the returned
    references would be ``[get_my_class, "call"]``.

    .. code-block:: python

        def x():
            my_cls = get_my_class("MyClass") # Returns class MyClass
            my_cls.call()

    This function uses a CodeContext object to look up variables defined outside
    the local scope and to track local variables created while running the logic
    to find references.
    """

    # We mutate context while finding references. Let's make a copy of
    # context to not change the original context. The original context
    # can be shared between different code objects, like between an
    # outer and an inner function.
    context = context.copy()

    # Top of the stack.
    tos = None
    lineno = None
    refs = []

    def set_tos(t):
        nonlocal tos
        if tos is not None:
            # If the top of stack item already exists, that means we
            # have gone through all the instructions that use the item,
            # and it is a reference object.
            refs.append(tos)

        tos = t

    # Our goal is to find referenced objects. The problem is that co_names
    # does not have fully qualified names in it. So if you access `foo.bar`,
    # co_names has `foo` and `bar` in it but it doesn't tell us that the
    # code reads `bar` of `foo`. We are going over the bytecode to resolve
    # from which object an attribute is requested.
    # Read more about bytecode at https://docs.python.org/3/library/dis.html

    for op in dis.get_instructions(code):
        try:
            if op.opname not in SUPPORTED_INSTRUCTIONS and not suppress_warnings:
                if sys.version_info < (3, 6) or sys.version_info > (3, 8):
                    message = """
                    You are using an unsupported Python version for Bionic. This
                    can result in Bionic missing some code changes to invalidate
                    cache. Consider using a supported Python version to avoid any
                    caching issues.
                    """
                else:
                    message = f"""
                    Bionic does not recognize {op.opname} Bytecode operation.
                    This should be impossible and is most likely a bug in Bionic.
                    Please raise a new issue at
                    https://github.com/square/bionic/issues to let us know.
                    """
                message += """
                You can also suppress this warning by removing the
                `suppress_bytecode_warnings` override from the
                `@version` decorator on the corresponding function.
                """
                warnings.warn(oneline(message))
            # Sometimes starts_line is None, in which case let's just remember the
            # previous start_line (if any). This way when there's an exception we at
            # least can point users somewhat near the line where the error stems from.
            if op.starts_line is not None:
                lineno = op.starts_line

            if op.opname in ["LOAD_GLOBAL", "LOAD_NAME"]:
                if op.argval in context.globals:
                    set_tos(context.globals[op.argval])
                else:
                    # This can happen if the variable does not exist, or if LOAD_NAME
                    # is trying to access a local frame argument. If we cannot find the
                    # variable, we return its name instead.
                    set_tos(ReferenceProxy(op.argval))
            elif op.opname in ["LOAD_DEREF", "LOAD_CLOSURE"]:
                if op.argval in context.cells:
                    set_tos(context.cells[op.argval])
                else:
                    # This can happen when we have nested functions. The second
                    # level or further nested functions won't have free variables
                    # from any preceding function except for the top level
                    # function. This is because the code context that gives us
                    # free variables is created from the function variable, which
                    # we only have for the top level function. We get only the
                    # code object for any inner functions. Since we can't get the
                    # code context for inner functions, we use the code context
                    # of the top level function.
                    set_tos(ReferenceProxy(op.argval))
            elif op.opname == "IMPORT_NAME":
                # This instruction only appears if the code object imports a
                # module using the import statement. If a user is importing
                # modules inside a function, they probably don't want to import
                # the module until the function execution time.
                message = f"""
                Entity function in file {code.co_filename} imports the
                '{op.argval}' module at line {lineno};
                Bionic will not be able to automatically detect any changes to this
                module.
                To enable automatic detection of changes, import the module at the
                global level (outside the function) instead.

                To suppress this warning, remove the `suppress_bytecode_warnings`
                override from the `@version` decorator on the corresponding function.
                f"""
                warnings.warn(oneline(message))
                set_tos(None)
            elif op.opname in ["LOAD_METHOD", "LOAD_ATTR"]:
                if isinstance(tos, ReferenceProxy):
                    tos.val += "." + op.argval
                elif inspect.ismodule(tos) and hasattr(tos, op.argval):
                    tos = getattr(tos, op.argval)
                else:
                    set_tos(ReferenceProxy(op.argval))
            elif op.opname == "STORE_FAST" and tos:
                context.varnames[op.argval] = tos
                set_tos(None)
            elif op.opname == "LOAD_FAST" and op.argval in context.varnames:
                set_tos(context.varnames[op.argval])
            else:
                # For all other instructions, add the current TOS as a
                # reference.
                set_tos(None)
        except Exception as e:
            message = oneline(
                f"""
            Bionic found a code reference in file ${code.co_filename}
            at line ${lineno} that it cannot hash when hashing
            ${code.co_name}. This should be impossible and is most
            likely a bug in Bionic. Please raise a new issue at
            https://github.com/square/bionic/issues to let us know.

            In the meantime, you can disable bytecode analysis for
            the corresponding function by setting `ignore_bytecode`
            on its `@version` decorator. Please note that Bionic won't
            automatically detect changes in this function; you'll need
            to manually update the version yourself.
            """
            )
            raise AssertionError(message) from e

    return refs


@attr.s
class ReferenceProxy:
    """
    When we can't find the actual value of a reference variable, we
    return a proxy in it's place that contains the name of the
    variable. This class wraps those proxy references, so that we can
    differentiate them from actual string objects.
    """

    val = attr.ib()


# TODO: See if we can support Python 3.9.
"""
List of all the opcode instructions that we have evaluated for finding code
references. As of now, these are all the instructions listed in dis up to
Python 3.8.
"""
SUPPORTED_INSTRUCTIONS = {
    "NOP",
    "POP_TOP",
    "ROT_TWO",
    "ROT_THREE",
    "ROT_FOUR",
    "DUP_TOP",
    "DUP_TOP_TWO",
    "UNARY_POSITIVE",
    "UNARY_NEGATIVE",
    "UNARY_NOT",
    "UNARY_INVERT",
    "GET_ITER",
    "GET_YIELD_FROM_ITER",
    "BINARY_POWER",
    "BINARY_MULTIPLY",
    "BINARY_MATRIX_MULTIPLY",
    "BINARY_FLOOR_DIVIDE",
    "BINARY_TRUE_DIVIDE",
    "BINARY_MODULO",
    "BINARY_ADD",
    "BINARY_SUBTRACT",
    "BINARY_SUBSCR",
    "BINARY_LSHIFT",
    "BINARY_RSHIFT",
    "BINARY_AND",
    "BINARY_XOR",
    "BINARY_OR",
    "INPLACE_POWER",
    "INPLACE_MULTIPLY",
    "INPLACE_MATRIX_MULTIPLY",
    "INPLACE_FLOOR_DIVIDE",
    "INPLACE_TRUE_DIVIDE",
    "INPLACE_MODULO",
    "INPLACE_ADD",
    "INPLACE_SUBTRACT",
    "INPLACE_LSHIFT",
    "INPLACE_RSHIFT",
    "INPLACE_AND",
    "INPLACE_XOR",
    "INPLACE_OR",
    "STORE_SUBSCR",
    "DELETE_SUBSCR",
    "GET_AWAITABLE",
    "GET_AITER",
    "GET_ANEXT",
    "END_ASYNC_FOR",
    "BEFORE_ASYNC_WITH",
    "SETUP_ASYNC_WITH",
    "PRINT_EXPR",
    "BREAK_LOOP",
    "CONTINUE_LOOP",
    "SET_ADD",
    "LIST_APPEND",
    "MAP_ADD",
    "RETURN_VALUE",
    "YIELD_VALUE",
    "YIELD_FROM",
    "SETUP_ANNOTATIONS",
    "IMPORT_STAR",
    "POP_BLOCK",
    "POP_EXCEPT",
    "POP_FINALLY",
    "BEGIN_FINALLY",
    "END_FINALLY",
    "LOAD_BUILD_CLASS",
    "SETUP_WITH",
    "WITH_CLEANUP_START",
    "WITH_CLEANUP_FINISH",
    "STORE_NAME",
    "DELETE_NAME",
    "UNPACK_SEQUENCE",
    "UNPACK_EX",
    "STORE_ATTR",
    "DELETE_ATTR",
    "STORE_GLOBAL",
    "DELETE_GLOBAL",
    "LOAD_CONST",
    "LOAD_NAME",
    "BUILD_TUPLE",
    "BUILD_LIST",
    "BUILD_SET",
    "BUILD_MAP",
    "BUILD_CONST_KEY_MAP",
    "BUILD_STRING",
    "BUILD_TUPLE_UNPACK",
    "BUILD_TUPLE_UNPACK_WITH_CALL",
    "BUILD_LIST_UNPACK",
    "BUILD_SET_UNPACK",
    "BUILD_MAP_UNPACK",
    "BUILD_MAP_UNPACK_WITH_CALL",
    "LOAD_ATTR",
    "COMPARE_OP",
    "IMPORT_NAME",
    "IMPORT_FROM",
    "JUMP_FORWARD",
    "POP_JUMP_IF_TRUE",
    "POP_JUMP_IF_FALSE",
    "JUMP_IF_TRUE_OR_POP",
    "JUMP_IF_FALSE_OR_POP",
    "JUMP_ABSOLUTE",
    "FOR_ITER",
    "LOAD_GLOBAL",
    "SETUP_LOOP",
    "SETUP_EXCEPT",
    "SETUP_FINALLY",
    "CALL_FINALLY",
    "LOAD_FAST",
    "STORE_FAST",
    "DELETE_FAST",
    "STORE_ANNOTATION",
    "LOAD_CLOSURE",
    "LOAD_DEREF",
    "LOAD_CLASSDEREF",
    "STORE_DEREF",
    "DELETE_DEREF",
    "RAISE_VARARGS",
    "CALL_FUNCTION",
    "CALL_FUNCTION_KW",
    "CALL_FUNCTION_EX",
    "LOAD_METHOD",
    "CALL_METHOD",
    "MAKE_FUNCTION",
    "BUILD_SLICE",
    "EXTENDED_ARG",
    "FORMAT_VALUE",
    "HAVE_ARGUMENT",
}
