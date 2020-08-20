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


def get_referenced_objects(code, context):
    """
    Returns referenced objects for a code object and name of the references when
    the references cannot be detected without performing runtime logic.

    Referenced objects can be anything from a class, a function, a module or any
    variables. The references can also be any scope, like global, local, cell,
    free, or can even be in another object referenced using a full qualified
    name.

    Note that this function does not perform any runtime logic. This includes
    calling any functions used by the input code because doing so can be
    expensive and have unintended consequences. Due to this, any references with
    a fully qualified name that uses result of such a call won't be detected.
    This means that if an inner function returns a module or a class, any
    variables of the module or the class won't be detected. In this case, we will
    return the name of the reference as a proxy. So for a function like below,
    the returned reference would be ``"call"`` instead of ``MyClass.call``.

    .. code-block:: python

        def x():
            my_cls = get_my_class("MyClass") # Returns class MyClass
            my_cls.call()


    On a higher level, this leaves out some changes that Bionic won't be able to
    auto-detect for caching. Bionic should be able to detect any changes except
    when a function returns a class or a module. In those case, Bionic cannot
    detect what is returned. But it should be able to detect the class / module
    returned when hashing the function which returns the said result. Bionic will
    still hash those classes / modules, and detect as many changes as it can in
    them.
    """

    # We mutate context while finding references. Let's make a copy of
    # context to not change the original context. The original context
    # can be shared between different code objects, like between an
    # outer and an inner function.
    context = CodeContext(
        globals=context.globals.copy(),
        cells=context.cells.copy(),
        varnames=context.varnames.copy(),
    )

    # Top of the stack.
    tos = None
    lineno = None
    refs = []

    def set_tos(t, placeholder=False):
        nonlocal tos
        if tos is not None:
            # If the top of stack item already exists, that means we
            # have gone through all the instructions that use the item,
            # and it is a reference object.
            if isinstance(tos, PlaceholderTos):
                # TODO: Consider returning the value as PlaceholderTos
                # class once we hash classes too.
                refs.append(tos.val)
            else:
                refs.append(tos)

        if placeholder:
            t = PlaceholderTos(t)
        tos = t

    # Our goal is to find referenced objects. The problem is that co_names
    # does not have full qualified names in it. So if you access `foo.bar`,
    # co_names has `foo` and `bar` in it but it doesn't tell us that the
    # code reads `bar` of `foo`. We are going over the bytecode to resolve
    # from which object an attribute is requested.
    # Read more about bytecode at https://docs.python.org/3/library/dis.html

    for op in dis.get_instructions(code):
        try:
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
                    set_tos(op.argval, placeholder=True)
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
                    set_tos(op.argval, placeholder=True)
            elif op.opname == "IMPORT_NAME":
                # This instruction only appears if the code object imports a
                # module using the import statement. If a user is importing
                # modules inside a function, they probably don't want to import
                # the module until the function execution time.
                message = oneline(
                    f"""
                Bionic found a code reference in file ${code.co_filename} at line
                ${lineno} that imports ${op.argval} module when hashing
                ${code.co_name}. Changes inside the module will likely not be
                detected. To detect changes inside the module, import it globally
                instead.
                """
                )
                warnings.warn(message)
                set_tos(None)
            elif op.opname in ["LOAD_METHOD", "LOAD_ATTR"]:
                if isinstance(tos, PlaceholderTos):
                    tos.val += "." + op.argval
                # TODO: Consider calling getattr only when TOS is a module.
                # Doing so has risk of missing cases, and any missing case would
                # be hard to detect for users. Before making this change,
                # document all the cases that we won't be catching so that we are
                # aware of them.
                # TODO: Due to the same reason as why we don't import modules, we
                # should not call `getattr` on properties. We should either add a
                # separate check for properties, or implement the above TODO to
                # fix this.
                elif hasattr(tos, op.argval):
                    tos = getattr(tos, op.argval)
                else:
                    set_tos(op.argval, placeholder=True)
            elif op.opname == "STORE_FAST" and tos:
                context.varnames[op.argval] = tos
                tos = None
            elif op.opname == "LOAD_FAST" and op.argval in context.varnames:
                set_tos(context.varnames[op.argval])
            else:
                # For all other instructions, add the current TOS as a
                # reference.
                set_tos(None)
        except Exception as e:
            # TODO: Provide a way for users to disable bytecode hashing
            # for individual entities and add information on how to
            # disable it in the error message.
            message = oneline(
                f"""
            Bionic found a code reference in file ${code.co_filename}
            at line ${lineno} that it cannot hash when hashing
            ${code.co_name}. This should be impossible and is most
            likely a bug in Bionic. Please raise a new issue at
            https://github.com/square/bionic/issues to let us know.
            """
            )
            raise AssertionError(message) from e

    return refs


class PlaceholderTos:
    """
    Wraps the placeholder strings for TOS objects to differentiate the
    object from actual string objects.
    """

    def __init__(self, val):
        self.val = str(val)
