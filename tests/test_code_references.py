import pytest

import logging

from bionic.code_references import (
    get_code_context,
    get_referenced_objects,
    ReferenceProxy,
)
from bionic.flow import FlowBuilder
from bionic.utils.misc import oneline

global_val = 42


def get_references(func):
    context = get_code_context(func)
    return get_referenced_objects(func.__code__, context)


def test_empty_references():
    def x():
        pass

    assert get_references(x) == []

    def x():
        return 42

    assert get_references(x) == []

    def x(val="42"):
        return val

    assert get_references(x) == []

    def x():
        import warnings

        return warnings

    with pytest.warns(UserWarning, match=".*imports the 'warnings' module.*"):
        assert get_references(x) == []


def test_global_references():
    def x():
        return global_val

    assert get_references(x) == [42]


def test_free_references():
    free_val = "42"

    def x():
        return free_val

    assert get_references(x) == ["42"]


def test_cell_references():
    def x():
        cell_val = "42"

        def y():
            return cell_val

    assert get_references(x) == ["cell_val"]


def test_function_references():
    def x():
        return "42"

    def y():
        return x()

    assert get_references(y) == [x]

    def y():
        return oneline("Use a function in another module")

    assert get_references(y) == [oneline]

    def y():
        return func_does_not_exist()  # noqa: F821

    assert get_references(y) == [ReferenceProxy("func_does_not_exist")]


def test_class_references():
    class MyClass:
        def __init__(self):
            self.value = "42"

        @property
        def val(self):
            return self.value

        def log_val(self):
            logging.info(self.value)

    my_class = MyClass()

    def x():
        logging.info(my_class.val)
        my_class.log_val()
        return my_class

    assert get_references(x) == [logging.info, "42", my_class.log_val, my_class]

    def x():
        builder = FlowBuilder()
        builder.assign("cls", MyClass)
        return builder

    assert get_references(x) == [FlowBuilder, ReferenceProxy("assign"), MyClass]


def test_method_references():
    class MyClass:
        def __init__(self):
            self.value = "42"

        def log_val(self):
            logging.log(self.value)

    def x():
        my_class = MyClass()
        my_class.log_val()

    # We don't get the method as a reference because class initialization
    # is a function call and that incorrectly sets my_class as None.
    assert get_references(x) == [MyClass, ReferenceProxy("log_val")]

    def y(my_class):
        my_class.log_val()

    assert get_references(y) == [ReferenceProxy("log_val")]


def test_references_with_qualified_names():
    import multiprocessing

    def x():
        """This function tests LOAD_ATTR opcode"""
        p = multiprocessing.managers.public_methods
        return p(multiprocessing.managers.SyncManager)

    assert get_references(x) == [
        multiprocessing.managers.public_methods,
        multiprocessing.managers.SyncManager,
    ]

    def x():
        """This function tests LOAD_METHOD opcode"""
        m = multiprocessing.managers.SyncManager
        return m.start()

    assert get_references(x) == [
        multiprocessing.managers.SyncManager.start,
    ]


def test_multiple_references():
    def get_val(dict, key):
        return dict[key]

    def enclosing_x():
        free_var = "free_val"

        def x(func_def_var="func_def_val"):
            local_var = {"local_key": "local_val"}
            local_val = get_val(local_var, "local_key")
            cell_var = "cell_val"
            logging.log(global_val, free_var, local_val, cell_var, func_def_var)
            builder = FlowBuilder("x")
            # more business logic
            builder.build()

            def inner_x():
                inner_var = "inner_val"

                logging.debug(inner_var, cell_var)

        return x

    assert get_references(enclosing_x()) == [
        get_val,
        logging.log,
        global_val,
        "free_val",
        "cell_var",
        FlowBuilder,
        ReferenceProxy("build"),
        "cell_var",
    ]


def test_conditionals():
    def x(my_class):
        if my_class.val == "42":
            logging.info("INFO log")
        else:
            logging.debug("DEBUG log")

    assert get_references(x) == [ReferenceProxy("val"), logging.info, logging.debug]


def test_complex_function():
    import inspect
    import warnings
    from bionic.code_hasher import CodeHasher, TypePrefix
    from bionic.utils.reload import is_internal_file

    hasher = CodeHasher(False)
    # TODO: This makes me realize that maybe we should dedup the results.
    assert get_references(hasher._ingest) == [
        ReferenceProxy("isinstance"),
        ReferenceProxy("bytes"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.BYTES,
        ReferenceProxy("isinstance"),
        ReferenceProxy("bytearray"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.BYTEARRAY,
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.NONE,
        ReferenceProxy("isinstance"),
        ReferenceProxy("int"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.INT,
        ReferenceProxy("str"),
        ReferenceProxy("encode"),
        ReferenceProxy("isinstance"),
        ReferenceProxy("float"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.FLOAT,
        ReferenceProxy("str"),
        ReferenceProxy("encode"),
        ReferenceProxy("isinstance"),
        ReferenceProxy("str"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.STRING,
        ReferenceProxy("encode"),
        ReferenceProxy("isinstance"),
        ReferenceProxy("bool"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.BOOL,
        ReferenceProxy("str"),
        ReferenceProxy("encode"),
        ReferenceProxy("isinstance"),
        ReferenceProxy("list"),
        ReferenceProxy("set"),
        ReferenceProxy("tuple"),
        ReferenceProxy("isinstance"),
        ReferenceProxy("list"),
        ReferenceProxy("isinstance"),
        ReferenceProxy("set"),
        # TODO: These don't appear in references, because they are
        # stored in varnames. Maybe we should return new variables as
        # references too.
        # TypePrefix.LIST,
        # TypePrefix.SET,
        # TypePrefix.TUPLE, Note that this appears later.
        ReferenceProxy("str"),
        ReferenceProxy("len"),
        ReferenceProxy("encode"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.TUPLE,
        hasher._check_and_ingest,
        ReferenceProxy("isinstance"),
        ReferenceProxy("dict"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.DICT,
        ReferenceProxy("str"),
        ReferenceProxy("len"),
        ReferenceProxy("encode"),
        ReferenceProxy("items"),
        hasher._check_and_ingest,
        hasher._check_and_ingest,
        ReferenceProxy("isinstance"),
        ReferenceProxy,
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.REF_PROXY,
        ReferenceProxy("val.encode"),
        inspect.isbuiltin,
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.BUILTIN,
        ReferenceProxy("__module__"),
        ReferenceProxy("__name__"),
        hasher._check_and_ingest,
        inspect.isroutine,
        ReferenceProxy("__module__"),
        ReferenceProxy("__module__.startswith"),
        is_internal_file,
        ReferenceProxy("__code__.co_filename"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.INTERNAL_ROUTINE,
        ReferenceProxy("__module__"),
        ReferenceProxy("__name__"),
        hasher._check_and_ingest,
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.ROUTINE,
        get_code_context,
        hasher._check_and_ingest,
        ReferenceProxy("__defaults__"),
        hasher._ingest_code,
        ReferenceProxy("__code__"),
        inspect.iscode,
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.CODE,
        hasher._ingest_code,
        inspect.isclass,
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.CLASS,
        ReferenceProxy("__name__.encode"),
        hasher._ingest_raw_prefix_and_bytes,
        TypePrefix.DEFAULT,
        False,  # hasher._supress_warnings
        oneline,
        ReferenceProxy("type"),
        warnings.warn,
    ]
