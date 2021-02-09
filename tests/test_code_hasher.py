import attr
import cmath
import contextlib
import logging
import pytest
from sklearn import linear_model
from textwrap import dedent
import threading
import types

from bionic.code_hasher import CodeHasher, TypePrefix
from .helpers import import_code


global_var_10 = 10
global_var_10_copy = 10
global_var_20 = 20


def test_code_hasher():
    def barray(value):
        return bytearray(value, "utf8")

    circular_dict_1_a = {"k11": "v1"}
    circular_dict_1_b = {"k21": "v2"}
    circular_dict_1_c = {"k31": "v3"}
    circular_dict_1_a["k12"] = circular_dict_1_b
    circular_dict_1_b["k22"] = circular_dict_1_c
    circular_dict_1_c["k32"] = circular_dict_1_a

    circular_dict_2_a = {"k11": "v1"}
    circular_dict_2_b = {"k21": "v2"}
    circular_dict_2_c = {"k31": "v3"}
    circular_dict_2_a["k12"] = circular_dict_2_b
    circular_dict_2_b["k22"] = circular_dict_2_c
    circular_dict_2_c["k32"] = circular_dict_2_b

    def f1():
        v = 10
        return v

    def f2():
        v = 20
        return v

    def f3():
        v = "10"
        return v

    def f4():
        return "10"

    def g1():
        return global_var_10

    def g2():
        return global_var_20

    free_var_10 = 10
    free_var_20 = 20

    def free1():
        return free_var_10

    def free2():
        return free_var_20

    def fref1():
        return f1()

    def fref2():
        return f2()

    def inc(x):
        return x + 1

    def dec(x):
        return x - 1

    def one():
        return 1

    def inc_with_one(x):
        return x + one()

    def dec_with_one(x):
        return x - one()

    def quadratic_eq(a, b, c):
        d = b ** 2 - 4 * a * c
        s1 = (b - cmath.sqrt(d)) / (2 * a)
        s2 = (-b - cmath.sqrt(d)) / (2 * a)
        return (s1, s2)

    def logistic_reg(train_frame, random_seed, hyperparams_dict):
        m = linear_model.LogisticRegression(
            solver="liblinear", random_state=random_seed, **hyperparams_dict
        )
        m.fit(train_frame.drop("target", axis=1), train_frame["target"])
        return m

    def a_lot_of_consts(train_frame, random_seed, hyperparams_dict):
        docstring = """
        This function uses a few constants and demonstrates that Bionic
        can hash all of them without any issues.
        """
        logging.log(docstring)  # Log these variables to avoid F841 errors.
        add_numbers = lambda x, y: x + y  # noqa: E731
        four = add_numbers(2, 2)
        logging.log(four)
        seven = add_numbers(3, 4)
        logging.log(seven)

        a, b, c = (1, -30, 200)
        (s1, s2) = quadratic_eq(a, b, c)
        assert [s1, s2] == [10, 20]

    def f_with_defaults1(x=10, y=20):
        return x + y

    def f_with_defaults2(x=20, y=10):
        return x + y

    def f_with_defaults3(x=10.0, y=20):
        return x + y

    def f_docstring1():
        """Docstring1"""
        pass

    def f_docstring2():
        """Docstring2"""
        pass

    def fib(n):
        return fib(n - 1) + fib(n - 2)

    def nested():
        v = 1

        def inner():
            logging.info(v)
            w = 5

            def innermost():
                logging.info(v, w)

    class ClassDefault:
        v = 1

    class ClassWithInit:
        v = 1

        def __init__(self):
            self.a = 1

    class ClassWithDifferentInit:
        v = 1

        def __init__(self):
            self.a = 2

    class ClassWithInnerClass:
        v = 1

        def __init__(self):
            self.a = 1

        class InnerClass:
            def __init__(self):
                self.i = 1

    class ClassWithDifferentInnerClass:
        v = 1

        def __init__(self):
            self.a = 1

        class InnerClass:
            def __init__(self):
                self.i = 2

    class ClassWithMethod:
        def v(self):
            return 1

    class ClassWithClassMethod1:
        @classmethod
        def v(cls):
            return 10

    class ClassWithClassMethod2:
        @classmethod
        def v(cls):
            return 20

    class ClassWithProperty:
        def __init__(self):
            self._a = 1

        @property
        def a(self):
            return self._a

        @a.setter
        def a(self, value):
            self._a = value

    class ClassWithDiffProperty:
        def __init__(self):
            self._a = 1

        @property
        def a(self):
            return self._a + 1

        @a.setter
        def a(self, value):
            self._a = value

    class ClassWithDynamicAttrLikeProperty:
        def __init__(self):
            self._a = 1

        @property
        def a(self):
            return self._a

    class ClassWithDynamicAttr:
        def __init__(self):
            self._a = 1

        @types.DynamicClassAttribute
        def a(self):
            return self._a

    class ClassWithDiffDynamicAttr:
        def __init__(self):
            self._a = 1

        @types.DynamicClassAttribute
        def a(self):
            return self._a + 1

    @attr.s(frozen=True)
    class AttrClassFrozen:
        a = attr.ib()

    @attr.s
    class AttrClass:
        a = attr.ib()

    @attr.s
    class AttrClassWithDefaults:
        a = attr.ib(default=1)

    @attr.s
    class AttrClassWithMultipleMembers:
        a = attr.ib()
        b = attr.ib()

    @attr.s
    class AttrClassWithMetadata:
        a = attr.ib(metadata={"a": 1})

    values = [
        b"",
        b"123",
        b"None",
        barray("bytearray"),
        barray("anotherbytearray"),
        None,
        ...,
        NotImplemented,
        "",
        "None",
        "String1",
        "String2",
        "0",
        "1",
        "123",
        "1.23",
        0,
        1,
        123,
        23,
        1.23,
        23.0,
        complex(0),
        complex(1),
        complex(123),
        complex(1, 23),
        float("inf"),
        float("-inf"),
        float("nan"),
        True,
        False,
        [],
        [1, 2, 3],
        [1, 2, "3"],
        [1, 2],
        (),
        (1, 2, 3),
        (1, 2, "3"),
        (1, 2),
        {},
        {1, 2, 3},
        {1, 2, "3"},
        {1, 2},
        frozenset(),
        frozenset({1, 2, 3}),
        frozenset({1, 2, "3"}),
        frozenset({1, 2}),
        range(1, 2, 1),
        range(1, 3, 1),
        range(1, 3, 2),
        {0: "v1", 1: None, "2": ["value1", "value2"]},
        {0: "v1", 1: None, "2": ["value1", "value2"], None: "none_val"},
        {
            0: "v1",
            1: {10: "v2", 20: {100: [200, 300]}},
            "2": ["value1", "value2"],
            None: "none_val",
        },
        circular_dict_1_a,
        circular_dict_2_a,
        types.MappingProxyType({}),
        types.MappingProxyType({0: "v1", 1: None, "2": ["value1", "value2"]}),
        f1,
        f2,
        f3,
        f4,
        g1,
        g2,
        free1,
        free2,
        fref1,
        fref2,
        inc,
        dec,
        inc_with_one,
        dec_with_one,
        lambda x: x * 2,
        lambda x: x / 2,
        lambda: None,
        quadratic_eq,
        logistic_reg,
        a_lot_of_consts,
        f_with_defaults1,
        f_with_defaults2,
        f_with_defaults3,
        f_docstring1,
        f_docstring2,
        fib,
        nested,
        ClassDefault,
        ClassWithInit,
        ClassWithDifferentInit,
        ClassWithInnerClass,
        ClassWithDifferentInnerClass,
        ClassWithMethod,
        ClassWithClassMethod1,
        ClassWithClassMethod2,
        ClassWithProperty,
        ClassWithDiffProperty,
        ClassWithDynamicAttrLikeProperty,
        ClassWithDynamicAttr,
        ClassWithDiffDynamicAttr,
        AttrClassFrozen,
        AttrClass,
        AttrClassWithDefaults,
        AttrClassWithMultipleMembers,
        AttrClassWithMetadata,
        TypePrefix,
        TypePrefix.BYTES,
        TypePrefix.BYTEARRAY,
    ]

    values_with_complex_types = [
        lambda x=threading.Lock(): x,
        threading.Lock(),
    ]

    idx_by_hash_value = {}
    for idx, val in enumerate(values + values_with_complex_types):
        if idx >= len(values):
            ctx_mgr = pytest.warns(UserWarning, match="Found a complex object")
        else:
            ctx_mgr = contextlib.suppress()

        with ctx_mgr:
            hash_value = CodeHasher.hash(val)
            # Hashing again should return the same hash value.
            assert CodeHasher.hash(val) == hash_value
            assert (
                hash_value not in idx_by_hash_value
            ), f"{values[idx]} and {values[idx_by_hash_value[hash_value]]} have the same hash"
            idx_by_hash_value[hash_value] = idx


def test_set_order():
    s1 = set([1, "A", 2, 3, 3, 3, 3])
    s2 = {3, 2, 1, 2, 3, "A"}
    s3 = {"A", 3, 3, 3, 2, 1, "A"}
    s4 = {"A", 3, 3, 3, 2, 1, "B"}
    check_hash_equivalence([[s1, s2, s3], [s4]])


def test_complex_type_warning():
    lock = threading.Lock()
    cond = threading.Condition()
    with pytest.warns(
        UserWarning,
        match="Found a complex object",
    ):
        assert CodeHasher.hash(lock) == CodeHasher.hash(cond)

    with pytest.warns(None) as warnings:
        assert CodeHasher.hash(lock, True) == CodeHasher.hash(cond, True)
    assert len(warnings) == 0


def test_code_type_refs_warning():
    code1 = import_code.__code__
    code2 = check_hash_equivalence.__code__

    def print_code1():
        logging.info(code1)

    def print_code2():
        logging.info(code2)

    with pytest.warns(
        UserWarning,
        match="Found a complex object",
    ):
        assert CodeHasher.hash(print_code1) == CodeHasher.hash(print_code2)

    with pytest.warns(None) as warnings:
        assert CodeHasher.hash(print_code1, True) == CodeHasher.hash(print_code2, True)
    assert len(warnings) == 0


def test_same_func_different_names():
    def f1():
        v = 10
        return v

    def f2():
        v = 10
        return v

    check_hash_equivalence([[f1, f2]])


def test_global_variable_references():
    def f1():
        return global_var_10

    def f2():
        return global_var_10_copy

    def f3():
        return global_var_20

    check_hash_equivalence([[f1, f2], [f3]])


def test_free_variable_references():
    free_var_10 = 10
    free_var_10_copy = 10
    free_var_20 = 20

    def f1():
        return free_var_10

    def f2():
        return free_var_10_copy

    def f3():
        return free_var_20

    check_hash_equivalence([[f1, f2], [f3]])


def test_function_references():
    def ref10():
        return 10

    def ref10_copy():
        return 10

    def ref20():
        return 20

    def f1():
        return ref10()

    def f2():
        return ref10_copy()

    def f3():
        return ref20()

    check_hash_equivalence([[f1, f2], [f3]])


def test_class_references():
    # References change in dunder methods.
    class C1:
        def __init__(self):
            self.v = global_var_10

    class C2:
        def __init__(self):
            self.v = global_var_10_copy

    class C3:
        def __init__(self):
            self.v = global_var_20

    check_hash_equivalence([[C1, C2], [C3]])

    # References change in normal methods.
    class C1:
        def my(self):
            return global_var_10

    class C2:
        def my(self):
            return global_var_10_copy

    class C3:
        def my(self):
            return global_var_20

    check_hash_equivalence([[C1, C2], [C3]])

    # References change in properties.
    class C1:
        @property
        def my(self):
            return global_var_10

    class C2:
        @property
        def my(self):
            return global_var_10_copy

    class C3:
        @property
        def my(self):
            return global_var_20

    check_hash_equivalence([[C1, C2], [C3]])

    check_hash_equivalence([[type(1), type(2)], [type("str1"), type("str2")]])


def test_changes_in_references():
    v = 10

    def f():
        return v

    old_hash = CodeHasher.hash(f)
    assert old_hash == CodeHasher.hash(f)

    # Hash for f should change if we change v.
    v = 20
    new_hash = CodeHasher.hash(f)
    assert new_hash == CodeHasher.hash(f)
    assert old_hash != new_hash

    def f1():
        return 1

    def count(v):
        if v == 0:
            return 0
        return count(v - 1) + f1()

    old_hash = CodeHasher.hash(count)
    assert old_hash == CodeHasher.hash(count)

    # Hash for count should change if we change f1.
    def f1():  # noqa: F811
        return 2

    new_hash = CodeHasher.hash(count)
    assert new_hash == CodeHasher.hash(count)
    assert old_hash != new_hash


@pytest.mark.parametrize("is_module_internal", [True, False])
def test_changes_in_another_module(is_module_internal):
    f_mod_code = """
    def f_mod():
        return 1
    """
    m = import_code(dedent(f_mod_code), is_module_internal=is_module_internal)

    def f():
        return m.f_mod()

    old_hash = CodeHasher.hash(f)
    assert old_hash == CodeHasher.hash(f)

    # Hash for f should not change if we change f_mod when module is
    # external.
    f_mod_code = """
    def f_mod():
        return 2
    """
    m = import_code(dedent(f_mod_code), is_module_internal=is_module_internal)

    new_hash = CodeHasher.hash(f)
    assert new_hash == CodeHasher.hash(f)
    if is_module_internal:
        assert old_hash == new_hash
    else:
        assert old_hash != new_hash


def check_hash_equivalence(groups):
    """
    Checks that hashes for every element in a given group are the same
    and hashes for elements between the groups are different. It also
    hashes the elements in every group twice to test that the hash is
    stable.
    """

    all_hashes = set()
    for group in groups:
        group_hashes = set()
        for f in group:
            group_hashes.add(CodeHasher.hash(f))
            group_hashes.add(CodeHasher.hash(f))
        assert len(group_hashes) == 1
        all_hashes.add(next(iter(group_hashes)))

    assert len(all_hashes) == len(groups)
