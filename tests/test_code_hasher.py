import cmath
import contextlib
import pytest
import threading

from bionic.code_hasher import CodeHasher, TypePrefix


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

    def inc(x):
        return x + 1

    def dec(x):
        return x - 1

    def quadratic_eq(a, b, c):
        d = b ** 2 - 4 * a * c
        s1 = (b - cmath.sqrt(d)) / (2 * a)
        s2 = (-b - cmath.sqrt(d)) / (2 * a)
        return (s1, s2)

    def logistic_reg(train_frame, random_seed, hyperparams_dict):
        from sklearn import linear_model

        m = linear_model.LogisticRegression(
            solver="liblinear", random_state=random_seed, **hyperparams_dict
        )
        m.fit(train_frame.drop("target", axis=1), train_frame["target"])
        return m

    def a_lot_of_consts(train_frame, random_seed, hyperparams_dict):
        import logging

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

    values = [
        b"",
        b"123",
        b"None",
        barray("bytearray"),
        barray("anotherbytearray"),
        None,
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
        f1,
        f2,
        f3,
        f4,
        inc,
        dec,
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
    ]

    values_with_complex_types = [
        lambda x=threading.Lock(): x,
        threading.Lock(),
    ]

    idx_by_hash_value = {}
    for idx, val in enumerate(values + values_with_complex_types):
        if idx >= len(values):
            ctx_mgr = pytest.warns(UserWarning, match="Found a constant")
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


def test_complex_type_warning():
    val = threading.Lock()
    with pytest.warns(
        UserWarning,
        match="Found a constant",
    ):
        assert CodeHasher.hash(val) == CodeHasher.hash(TypePrefix.DEFAULT)


def test_same_func_different_names():
    def f1():
        v = 10
        return v

    def f2():
        v = 10
        return v

    assert CodeHasher.hash(f1) == CodeHasher.hash(f1)
    assert CodeHasher.hash(f2) == CodeHasher.hash(f2)
    assert CodeHasher.hash(f1) == CodeHasher.hash(f2)
