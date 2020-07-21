import pytest


def test_longest_regex_prefix():
    from .helpers import longest_regex_prefix_match

    def longest_prefix(regex, string):
        return longest_regex_prefix_match(regex, string).re.pattern

    assert longest_prefix("test", "test") == "test"
    assert longest_prefix("test", "te") == "te"
    assert longest_prefix("test", "text") == "te"
    assert longest_prefix("test", "testtest") == "test"
    assert longest_prefix("zest", "test") == ""
    assert longest_prefix("(test)", "test") == "(test)"
    assert longest_prefix("(test)", "text") == ""
    assert longest_prefix("(test)test", "testtest") == "(test)test"
    assert longest_prefix("(test)test", "testtext") == "(test)te"
    assert longest_prefix("x\n\n\nx", "x\n\n\nx") == "x\n\n\nx"
    assert longest_prefix("x\n\n\nx", "x\n\n\ny") == "x\n\n\n"
    assert longest_prefix("x\n\n\nx", "x\n\ny") == "x\n\n"
    assert longest_prefix("x\n\n\nx", "y\n\n\nx") == ""
    assert longest_prefix("test.*test", "testtest") == "test.*test"
    assert longest_prefix("test.*test", "testxxtest") == "test.*test"
    assert longest_prefix("test.*test", "testxxzest") == "test.*t"
    assert longest_prefix("test.*test", "testxxz") == "test.*"
    assert longest_prefix("test.*test", "texttest") == "te"


def test_assert_re_matches():
    from .helpers import assert_re_matches

    def assert_re_nomatch(regex, string):
        with pytest.raises(AssertionError):
            assert_re_matches(regex, string)

    assert_re_matches("test", "test")
    assert_re_matches("test", "testxxx")
    assert_re_nomatch("test", "tesd")

    assert_re_matches("test$", "test")
    assert_re_nomatch("test$", "testx")

    assert_re_matches(".*test", "test")
    assert_re_matches(".*test", "xxtest")
    assert_re_matches(".*test", "testxx")
    assert_re_nomatch(".*test", "tesd")

    assert_re_matches("(test)", "test")
    assert_re_matches("(test)", "testx")
    assert_re_nomatch("(test)", "tesd")

    assert_re_matches("test.*test", "testtest")
    assert_re_matches("test.*test", "testxxtest")
    assert_re_nomatch("test.*test", "test\ntest")

    assert_re_matches("(?s)test.*test", "test\ntest")
