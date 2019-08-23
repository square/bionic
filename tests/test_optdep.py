import pytest

from bionic.optdep import (
    import_optional_dependency, TEST_EXTRA_NAME, TEST_PACKAGE_NAME)


def test_import_missing_dependency():
    with pytest.raises(
            ImportError,
            match='.*%s.*PURPOSE.*pip install bionic\\[%s\\].*' % (
                TEST_PACKAGE_NAME, TEST_EXTRA_NAME)):
        import_optional_dependency(TEST_PACKAGE_NAME, purpose='PURPOSE')


def test_import_missing_dependency_without_raising():
    module = import_optional_dependency(
        TEST_PACKAGE_NAME, raise_on_missing=False)
    assert module is None


def test_import_unrecognized_dependency():
    with pytest.raises(AssertionError):
        import_optional_dependency("_UNKNOWN_PACKAGE_", purpose="PURPOSE")
