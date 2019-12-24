import re
import importlib

from .extras import extras_require as package_desc_lists_by_extra


ILLEGAL_NAME_CHAR = re.compile('[^a-zA-Z0-9\\-._\\[\\]]')


# This really belongs in util.py, but that module depends on this one, so
# we define it here and then import it there.
def oneline(string):
    '''
    Shorten a multiline string into a single line, by replacing all newlines
    (and their surrounding whitespace) into single spaces. Convenient for
    rendering error messages, which are most easily written as multiline
    string literals but more readable as single-line strings.
    '''
    return ' '.join(
        substring.strip()
        for substring in string.split('\n')
        if substring.split()
    )


def first_token_from_package_desc(desc):
    first_mismatch = ILLEGAL_NAME_CHAR.search(desc)
    if first_mismatch is None:
        return desc

    if desc[first_mismatch.start()] not in ' <>=':
        raise AssertionError(oneline(f'''
            Package descriptor {desc!r} contained
            unexpected character {desc[first_mismatch.start()]!r}'''))

    return desc[:first_mismatch.start()]


# For packages that we don't import by the exact package name, these are
# aliases we use.
alias_lists_by_package = {
    'google-cloud-storage': ['google.cloud.storage'],
    'Pillow': ['PIL.Image'],
    'dask[dataframe]': ['dask.dataframe']
}

# Now we contruct a new data structure to allow us to give helpful error
# messages when the user tries to import a package that's not available.
extras_by_importable_name = {}
for extra, package_descs in package_desc_lists_by_extra.items():
    for package_desc in package_descs:
        package = first_token_from_package_desc(package_desc)

        # Associate this package with the extra it belongs to -- as long as
        # we haven't seen this package before.  (Because we're iterating over
        # an OrderedDict, this will end up associating each package with the
        # first extra that requires it, which should also be the most specific
        # extra, and therefore the most helpful one to mention in an error
        # message.)
        if package not in extras_by_importable_name:
            extras_by_importable_name[package] = extra

            if package in alias_lists_by_package:
                for importable_name in alias_lists_by_package[package]:
                    assert importable_name not in extras_by_importable_name
                    extras_by_importable_name[importable_name] = extra

# This is a fake entry for testing, since it's annoying to mock this.
TEST_EXTRA_NAME = '_FAKE_TEST_EXTRA_'
TEST_PACKAGE_NAME = '_FAKE_TEST_PACKAGE_'
extras_by_importable_name[TEST_PACKAGE_NAME] = TEST_EXTRA_NAME


# This is based on a similar function in Pandas:
# https://github.com/pandas-dev/pandas/blob/8ea102acdb45bb70cb30ea77108a50054c28c24d/pandas/compat/_optional.py
def import_optional_dependency(name, purpose=None, raise_on_missing=True):
    """
    Attempts to import a Python module that may or may not be available.  If
    it's not available, this function throws an ImportError explaining what the
    user needs to install.  (Unless ``raise_on_missing`` is set to False, in
    which case it returns None.)
    """

    if name not in extras_by_importable_name:
        raise AssertionError(oneline(f'''
            Attempted to import {name!r},
            which is not registered as a dependency'''))

    # TODO Once we have specific version requirements for our optional
    # packages, we should check that the version is correct.

    try:
        return importlib.import_module(name)
    except ImportError:
        if raise_on_missing:
            extra_name = extras_by_importable_name[name]

            if purpose is None:
                description = 'required'
            else:
                description = 'required for ' + purpose

            raise ImportError(oneline(f'''
                Unable to import package {name!r}, which is {description};
                you can use ``pip install bionic[{extra_name}]``
                to resolve this'''))

        else:
            return None
