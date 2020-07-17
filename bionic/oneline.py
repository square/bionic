"""
Contains the oneline method, which is very useful for formatting error messages.

This is in its own module, rather than somewhere in ``util``, because it's used in
``deps`` and ``deps`` is used by ``util``, so putting it here avoids a circular
dependency.
"""


def oneline(string):
    """
    Shorten a multiline string into a single line, by replacing all newlines
    (and their surrounding whitespace) into single spaces. Convenient for
    rendering error messages, which are most easily written as multiline
    string literals but more readable as single-line strings.
    """
    return " ".join(
        substring.strip() for substring in string.split("\n") if substring.split()
    )
