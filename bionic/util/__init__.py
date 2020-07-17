"""
Contains reusable utility functions that don't have any Bionic-specific logic.
"""

# We expose this at the top level for backwards compatibility, since some of our
# documentation recommends using this function to expose Bionic's logs. Eentually we
# should remove the need for this function () and deprecate it.
from .misc import init_basic_logging  # noqa: F401
