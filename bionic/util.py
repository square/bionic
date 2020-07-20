"""
This module is deprecated and exists only for backwards compatibility.

Some older documentation recommended using `bionic.util.init_basic_logging` to expose
Bionic's logs. This function is now located at `bionic.utils.misc.init_basic_logging`.
Eventually we should remove the need for this function and deprecate it there too.
"""

from .utils.misc import init_basic_logging  # noqa: F401
