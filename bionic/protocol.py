from __future__ import absolute_import

from . import protocols

# These are callable with or without arguments.  See BaseProtocol.__call__ for
# why we instantiate them here.
picklable = protocols.PicklableProtocol()  # noqa: F401
dillable = protocols.DillableProtocol()  # noqa: F401
frame = protocols.DataFrameProtocol()  # noqa: F401
image = protocols.ImageProtocol()  # noqa: F401
numpy = protocols.NumPyProtocol()  # noqa: F401

# These need to be called with arguments.
enum = protocols.EnumProtocol  # noqa: F401
type = protocols.TypeProtocol  # noqa: F401
