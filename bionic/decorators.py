'''
These are the decorators we expose to Bionic users.  They are used as follows:

    @builder.derive
    @bionic.decorator1
    @bionic.decorator2
    ...
    def resource_name(arg1, arg2, ...):
        ...

'''

from resource import (
    VersionedResource, GatherResource, PersistedResource, resource_wrapper)
import interpret


# TODO Consider making this an argument to the persist decorator?
def version(version_):
    if not (isinstance(version_, basestring) or isinstance(version_, int)):
        raise ValueError("Version must be an int or string; got %r" % version_)

    return resource_wrapper(VersionedResource, version_)


def gather(over, also, into='gather_df'):
    over = interpret.str_or_seq_as_list(over)
    return resource_wrapper(GatherResource, over, also, into)


persist = resource_wrapper(PersistedResource)
