'''
These are the decorators we expose to Bionic users.  They are used as follows:

    @builder.derive
    @bionic.decorator1
    @bionic.decorator2
    ...
    def resource_name(arg1, arg2, ...):
        ...

'''
from __future__ import absolute_import

from past.builtins import basestring
from .resource import (
    VersionedResource, GatherResource, AttrUpdateResource, PyplotResource,
    resource_wrapper)
from . import interpret


# TODO Consider making this an argument to the persist decorator?
def version(version_):
    '''
    Attaches a numeric identifier to distinguish different iterations of the
    same Python function.
    '''
    if not (isinstance(version_, basestring) or isinstance(version_, int)):
        raise ValueError("Version must be an int or string; got %r" % version_)

    return resource_wrapper(VersionedResource, version_)


# In the future I expect we'll have other caching options -- disabling in-memory
# caching, allowing caching for shorter periods, etc. -- but I'm not sure what
# the API should look like.
def persist(enabled):
    '''
    Indicates whether computed values should be cached persistently.
    '''
    if not isinstance(enabled, bool):
        raise ValueError("Argument must be a boolean; got %r" % enabled)

    return resource_wrapper(AttrUpdateResource, 'should_persist', enabled)


def gather(over, also=None, into='gather_df'):
    '''
    Gathers all values of the `over` resources along with associated values of
    the `also` resources into a single dataframe argument with name `into`.

    Example usage:

    @builder
    @gather('hyperparameters', ['model', 'error'])
    def best_model(gather_df):
        best_row = gather_df.sort_values('error').iloc[0]
        return best_row.model
    '''
    over = interpret.str_or_seq_as_list(over)
    also = interpret.str_or_seq_or_none_as_list(also)
    return resource_wrapper(
        GatherResource,
        primary_names=over, secondary_names=also, gathered_dep_name=into)


def pyplot(name=None):
    '''
    Provides a Matplotlib pyplot module to the decorated resource.  By default
    the module is provided as an argument named "pyplot", but this can be
    changed with the `name` argument.  The resource's Python function should
    use the pyplot module to create a plot, but should not return any values.
    The output of the final resource will be a Pillow Image containing the
    plot.
    '''

    DEFAULT_NAME = 'pyplot'
    if callable(name):
        func_or_resource = name
        wrapper = resource_wrapper(PyplotResource, DEFAULT_NAME)
        return wrapper(func_or_resource)

    if name is None:
        name = DEFAULT_NAME
    return resource_wrapper(PyplotResource, name)


immediate = persist(False)
immediate.__doc__ = '''
Guarantees that a resource can computed during bootstrap resolution; e.g.,
doesn't get cached, etc.
'''
