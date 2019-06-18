"""
These are the decorators we expose to Bionic users.  They are used as follows:

    @builder.derive
    @bionic.decorator1
    @bionic.decorator2
    ...
    def resource_name(arg1, arg2, ...):
        ...

"""

from __future__ import absolute_import

from past.builtins import basestring
from .resource import (
    VersionedResource, GatherResource, AttrUpdateResource, PyplotResource,
    RenamingResource, NameSplittingResource, resource_wrapper,
)
from . import interpret


# TODO Consider making this an argument to the persist decorator?
def version(version_):
    """
    Identifies the version of a Python function.

    When you change the implementation of a resource function, you should add
    or change its version identifier so that Bionic knows to invalidate any
    cached values of that function and re-compute them.

    You may also want to update the version when there are changes in functions
    or libraries that the resource calls, or in any external data source (like
    a database) that the resource accesses.

    Parameters
    ----------

    version_: Integer or string
        An arbitary identifier.

    Returns
    -------
    Function:
        A decorator which can be applied to a resource function.
    """

    if not (isinstance(version_, basestring) or isinstance(version_, int)):
        raise ValueError("Version must be an int or string; got %r" % version_)

    return resource_wrapper(VersionedResource, version_)


# In the future I expect we'll have other caching options -- disabling in-memory
# caching, allowing caching for shorter periods, etc. -- but I'm not sure what
# the API should look like.
def persist(enabled):
    """
    Indicates whether computed values should be cached persistently.

    Parameters
    ----------

    enabled: Boolean
        Whether this resource's values should be persisted (e.g., to local
        disk).

    Returns
    -------
    Function:
        A decorator which can be applied to a resource function.
    """

    if not isinstance(enabled, bool):
        raise ValueError("Argument must be a boolean; got %r" % enabled)

    return resource_wrapper(AttrUpdateResource, 'should_persist', enabled)


def output(name):
    """
    Renames a resource.  The resource must have a single value.

    When this is used to decorate a resource function, the provided name is
    used as the resource name instead of the function's name.

    Parameters
    ----------

    name: String
        The new name for the resource.

    Returns
    -------
    Function:
        A decorator which can be applied to a resource function.
    """

    return resource_wrapper(RenamingResource, name)


def outputs(*names):
    """
    Indicates that a result produces a (fixed-size) collection of values, and
    assigns a name to each value.

    When this is used to decorate a resource function, the function will
    actually define multiple resources, one for each provided name.  The
    decorated function must return a sequence with exactly as many values as
    the provided list of names.

    Any other decorators which would normally modify the definition of the
    resource (such as protocols) will be applied to each of the final
    resources.

    Parameters
    ----------

    names: Sequence of strings
        The names of the defined resources.

    Returns
    -------
    Function:
        A decorator which can be applied to a resource function.
    """

    return resource_wrapper(NameSplittingResource, names)


# TODO I'd like to put a @protocols decorator here that exposes the
# MultiProtocolUpdateResource class, but that would collide with the
# protocols.py module.  Let's do this in a later PR.


def gather(over, also=None, into='gather_df'):
    """
    Gathers multiple instances of resources into a single dataframe.

    Gathers all values of the ``over`` resource(s) along with associated values
    of the ``also`` resource(s) into a single dataframe, which is provided to
    the decorated function as an argument whose name is determined by
    ``into``.


    Parameters
    ----------
    over: String or sequence of strings
        Primary names to collect.  Any cases that differ only in these names
        will be grouped together in the same frame.
    also: String or sequence of strings
        Secondary names to include.  These resource values are added to the
        frame but don't affect the grouping.
    into: String, optional (default ``'gather_df'``)
        The argument name of the gathered frame.

    Returns
    -------
    Function:
        A decorator which can be applied to a resource function.


    Example usage:

    .. code-block:: python

        builder = FlowBuilder('my_flow')

        builder.assign('color', values=['red', 'blue'])
        builder.assign('shape', values=['square', 'circle'])

        @builder
        def colored_shape(color, shape):
            return color + ' ' + shape

        @builder
        @gather('color', 'colored_shape', 'df')
        def all_color_shapes(df):
            return ', '.join(gather_df.colored_shape.sort_values())

        flow = builder.build()

        flow.get('colored_shape', set)
        # Returns {'red square', 'blue square', 'red circle', 'blue circle'}

        flow.get('all_color_shapes', set)
        # Returns {'blue square, red square', 'blue circle, red circle'}
        # Note that the colored shapes are gathered into two groups: within
        # each group, the color varies but the shape does not.

    """
    over = interpret.str_or_seq_as_list(over)
    also = interpret.str_or_seq_or_none_as_list(also)
    return resource_wrapper(
        GatherResource,
        primary_names=over, secondary_names=also, gathered_dep_name=into)


def pyplot(name=None):
    """
    Provides a Matplotlib pyplot module to the decorated resource.

    By default the module is provided as an argument named ``"pyplot"``, but
    this can be changed with the ``name`` argument.  The resource's Python
    function should use the pyplot module to create a plot, but should not
    return any values.  The output of the final resource will be a
    ``Pillow.Image`` containing the plot.

    Parameters
    ----------
    name: String, optional (default "pyplot")
        The argument name of the module provided to the decorated function.

    Returns
    -------
    Function:
        A decorator which can be applied to a resource function.
    """

    DEFAULT_NAME = 'pyplot'
    if callable(name):
        func_or_resource = name
        wrapper = resource_wrapper(PyplotResource, DEFAULT_NAME)
        return wrapper(func_or_resource)

    if name is None:
        name = DEFAULT_NAME
    return resource_wrapper(PyplotResource, name)


immediate = persist(False)
immediate.__doc__ = """
Guarantees that a resource can computed during bootstrap resolution.

Currently ``@immediate`` is equivalent to ``@persist(False)``.
"""
