"""
These are the decorators we expose to Bionic users.  They are used as follows:

    @builder.derive
    @bionic.decorator1
    @bionic.decorator2
    ...
    def entity_name(arg1, arg2, ...):
        ...

"""

from .datatypes import CodeVersion
from .provider import (
    GatherProvider,
    AttrUpdateProvider,
    PyplotProvider,
    RenamingProvider,
    NameSplittingProvider,
    provider_wrapper,
)
from . import interpret


def version(major=None, minor=None):
    """
    Identifies the version of a Python function.  The version has two
    components: a major version and a minor version.  Each of these can be
    either an integer or a string, and defaults to ``0``.

    When you change the implementation of an entity function, you should update
    its version so that Bionic knows to whether invalidate any cached values of
    that function and re-compute them.  An update in *major* version indicates
    a *functional* change: Bionic will assume that the function can return
    different output and won't use any cached artifacts created by a previous
    version of the function.  An update in *minor* version indicates a
    *nonfunctional* change, such as a refactoring or performance optimization:
    Bionic will assume that the function behaves the same for all inputs, and
    will continue using cached artifacts as long as the major version still
    matches.  Updating the minor version is only required when using Bionic's
    "assisted versioning" mode.

    You may also want to update the major version when there are changes in
    functions or libraries that the entity function calls, or in any external
    data source (like a database) that the function accesses.

    Parameters
    ----------

    major: Integer or string (default 0)
        An arbitrary identifier for a function's behavior.

    minor: Integer or string (default 0)
        An arbitrary identifier for a function's nonfunctional characteristics.

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.
    """

    return provider_wrapper(
        AttrUpdateProvider, "code_version", CodeVersion(major, minor)
    )


# In the future I expect we'll have other caching options -- disabling in-memory
# caching, allowing caching for shorter periods, etc. -- but I'm not sure what
# the API should look like.
def persist(enabled):
    """
    Indicates whether computed values should be cached persistently.

    Parameters
    ----------

    enabled: Boolean
        Whether this entity's values should be persisted (e.g., to local
        disk).

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.
    """

    if not isinstance(enabled, bool):
        raise ValueError(f"Argument must be a boolean; got {enabled!r}")

    return provider_wrapper(AttrUpdateProvider, "_can_persist", enabled)


def memoize(enabled):
    """
    Indicates whether computed values should be cached in memory.

    Parameters
    ----------

    enabled: Boolean
        Whether this entity's values should be memoized.

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.
    """

    if not isinstance(enabled, bool):
        raise ValueError(f"Argument must be a boolean; got {enabled!r}")

    return provider_wrapper(AttrUpdateProvider, "_can_memoize", enabled)


def changes_per_run(enabled=None):
    """
    Indicates whether this function is non-deterministic: i.e., if it’s called multiple
    times with the same inputs, can it return different outputs?

    When ``enabled`` is true, Bionic will recompute this function's value (and
    potentially the values of anything depending on it) each time this flow is
    instantiated, rather than reusing a value cached on disk. For example, if the
    function queries data from an external database, the results may be different each
    time even if the query stays the same.

    However, for practical reasons, Bionic won't compute a new value more than once
    within a single run. That is, once it's been computed for a particular Flow
    instance, that value will be saved in memory and reused. This is a compromise:
    logically it makes sense to recompute it every time, but it's much simpler to have
    a single fixed value for each entity within a given flow instance. For this reason,
    when this decorator is enabled, memoization must not be disabled for this entity.

    Note that ``@changes_per_run`` is not the same as ``@persist(False)``. For example,
    the following code will not necessarily query the database each time:

    .. code-block:: python

        @builder
        @bn.persist(False)
        @builder
        def current_data():
            return download_data()

        @builder
        def summary(current_data):
            return summarize(current_data)

    This fails because if we call ``flow.get('summary')`` and Bionic finds a cached
    value, it will return the cached value because it doesn't know that current_data
    ought to be recomputed. On the other hand, if we replace ``persist(False)`` with
    ``changes_per_run`` -- as in the example below -- then ``current_data`` will be
    recomputed each time (and ``summary`` will be recomputed if ``current_data``
    changes).

    Parameters
    ----------

    enabled: Boolean, optional (default ``True``)
        Whether this function's output changes per run.

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.


    Example usage:

    .. code-block:: python

        @builder
        @bn.changes_per_run
        @builder
        def current_data():
            return download_data()

        @builder
        def summary(current_data):
            return summarize(current_data)
    """

    DEFAULT_VALUE = True
    if callable(enabled):
        func_or_provider = enabled
        wrapper = provider_wrapper(AttrUpdateProvider, "changes_per_run", DEFAULT_VALUE)
        return wrapper(func_or_provider)

    if enabled is None:
        enabled = DEFAULT_VALUE

    if not isinstance(enabled, bool):
        raise ValueError(f"Argument must be a boolean; got {enabled!r}")

    return provider_wrapper(AttrUpdateProvider, "changes_per_run", enabled)


def output(name):
    """
    Renames an entity.  The entity function must have a single value.

    When this is used to decorate an entity function, the provided name is
    used as the entity name, instead of using the function's name.

    Parameters
    ----------

    name: String
        The new name for the entity.

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.
    """

    return provider_wrapper(RenamingProvider, name)


def outputs(*names):
    """
    Indicates that a result produces a (fixed-size) collection of values, and
    assigns a name to each value.

    When this is used to decorate an entity function, the function will
    actually define multiple entities, one for each provided name.  The
    decorated function must return a sequence with exactly as many values as
    the provided list of names.

    Any other decorators which would normally modify the definition of the
    entity (such as protocols) will be applied to each of the final entities.

    Parameters
    ----------

    names: Sequence of strings
        The names of the defined entities.

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.
    """

    return provider_wrapper(NameSplittingProvider, names)


def docs(*docs):
    """
    Assigns documentation strings to the entities defined by the decorated
    function. Typically used in conjuction with ``@outputs`` for functions
    that return multiple entity values. (In the more common case where your
    function returns a single entity value, you can just use a regular Python
    docstring.)

    Parameters
    ----------

    docs: Sequence of strings
        Documentation strings for each of the defined entities.

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.
    """

    return provider_wrapper(AttrUpdateProvider, "docs", docs)


# TODO I'd like to put a @protocols decorator here that exposes the
# MultiProtocolUpdateProvider class, but that would collide with the
# protocols.py module.  Let's do this in a later PR.


def gather(over, also=None, into="gather_df"):
    """
    Gathers multiple instances of entities into a single dataframe.

    Gathers all values of the ``over`` entity (or entities) along with
    associated values of the ``also`` entity (or entities) into a single
    dataframe, which is provided to the decorated function as an argument whose
    name is determined by
    ``into``.


    Parameters
    ----------
    over: String or sequence of strings
        Primary names to collect.  Any cases that differ only in these names
        will be grouped together in the same frame.
    also: String or sequence of strings
        Secondary names to include.  These entity values are added to the
        frame but don't affect the grouping.
    into: String, optional (default ``'gather_df'``)
        The argument name of the gathered frame.

    Returns
    -------
    Function:
        A decorator which can be applied to a entity function.


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
            return ', '.join(df.colored_shape.sort_values())

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
    return provider_wrapper(
        GatherProvider, primary_names=over, secondary_names=also, gathered_dep_name=into
    )


def pyplot(name=None, savefig_kwargs=None):
    """
    Provides a Matplotlib pyplot module to the decorated entity function.

    By default the module is provided as an argument named ``"pyplot"``, but
    this can be changed with the ``name`` argument.  The entity's Python
    function should use the pyplot module to create a plot, but should not
    return any values.  The output of the final entity will be a
    ``Pillow.Image`` containing the plot.

    Parameters
    ----------
    name: String, optional (default "pyplot")
        The argument name of the module provided to the decorated function.
    savefig_kwargs: Dict, optional
        Additional arguments to pass to `matplotlib.pytplot.savefig` when
        converting the plot to an image.  By default, passes ``format=png`` and
        ``bbox_inches="tight"``; any arguments passed in this dict will
        override the default values.

    Returns
    -------
    Function:
        A decorator which can be applied to an entity function.
    """

    DEFAULT_NAME = "pyplot"
    if callable(name):
        func_or_provider = name
        wrapper = provider_wrapper(PyplotProvider, DEFAULT_NAME)
        return wrapper(func_or_provider)

    if name is None:
        name = DEFAULT_NAME
    return provider_wrapper(PyplotProvider, name, savefig_kwargs)


immediate = persist(False)
immediate.__doc__ = """
Guarantees that an entity can computed during bootstrap resolution.

Currently ``@immediate`` is equivalent to ``@persist(False)``.
"""
