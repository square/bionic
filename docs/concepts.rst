=====================
Concepts and Features
=====================

Flows, FlowBuilders, and Entities
---------------------------------

The two fundamental objects in Bionic are the *Entity* and the *Flow*.  An
*entity* is any of the Python objects that makes up an analysis: a dataframe, a
parameter, a model, a plot, even a database connection.  Each entity has a
unique name and a value, which may be either fixed or derived from other
entities.

For example, a very simple analysis might have four entities:

* some raw data (defined as a fixed value)
* the cleaned-up data (a function of the raw data)
* a statistical model (a function of the clean data)
* a plot showing the fit of the model (a function of the model and the clean
  data)

When grouped together in an analysis, these entities make up a *flow*.  The
goal of Bionic is to make it easy to assemble flows, run them to compute their
component entities, and eventually share and re-use them.

Bionic has two classes for representing flows: :class:`Flow <bionic.Flow>` and
:class:`FlowBuilder <bionic.FlowBuilder>`.  They both represent the same data
model, but ``FlowBuilder`` is *mutable* and intended for *building* a flow,
while ``Flow`` is *immutable* and intended for *running* or *sharing* a flow.

The basic mechanics of building and running flows are illustrated in the
`Hello World tutorial`_.

.. _Hello world tutorial: tutorials/hello_world.ipynb

Declaring, Setting, Assigning, and Deriving Entities
....................................................

Once a FlowBuilder is created, entities can be defined and updated in a few
different ways:

.. code-block:: python

    import bionic as bn

    builder = bn.FlowBuilder('my_flow')

    # Creates a new entity and assigns it a fixed value.
    builder.assign('greeting', 'Hello')

    # Declares an entity but doesn't set any value.
    builder.declare('subject')

    # Sets the value of an existing entity.  Can overwrite previously-set
    # values.
    builder.set('subject', 'world')

    # Creates a new entity whose value is derived from other entities.
    # The entity name and its dependencies are inferred from the function
    # name and arguments.
    # (Can also overwrite an existing entity definition.)
    @builder
    def message(greeting, subject):
        return '{0} {1}!'.format(greeting, subject)

The point of the distinction between ``declare``, ``set``, and ``assign`` is to
make it explicit whether you're creating a new entity or updating an existing
one.  In particular, when working with a flow created by someone else, it
would otherwise be easy to attempt to change an existing entity value, but
mistype the name and create a new, unused entity.

Building And Running Flows
..........................

To run a flow, we ``build`` the ``FlowBuilder`` into a ``Flow``, then use
``get`` to compute the value of any of the entities:

.. code-block:: python

    flow = builder.build()

    # Returns 'Hello world!'
    flow.get('message')

Defining Multiple Outputs Using Decorators
..........................................

When creating derived entities, Bionic infers the inputs and output of your
entity from the Python function you provide.  This provides a very convenient
way to define relationships between entities -- but sometimes we want to
specify more complicated behavior.  For these cases Bionic provides special
`decorators`_.

.. _decorators: api/decorators.rst

For example, sometimes we have one function that returns multiple distinct
values.  These can be assigned to different entities with the :func:`@outputs
<bionic.outputs>` decorator:

.. code-block:: python

    @builder
    @bn.outputs('first_name', 'last_name')
    def split_name(full_name):
        first_name, last_name = full_name.split()
        return first_name, last_name

(Since we're explicitly providing the names of the output entities, the name of
the function is ignored here.)

Bionic provides several built-in `decorators`_ that modify how a function is
interpreted and converted to an entity (or entities).  In the `future
<future.rst#user-defined-decorators>`__, it will be possible for users to write
their own decorators as well.

Configuration with Internal Entities
....................................

In additional to the entities defined by the user, each ``Flow`` has a
collection of "internal" entities which control its behavior.  For example,
the built-in ``core__persistent_cache__global_cache_dir`` entity controls the
location of Bionic's persistent cache.  Internal entities are usually omitted
from user-facing lists and visualizations, but they can be accessed and
modified by name just like regular entities.

Caching and Protocols
---------------------

Whenever Bionic computes an entity's value, it automatically caches that value
in memory (in case you access it again in from the same ``Flow`` object) and in
persistent storage (in case you want to access it later, perhaps after
restarting your script or notebook).  Currently the only supported location for
persistent storage is your computer's hard disk.

Bionic's caching can be seen in action in the `ML tutorial`_.

.. _ML tutorial: tutorials/ml_workflow.ipynb

Cache Invalidation and Versioning
.................................

If parts of your flow change, old cached entries may become invalid and need to
be recomputed.  This is not an issue for the in-memory cache -- it is
associated with a specific ``Flow`` object, which is immutable, so if you
create a new ``Flow`` instance its in-memory cache will be empty.  However,
with the persistent cache, the situation is more involved.

There are three ways for a cached value to become invalid:

1. A new value is defined for that entity, such as via :meth:`FlowBuilder.set
   <bionic.FlowBuilder.set>` or :meth:`Flow.setting <bionic.Flow.setting>`.
2. The entity is defined as a function, and one of its dependencies becomes
   invalid.
3. The entity is defined as a function, and the code of that function is
   changed.

Bionic can detect cases 1 and 2 automatically: if you update the value of any
entity in your flow, all downstream cached values will automatically be
invalidated, and they will be recomputed from scratch next time they're
requested [#f1]_.  However, case 3 is difficult to detect automatically, so we
provide a special :func:`@version <bionic.version>` decorator to tell Bionic
when a function's code has changed.  For example, if we've defined a
``message`` entity:

.. code-block:: python

    @builder
    def message(greeting, subject):
        return '{0} {1}!'.format(greeting, subject)

If we want to change the code that generates ``message``, we attach the
decorator:

.. code-block:: python

    @builder
    @bionic.version(1)
    def message(greeting, subject):
        return '{greeting} {subject}!!!'.format(greeting, subject).upper()

If the function has a different ``version`` from the cached value, the cached
value will be disregarded and a new value will be recomputed.  Each subsequent
time we change this function, we just increment the version number.

.. [#f1] Bionic detects changes by hashing all of the fixed entity values, and
  storing each computed value alongside a hash of all its inputs.

Disabling Persistent Caching
............................

In some cases, it doesn't make sense to make a persistent copy of an entity's
value, either because the value is much cheaper to compute than to store, or
because the value has a type that's difficult to serialize.  In these cases,
we can disable persistent caching altogether:

.. code-block:: python

    @builder
    @bionic.persist(False)
    def message(subject):
        return 'Hello {subject}.'.format(subject=subject)

Location of the Cache Directory
...............................

By default, Bionic persists cached values on the local disk, in a directory
called ``bndata/$NAME_OF_FLOW``.  This can be configured by modifying one of
two internal entities:

.. code-block:: python

    builder = bionic.FlowBuilder('my_flow')

    # Cache this flow's data in /my_cache_dir/my_flow/
    builder.set('core__persistent_cache__global_dir', 'my_cache_dir')

    # Cache this flow's data in /my_cache_dir/
    builder.set('core__persistent_cache__flow_dir', 'my_cache_dir')

In the `future <future.rst#cloud-storage>`__, it will be possible to configure
Bionic to cache data in cloud storage (such as GCS) instead of on the local
disk.

Serialization Protocols
.......................

In order to persistently cache an entity's value -- which is a Python object --
Bionic needs to `serialize <https://en.wikipedia.org/wiki/Serialization>`_ the
value, converting it to a series of bytes which can be stored in a file.
Conversely, to retrieve the value from the cache, those bytes need to be
deserialized back into a Python object.  The best way to serialize and
deserialize a given value depends on its type.

Most Python objects can be serialized with Python's built-in `pickle
<https://docs.python.org/3/library/pickle.html>`_ module.  However, for some
object types it's more efficient or more idiomatic to use a different format.
There are also some types of objects that can't be pickled at all.  Bionic uses
``pickle`` by default, but handles some types specially: `Pandas
<https://pandas.pydata.org/>`_ DataFrames are serialized in the `Parquet
<https://parquet.apache.org/>`_ format, while `Pillow
<https://pillow.readthedocs.io/en/stable/>`_ Images are serialized as `PNG
<https://en.wikipedia.org/wiki/Portable_Network_Graphics>`_\ s.  You can
explictly specify a serialization strategy for an entity by attaching a
`Protocol`_ to its definition.

.. _Protocol: api/protocols.rst

Exporting Persisted Files
.........................

In some cases, you'll want to directly access the persisted file for an entity,
rather than its in-memory representation.  (For example, if you're writing a
a paper or report, you may want to access the files containing the plots.)
This can be accomplished using the :meth:`Flow.export <bionic.Flow.export>`
method.

Multiplicity
------------

So far we've only considered flows where each entity has a single value.
However, often we want several instances of a particular part of our flow.  To
facilitate this, Bionic allows any entity to be assigned multiple values at
once:

.. code-block:: python

    flow = builder.build()
    flow2 = flow.setting('subject', values=['Alice', 'Bob'])

If an entity has multiple values, we have to tell Bionic that we expect a
collection of values when we retrieve it:

.. code-block:: python

    # Returns `{'Alice', 'Bob'}`.
    flow2.get('subject', 'set')

The "multiplicity" of the ``subject`` entity is propagated to all downstream
entities as well:

.. code-block:: python

    # Returns `{'Hello Alice!', 'Hello Bob!'}`.
    flow2.get('message', 'set')

This can also be used on multiple entities at once:

.. code-block:: python

    flow4 = flow2.setting('greeting', values=['Hello', 'Hi'])

    # Returns `{'Hello Alice!', 'Hello Bob!', 'Hi Alice!', 'Hi Bob!}`.
    flow4.get('message', 'set')

The multiplicity feature is illustrated in more detail `later in the ML
tutorial <tutorials/ml_workflow.ipynb#Multiplicity>`_.

The Relational Model of Multiplicity
.....................................

Bionic uses a relational model to determine how many instances of each entity
to create.  In essence, each entity has a "table" of values.  For fixed
entities, the values are provided explicitly by the user; for derived entities,
they are constructed by a `join
<https://en.wikipedia.org/wiki/Join_(SQL)>`_-like operation on the entity's
dependencies' tables.

For example, in the previous flow, we had two values of ``greeting`` and two
values of ``subject``, producing four values of ``message`` -- one for each
combination.  In other words, we took the `Cartesian product
<https://en.wikipedia.org/wiki/Cartesian_product>`_ of all possible inputs for
the ``message`` entity.

However, Bionic will only combine values that are "compatible" with each other.
For example:

.. code-block:: python

    builder.set('full_name', values=['Alice Adams', 'Bob Baker'])

    @builder
    def first_name(full_name):
        return full_name.split()[0]

    @builder
    def last_name(full_name):
        return full_name.split()[-1]

    @builder
    def reversed_name(first_name, last_name):
        return '{0}, {1}'.format(last_name, first_name)

    flow = builder.build()

    # Returns `{'Adams, Alice', 'Baker, Bob'}`.
    flow.get('reversed_name', 'set')

Even though ``reversed_name`` depends on ``first_name`` and ``last_name``, and
they each have two values, we don't use every possible combination.  Since
``first_name`` and ``last_name`` share an ancestor, we only combine values
derived from the same ancestor value.  ``"Alice"`` and ``"Baker"`` are derived
from different ``full_name``\ s, so they won't be combined together.

Gathering
.........

Often, if we have multiple instances of an entity, we eventually want to
aggregate those instances together and compare them somehow.  This is the
function of the :func:`@gather <bionic.gather>` decorator.

Returning to the "hello world" example:

.. code-block:: python

    builder.set('greeting', values=['Hello', 'Hi'])
    builder.set('subject', values=['Alice', 'Bob'])

    # Returns `{'Hello Alice!', 'Hello Bob!', 'Hi Alice!', 'Hi Bob!}`.
    builder.build().get('message', 'set')

    @builder
    @bn.gather(over='subject', also='message', into='gather_df')
    def message_for_all_subjects(gather_df):
        messages = gather_df.sort_values('subject')['message']
        return ' '.join(messages)

    # Return `{'Hello Alice! Hello Bob!', 'Hi Alice! Hi Bob!'}`
    builder.build().get('message_for_all_subjects', 'set')

The effect of ``@gather`` here is to "gather" together all the different
instances of ``subject`` into a single dataframe, along with the associated
values of ``message``.  Our ``message_for_all_subjects`` function then combines
those messages together into a single message.  The final result is an entity
with two distinct values.

Essentially, we create multiplicity with the ``values=`` keyword, and we remove
it with the ``@gather`` decorator.  In this example, we created multiplicity
across two dimensions (``greeting`` and ``subject``), and then removed one
dimension (``subject``), leaving one dimension remaining (``greeting``).

Notice also that ``@gather`` is treating the ``over`` argument differently from
the ``also`` argument; both are included in the dataframe, but only the former
affects the multiplicity of the resulting entity.  (Incidentally, either of
these arguments can also accept a list of strings instead of a single string.)

This model of multiplicity takes some getting used to, but the payoff is that
we only have to think about multiplicity in two places: where we create it,
and where we remove it.  Any intermediate entities are oblivious to how many
times they're instantiated.  This quality is also demonstrated in the `same
tutorial section <tutorials/ml_workflow.ipynb#Multiplicity>`_.

Case-by-Case Assignment
.......................

Normally, Bionic infers which entity values can be combined with others based
on their ancestry.   However, sometimes we want to explicitly specify which
values are "compatible" with each other.  In the situations, we can assign
values by "case" instead of by entity.

.. code-block:: python

    builder.declare('color')
    builder.declare('animal')

    builder.add_case('color', 'black', 'animal', 'cat')
    builder.add_case('color', 'brown', 'animal', 'cat')
    builder.add_case('color', 'brown', 'animal', 'fox')

    @builder
    def colored_animal(color, animal):
        return '{0} {1}'.format(color, animal)

    # Returns `{'black cat', 'brown cat', 'brown fox'}`.
    builder.build().get('colored_animal', 'set')

Other Features
--------------

Plotting
........

Bionic is based on a `functional
<https://en.wikipedia.org/wiki/Functional_programming>`_ paradigm: the only
important thing about a function is the value it returns, rather than any side
effects it might have.  However, some plotting libraries -- most notably
`Matplotlib <https://matplotlib.org/>`_ -- don't work like this.  Instead,
they maintain a global, stateful canvas which the user incrementally writes to
and then visualizes.

Since plotting is a crucial part of data analysis, Bionic bridges this gap by
providing a :func:`@pyplot <bionic.pyplot>` decorator, which translates a
function using the Matplotlib API into a regular Bionic entity whose value is
a `Pillow <https://pillow.readthedocs.io/en/stable/>`_ Image object.

.. code-block:: python

    @builder
    @bn.pyplot('my_plt')
    def my_plot(dataframe, my_plt):
        my_plt.scatter(x=dataframe['time'], y=dataframe['profit'])

    # Returns an Image object containing the plot.
    builder.build().get('my_plot')

Logging
.......

Bionic uses the built-in Python `logging
<https://docs.python.org/2/library/logging.html>`_ module to log what it does.
Currently it doesn't attempt to configure any log handlers, since that's
conventionally the responsibility of the application rather than a library.
This means that you will only see log messages that meet Python's default
severity threshold: ``WARNING`` and above.  To see a running log of what Bionic
is computing, set the threshold to ``INFO``.  You can do this with the
``bionic.util.init_basic_logging`` convenience function.

In the future, Bionic will probably have a configurable option to initialize
the logging state itself.  It will also provide an easy way for entity
functions to access individually-named loggers, rather than having to create
them themselves.

.. _reloading-flows:

Reloading Flows in Notebooks
............................

One of Bionic's design goals is to make it easy for flows to be defined in
Python module files but accessed in notebooks.  However, one challenge is that
when a module file is updated, the change is not reflected in the notebook --
instead, the module has to be manually reloaded, and then the flow object has to
be re-imported.

.. code-block:: python

    from my_module import flow

    ...

    import my_module
    reload(my_module)
    from my_module import flow
    flow.get('my_entity')

(Jupyter's `autoreload
<https://ipython.org/ipython-doc/3/config/extensions/autoreload.html>`_ doesn't
work here, because after reloading we still need to re-import the flow.)

To address this, the :meth:`Flow.reloading <bionic.Flow.reloading>` method can
be used:

.. code-block:: python

    from my_module import flow

    ...

    flow = flow.reloading()
    flow.get('my_entity')

This attempts to reload all modules associated with the flow, and then return
a re-imported version of the flow.  (This is a fairly magical procedure -- in
complicated cases, it may not be able to figure out how to do this.  In these
cases it will try to throw an exception rather than fail silently.)
