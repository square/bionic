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
        return f'{greeting} {subject}!'

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

Modifying Built Flows
.....................

Although ``Flow`` objects are immutable, they provide a :meth:`setting
<bionic.Flow.setting>` method that can be used to create a modified copy of a
flow:

.. code-block:: python

    new_flow = flow.setting('greeting', 'Goodbye').setting('subject', 'galaxy')

    # Returns "Goodbye galaxy!"
    new_flow.get('message')

    # Still returns "Hello world!"
    flow.get('message')

If more extensive changes are needed (such as creating new entities, or setting
derived entities), a ``Flow`` can also be converted back to a mutable
``FlowBuilder``:

.. code-block:: python

    new_builder = flow.to_builder()

    @new_builder
    def loud_message(message):
        return message.upper()

    # Returns "HELLO WORLD!"
    new_builder.build().get('loud_message')

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

Documenting Entities
.........................

Each Bionic entity can optionally have a documentation string associated with
it.  Entities defined by functions can use the regular Python docstring
syntax:

.. code-block:: python

    @builder
    def message(greeting, subject):
        """A nice thing to say to someone."""
        return f'{greeting} {subject}!'

If the function defines multiple entities, the :meth:`@docs <bionic.docs>`
decorator can be used to specify documentation for each one:

.. code-block:: python

    @builder
    @bn.outputs('first_name', 'last_name')
    @bn.docs('The first name.', 'The last name.')
    def split_name(full_name):
        first_name, last_name = full_name.split()
        return first_name, last_name

For entities with fixed values, an optional ``doc`` argument is available:

.. code-block:: python

    builder.assign('greeting', 'Hello', doc="A nice way to start a message.")
    builder.declare('subject', doc="The person we're talking to.")

These documentation strings are helpful for people reading your code, and are
sometimes visible to the users of your flow.  For example, Python's built-in
``help`` method can be used to view an entity's documentation:

.. code-block:: python

    help(flow.get.message)

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
in memory (in case you access it again in from the same ``Flow`` object) and to
disk (in case you want to access it later, perhaps after restarting your script
or notebook).  Bionic can also be configured to cache to
:ref:`Google Cloud Storage <google_cloud_storage_anchor>`.

Bionic's caching can be seen in action in the `ML tutorial`_.

.. _ML tutorial: tutorials/ml_workflow.ipynb

.. _cache-invalidation-and-versioning:

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
requested [#value_hash]_. However, case 3 is difficult to detect automatically, so we
provide a special :func:`@version <bionic.version>` decorator to tell Bionic when a
function's code has changed. For example, if we've defined a ``message`` entity:

.. code-block:: python

    @builder
    def message(greeting, subject):
        return f'{greeting} {subject}!'

If we want to change the code that generates ``message``, we attach the
decorator:

.. code-block:: python

    @builder
    @bionic.version(1)
    def message(greeting, subject):
        return f'{greeting} {subject}!!!'.upper()

If the function has a different ``version`` from the cached value, the cached
value will be disregarded and a new value will be recomputed.  Each subsequent
time we change this function, we just increment the version number.

.. [#value_hash] Bionic detects changes by hashing all of the fixed entity
  values, and storing each computed value alongside a hash of all its inputs.

.. _automatic-versioning:

Automatic Versioning
....................

.. versionadded:: 0.5.0

.. note::  This feature is somewhat experimental.  However, if it proves
    useful, we may make assisted versioning the default behavior in the future.

By default, Bionic expects you to manually update a version decorator each time
you modify a function's code.  However, it can be configured to automatically
detect code changes and warn you if the code changes but the version doesn't.
This "assisted versioning" behavior is enabled by changing Bionic's versioning
mode from ``'manual'`` to ``'assist'``:

.. code-block:: python

    builder.set('core__versioning_mode', 'assist')

In this mode, if Bionic finds a cached file created by a function with the
*same version* but *different code* [#risk_factors]_, it will raise a
``CodeVersioningError``. You can resolve this error by updating the :func:`@version
<bionic.version>`, which tells Bionic to ignore the cached file and compute a new
value.

.. code-block:: python

    # Trying to compute this new version of ``message`` will throw an exception.
    @builder
    def message(greeting, subject):
        return f'{greeting} {subject}!!!'.upper()

.. code-block:: python

    # With the version updated, Bionic knows to recompute this.
    @builder
    @bionic.version(1)
    def message(greeting, subject):
        return f'{greeting} {subject}!!!'.upper()

However, some code changes, such as refactoring or performance optimizations,
have no effect on the function's behavior; in this case we might prefer to keep
using the cached value.  If you're confident that your change has no effect,
you can provide a ``minor`` argument to ``@version``.  Bionic only uses the
first argument ("``major``") for cache invalidation; updating the ``minor``
argument tells Bionic to ignore the code differences and keep using any cached
file as long as the ``major`` version matches.

.. code-block:: python

    # Even though we changed the code, Bionic won't recompute this.
    @builder
    @bionic.version(major=1, minor=1)
    def message(greeting, subject):
        return f'{greeting} {subject}!!!'.upper()


Be aware that Bionic can't detect every change that can affect your code's
behavior. Bionic will inspect the bytecode of each function, as well as any
global variables and other functions it references [#bytecode_hash]_. However, it
won’t detect changes of the following types:

1. **Built-in or Installed Modules**: Bionic won’t detect changes in any built-in
   modules or installed functions (modules accessed through Python’s
   `installation paths <https://docs.python.org/3/library/sysconfig.html#installation-paths>`_).
   Bionic *will* detect changes to modules in the same directory as your code,
   and modules accessed through
   `PYTHONPATH <https://docs.python.org/3/using/cmdline.html#envvar-PYTHONPATH>`_.

2. **Global Variables with Complex Types**: Bionic will only detect changes in
   global variables that have simple types (int, string, bool, bytes, etc.), but
   it will ignore changes for other types of variables.

3. **Python Classes**: Bionic detects some but not all changes to the code of
   Python classes.

4. **Runtime Code**: Bionic doesn’t actually run any code when inspecting it, so
   it won’t recursively inspect functions that are referenced dynamically. If a
   function is not referenced straightforwardly (that is, as a global variable or
   as an attribute of a global variable), Bionic may not inspect it. For example:

   .. code-block:: python

    def f():
        a = get_a()  # We will inspect `get_a`.
        b = module.submodule.get_b()  # We will inspect `get_b`.
        import new_module
        c = new_module.get_c()  # We won’t inspect `get_c`.
        d = exec("get_d()")  # We definitely won’t inspect `get_d`!

If you're not worried about these false negatives, you can set Bionic to a "fully
automatic" mode:

.. code-block:: python

    builder.set('core__versioning_mode', 'auto')

In this mode, Bionic will automatically invalidate cached files whenever a
function's code changes, so you don't need to set a ``@version`` at all.
(However, you can still update the ``@version`` to tell Bionic about external
changes that it can't detect.)  This mode increases the risk of an undetected
change, but it may be more convenient when your code doesn't include any of the
risk factors listed above.

.. [#risk_factors] The details are described later in this section.

.. [#bytecode_hash] And any global variables and other functions they reference,
  and so on.


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
        return f'Hello {subject}.'

If your goal is just to force an entity to be recomputed more frequently, you may
want :ref:`@changes_per_run <changes_per_run>` instead.

Persistent caching can also be globally disabled:

.. code-block:: python

    builder.set('core__persist_by_default', False)

This only changes the default behavior, so it can be explicitly re-enabled for
individual entities:

.. code-block:: python

    builder.set('core__persist_by_default', False)

    @builder
    @bionic.persist(True)
    def message(subject):
        return f'Hello {subject}.'


Disabling In-Memory Caching
............................

In other cases, it might be useful to not keep an entity in memory, but store
it on disk and load it only when needed for downstream computation. This might
be useful when it is too expensive to keep all entities in memory. In these
cases, we can disable in-memory caching:

.. code-block:: python

    @builder
    @bionic.memoize(False)
    def message(subject):
        return f'Hello {subject}.'

Like persistent caching, in-memory caching can also be globally disabled:

.. code-block:: python

    builder.set('core__memoize_by_default', False)

This can also be explicitly re-enabled for individual entities:

.. TODO: We should be consistent between the usage of @bn and @bionic.

.. code-block:: python

    builder.set('core__memoize_by_default', False)

    @builder
    @bionic.memoize(True)
    def message(subject):
        return f'Hello {subject}.'

When both persistent and in-memory caching are disabled for an entity, Bionic
will cache its values in a temporary in-memory cache that only lasts for the
duration of the :meth:`Flow.get <bionic.Flow.get>` call. These temporarily-cached
values are discarded when the call is completed.


.. _changes_per_run :

Non-Deterministic Computation
.............................

.. versionadded:: 0.7.0

The basic assumption behind Bionic's caching behavior is that entity functions
are *deterministic*: if you call them multiple times with the same input, they
always return the same output.  However, some functions are
*non-deterministic*: their output can change even when their input doesn't.
For example, a function that retrieves data from an external database may
return different results whenever the database's contents change.  In
cases like this, it's not appropriate to reuse the function's previous cached
values; we want Bionic to recompute the value each time.

You can tell Bionic that a function is non-deterministic by applying the
:meth:`@changes_per_run <bionic.changes_per_run>` decorator:

.. code-block:: python

    @builder
    @bn.changes_per_run
    def current_data():
        return download_data()

This causes Bionic to recompute the entity's value instead of loading a
cached value from disk. (However, this recomputation will only happen once
for any given ``Flow`` instance; after that, the value will be cached in
memory and reused [#per_run]_.)

.. [#per_run] I.e., the value is computed once per "run".  This is a compromise:
  although it makes logical sense to recompute the value every single time,
  it's much simpler for each entity to have a consistent value within a single
  flow instance.

``@changes_per_run`` vs ``@persist``
::::::::::::::::::::::::::::::::::::

Note that ``@changes_per_run`` has a different effect from ``@persist(False)``.
If an entity is decorated with ``@persist(False)``, Bionic will never cache its
value to disk, but it will still assume that its output is deterministic. The
difference can be seen when we add a downstream entity:

.. code-block:: python

    @builder
    @bn.persist(False)
    def current_data():
        return download_data()

    @builder
    def summary(current_data):
        return summarize(current_data)

In this case, ``builder.build().get('current_data')`` will always recompute
``current_data``, since its value is never persisted. However,
``builder.build().get('summary')`` will use a cached value if one is available;
Bionic won't bother to recompute ``current_data`` because it assumes its value
will be the same anyway. In more complex flows, this incorrect assumption may
lead to inconsistent results.

By contrast, if we use the appropriate decorator, ``@bn.changes_per_run``:

.. code-block:: python

    @builder
    @bn.changes_per_run
    def current_data():
        return download_data()

    @builder
    def summary(current_data):
        return summarize(current_data)


Here ``builder.build().get('summary')`` will always recompute ``current_data``
first. Then, if ``current_data``'s value has changed, it will recompute
``summary`` as well; otherwise it will use a cached value.

As a rule: use ``@persist(False)`` for entities whose values are *impossible to
serialize* or *not worth serializing*. Use ``@changes_per_run`` for
entities whose values are *non-deterministic*.


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


.. _google_cloud_storage_anchor :

Caching in Google Cloud Storage
...............................

Bionic can be configured to cache to `Google Cloud Storage`_ as well
as on the local filesystem:

.. _Google Cloud Storage: https://cloud.google.com/storage/

.. code-block:: python

    builder = bionic.FlowBuilder('my_flow')

    # You need to have an existing, accessible GCS bucket already.
    builder.set('core__persistent_cache__gcs__bucket_name', 'my-bucket')
    builder.set('core__persistent_cache__gcs__enabled', True)

By default, Bionic stores its cached files with a prefix of
``$NAME_OF_USER/bndata/$NAME_OF_FLOW/``; this can be configured by setting the
``core__persistent_cache__gcs__object_path`` entity:

.. code-block:: python

    builder.set('core__persistent_cache__gcs__bucket_name', 'my-bucket')
    builder.set('core__persistent_cache__gcs__object_path', 'my/path/')
    builder.set('core__persistent_cache__gcs__enabled', True)

Alternatively, a single GCS URL can be provided:

.. code-block:: python

    builder.set('core__persistent_cache__gcs__url', 'gs://my-bucket/my/path/')
    builder.set('core__persistent_cache__gcs__enabled', True)

Bionic will load data from the GCS cache whenever it's not in the local cache,
and will write back to both caches.  Note that the upload time will make each
entity computation a bit slower.

In order to use GCS caching, you must have the `gcloud`_ cli tool installed, and
you must have GCP credentials configured.  You should also use ``pip install
'bionic[gcp]'`` to install the required Python libraries.

.. _gcloud: https://cloud.google.com/sdk/gcloud

.. _protocols :

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
``pickle`` by default, but handles some types specially:

- `JSON <https://www.json.org/json-en.html>`_-serializable built-in types (int, float,
  str, bool, list, and dict) are serialized as JSON files.
- `Pandas <https://pandas.pydata.org/>`_ DataFrames are serialized as
  `Parquet <https://parquet.apache.org/>`_ files.
- `NumPy <https://numpy.org/>`_ Arrays are serialized as `NPY
  <https://numpy.org/devdocs/reference/generated/numpy.lib.format.html#npy-format>`_
  files.
- `Pillow <https://pillow.readthedocs.io/en/stable/>`_ Images are serialized
  as `PNG <https://en.wikipedia.org/wiki/Portable_Network_Graphics>`_ files.
- `Dask <https://docs.dask.org/>`_ Dataframes are serialized as `Parquet
  <https://parquet.apache.org/>`_ files.
- `GeoPandas <https://geopandas.org/>`_ Dataframes are serialized as `SHP
  <https://en.wikipedia.org/wiki/Shapefile#Shapefile_shape_format_(.shp)>`_ files.

You can can explictly specify a serialization strategy for an entity by
attaching a `Protocol`_ to its definition.

.. _Protocol: api/protocols.rst

Retrieving Persisted Files
..........................
In some cases, you'll want to directly access the persisted file(s) for an
entity rather than its in-memory representation.  (For example, if you're
writing a paper or report, you may want to access the files containing the
plots.) This can be achieved with the ``mode`` argument to
:meth:`Flow.get <bionic.Flow.get>` method.  For example:

.. code-block:: python

    flow = builder.build().setting('subject', 'Alice')
    flow.get('subject', mode='path')

This would return a ``Path`` object for the ``subject`` entity.

.. _cache-api:

Programmatic Cache Access
.........................

.. versionadded:: 0.8.0

.. note::  This API is intentionally quite minimal; we intend to add additional
  convenience features based on observed usage patterns. If you'd like to add new
  features, feel free to submit an issue or a PR on GitHub!

Although Bionic attempts to manage the cache for you automatically, it's sometimes
helpful to be able to interact with it directly. Bionic provides a basic API for
exploring the cache:

.. code-block:: python

    for entry in flow.cache.get_entries():
        print(entry.artifact_url)

The :meth:`get_entries <bionic.cache_api.Cache.get_entries>` method returns a
sequence of :class:`CacheEntry <bionic.cache_api.CacheEntry>` objects, one for each
cached entity value. These objects contain information about the cached entity and
the location of the cache file itself (which may be either a local file or a cloud
blob).

Cached entries can also be safely deleted using the :meth:`delete
<bionic.cache_api.CacheEntry.delete>` method. This can be used to selectively clean
up the cache:

.. code-block:: python

    for entry in flow.cache.get_entries():
        if entry.tier == 'local' and entry.entity == 'model':
            entry.delete()

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
        return f'{last_name}, {first_name}'

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
        return f'{color} {animal}'

    # Returns `{'black cat', 'brown cat', 'brown fox'}`.
    builder.build().get('colored_animal', 'set')

Parallel and Cloud Execution
----------------------------

By default, Bionic computes values one at a time. Requesting an entity value
with :meth:`Flow.get <bionic.Flow.get>` can lead to a long computation, as
Bionic may need to compute that entity's dependencies, and their dependencies,
and so on.

Bionic can take advantage of more computing resources by using local
multiprocessing or Google AI Platform. The speedup depends on the structure of
the :ref:`dependency graph <dagviz>` and/or the type of computation performed.
See :ref:`Caveats <parallel-caveats>` for more details.

.. _parallel-execution:

Local Parallel Execution
........................

.. versionadded:: 0.8.0

Bionic can use multiple CPU cores to compute entities in parallel. This feature
is useful if you have many expensive operations which do not depend on each
other.

Parallel execution can be enabled like this:

.. code-block:: python

    builder.set("core__parallel_execution__enabled", True)

When parallel execution is enabled, Bionic starts up several worker processes
[#workers]_, each of which can work on one value at a time. By default, Bionic
will create one worker process for each CPU on your machine. This is usually a
sensible number, but it can also be set directly:

.. code-block:: python

    builder.set("core__parallel_execution__worker_count", 8)

.. [#workers] The pool of workers is managed by
  `Loky <https://loky.readthedocs.io/en/stable/>`_,
  which is built on Python's
  `multiprocessing <https://docs.python.org/3.8/library/multiprocessing.html>`_ module.
  The pool is global and reusable, so it should only need to be initialized once in
  the lifetime of the main process.

.. _google-aip:

Google AI Platform Execution
............................

.. versionadded:: 0.10.0

Bionic can use compute resources in the cloud by sending entities to
`Google AI Platform (AIP) <https://cloud.google.com/ai-platform>`_ for
computation.

.. code-block:: python

    builder.set('core__aip_execution__enabled', True)

Bionic will use your Google Cloud Platform credentials to figure out the project
ID for running AI Platform jobs. Alternatively, you can manually specify the
project ID:

.. code-block:: python

    builder.set('core__aip_execution__gcp_project_id', 'my-project')

This feature requires having a Docker image in Google Container Registry
containing the same Python environment (runtime and libraries) as the one used
in the local environment Bionic is running in.

Bionic can build this Docker image if all the Python libraries in the local
environment are installed through ``pip``. Otherwise, you will need to specify
the Docker image.

To use a docker image that you have uploaded to Google Container Registry:

.. code-block:: python

    builder.set('core__aip_execution__docker_image_name', 'bionic:latest')

To use a docker image from a different project id or registry, you can specify
the full path.

.. code-block:: python

    builder.set('core__aip_execution__docker_image_uri', 'gcr.io/other-project/bionic:latest')

Finally, :ref:`Google Cloud Storage <google_cloud_storage_anchor>` must be
enabled.

Entities marked with the :func:`@run_in_aip <bionic.run_in_aip>`
decorator will be computed on AIP instead of locally. The decorator takes in a
`Compute Engine machine type identifier <https://cloud.google.com/ai-platform/training/docs/machine-types#compute-engine-machine-types>`_
which determines the hardware that runs the AIP job. The decorated entity must
be serializable and cannot be marked with :func:`@persist(False) <bionic.persist>`.

.. code-block:: python

    @builder
    @bionic.run_in_aip('n1-standard-4')
    def x():
      return 1

Caveats
.......

There is overhead for doing computation in external processes. The overhead is
especially significant for Google AI Platform because it may take a few minutes
to spin up a new job instance. Thus, enabling parallel execution is beneficial
only if the computation time far exceeds the overhead.

The total speedup also depends on the structure of the :ref:`dependency graph <dagviz>`.
If there are not many branches in the graph, then there will not be a lot of
parallelism. Bionic can only start computing a value once all its dependencies are
complete.

In order to compute an entity value in a separate process, Bionic needs to serialize
the entity function and transmit it to the other process; thus, all your functions
need to be serializable by `cloudpickle <https://github.com/cloudpipe/cloudpickle>`_.
(This shouldn't be a problem unless your function uses some kind of complex global
variable, which is already a `bad idea <warnings.rst#avoid-global-state>`_.) The
entity value itself doesn't necessarily need to be picklable; it will be serialized
using the :ref:`protocol<protocols>` specified for the entity.

Entities with non-serializable values (marked with
:func:`@persist(False) <bionic.persist>`) may be recomputed multiple times.
Such entities cannot be sent to another process, hence every process instance
(local or in Google AI Platform) that needs them for downstream nodes will need
to compute them separately. Try to avoid expensive functions that produce
non-serializable output.

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

To address this, the :meth:`Flow.reload <bionic.Flow.reload>` method can
be used:

.. code-block:: python

    from my_module import flow

    ...

    flow.reload()
    flow.get('my_entity')

This attempts to reload all modules associated with the flow, and then updates
the flow instance to use the reloaded modules.  (This is a fairly magical
procedure -- in complicated cases, it may not be able to figure out how to do
this.  In these cases it will try to throw an exception rather than fail
silently.) All future operations on the flow will reflect its updated status.

To ensure that any code changes are detected properly, you will need to use
versioning (see :ref:`cache-invalidation-and-versioning` and
:ref:`automatic-versioning`). Otherwise, the flow may keep using cached values
from previous versions of the code.

You can also use the :meth:`Flow.reloading <bionic.Flow.reloading>` method to
get a new copy of the flow that uses reloaded modules, without modifying the
original flow instance.

.. code-block:: python

  from my_module import flow

  ...

  new_flow = flow.reloading()
  new_flow.get('my_entity')

Combining Flows
...............

When building a flow, you can import entities from another flow using the
:meth:`merge <bionic.FlowBuilder.merge>` method:

.. code-block:: python

    builder.merge(flow)

This allows you to extend the functionality of a flow, or to combine multiple
flows into one.  You can also combine two already-built flows using the
analogous :meth:`merging <bionic.Flow.merging>` method.

If the two flows being merged have any entity names in common and Bionic can't
figure out which one to keep, it will throw an exception.  You can resolve the
conflict by using the ``keep`` argument to specify which definitions to keep:

.. code-block:: python

    builder.merge(flow, keep='old')

.. _dagviz :

Visualizing Flows
.................

Bionic can visualize any flow as a `directed acyclic graph
<https://en.wikipedia.org/wiki/Directed_acyclic_graph>`_, or "DAG":

.. code-block:: python

    flow.render_dag()

Each entity in the flow is represented as a box, with arrows representing
dependencies (the arrow points from the depended-on entity to the depending
one).  See the `ML tutorial`_ an example.  This functionality `requires the
Graphviz library`_.

.. _requires the Graphviz library: get-started.rst#installation
