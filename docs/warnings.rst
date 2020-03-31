=====================
Warnings and Pitfalls
=====================

This is Alpha Software
----------------------

Bionic is still in an early stage of development.  It also uses several
concepts which haven't been well-explored in this domain -- for example, the
way we create and gather multiple entity values.  We might discover that we've
created the wrong abstractions and need to adjust the API in a
non-backwards-compatible way.  To avoid being disrupted by breaking changes,
we recommend pinning a specific version of Bionic in your ``requirements.txt``
file:

.. parsed-literal::

    bionic==\ |version|

Similarly, since Bionic hasn't seen a lot of real-world use, there will
undoubtedly be some bugs -- you can get help with them `here <get-help.rst>`_.

Avoid Global State
------------------

Bionic assumes that your functions are completely self-contained: they don't
depend on or modify any external variables or systems.  This assumption is
necessary for parallelizing your code and caching the results.  If you break
it, your entity values may be inconsistent and unpredictable.

If you find yourself needing a global value, it's best to move that value into
its own entity.  (This means you need to define the value in such a way that it
doesn't visibly change after you've created and initialized it.)  This approach
has several advantages:

* Bionic can ensure that the value is properly initialized before any
  downstream entities access it.
* If you change the value's definition, Bionic can detect it and know to
  recompute those downstream entities.
* If you share your flow to another user or module, they will be able to
  redefine the value themselves using ``Flow.setting``.

Here are some situations you might encounter:

* If you have a global constant, consider making it an entity.
* If you have a dependency like a database connection, that can be an entity
  too.  You may want to use :meth:`@persist(False) <bionic.persist>` to make
  sure it only gets saved in memory, not to disk.
* If you need to initialize some external global dependency like Matplotlib,
  that should also happen in an entity.  In this case the entity may not have a
  meaningful value to return, but you can return a dummy value and use
  ``@persist(False)`` to make sure the initialization always happens.
* If you're reading data from an external source, like a database, that's a bit
  tricky.  Obviously Bionic has no way to detect whether the database's state
  has changed -- and even if it has, you might not want to re-run all your
  computations every time there's a slight update.  In some cases you can avoid
  this problem by constructing a query whose results shouldn't change; for
  example, reading over a specific time interval from an append-only database.
  Otherwise, your only option will be to manually :func:`@version
  <bionic.version>` the entity that reads from the database.
* If you're emitting debug information, like log messages, that's fine.  The
  important thing is that the log messages don't affect the behavior of any
  other entities.
* If you have a complex algorithm where multiple entities are reading from and
  writing to shared state, you'll want to refactor it into self-contained
  functions that return immutable values.  If there's no way to do that, your
  other option is just to move that entire algorithm into a single entity.

Don't Modify Your Arguments
---------------------------

After computing an entity's value, Bionic stores it in an in-memory cache to
avoid having to recompute it again.  This means that each entity function's
arguments come from a common cache shared among all entities.  If you modify
any of these arguments, those changes may affect other entities (depending on
what order they are computed in).  So don't do this:

.. code-block:: python

    @builder
    def augmented_frame(raw_frame):
        aug_frame = raw_frame
        # This adds a column to the original frame!
        aug_frame['duration'] = aug_frame['end_time'] - aug_frame['start_time']
        return aug_frame

Instead do this:

.. code-block:: python

    @builder
    def augmented_frame(raw_frame):
        aug_frame = raw_frame.copy()
        aug_frame['duration'] = aug_frame['end_time'] - aug_frame['start_time']
        return aug_frame

Watch Out For Stale Data
------------------------

When using a system like Bionic that tries to intelligently cache your data
for you, there's always the risk that it will fail to detect that it needs to
recompute something, and instead give you an old cached value.  (There's also
the risk that it will do unnecessary recomputation, but that's easier to
detect, and only wastes time rather than giving you incorrect results.)

There are two main situations where this can happen:

1. You've changed some of your code, but didn't use :func:`@version
   <bionic.version>` to tell Bionic about the change. You can use
   :ref:`automatic-versioning` to help avoid this.

2. You're working in a notebook and accessing a flow defined in a Python module
   file, and you've changed the definition of the flow but haven't reloaded the
   module.  See :ref:`reloading-flows` for an easy way to do this.

It's a good idea to `enable logging <concepts.rst#logging>`_ at the ``INFO`` level so you
can see what Bionic is doing -- this makes it much more obvious when it's
failing to recompute values for you.
