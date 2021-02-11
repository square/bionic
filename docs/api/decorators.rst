==========
Decorators
==========

Introduction
------------

Bionic decorators are Python decorators designed to be used in conjunction with
a ``FlowBuilder``.  They modify the way functions are incorporated into flows.

The normal way (without decorators) of incorporating functions into flows is
as follows:

.. code-block:: python

    import bionic as bn

    builder = FlowBuilder('my_flow')

    builder.assign('x', 1)

    @builder
    def x_plus_one(x):
        return x + 1

    print(builder.build().get('x_plus_one'))  # Prints "2".

In the simple case above, the function is interpreted as a new entity named
``x_plus_one`` which depends on the existing entity ``x``.  However, in many
cases we want Bionic to process the function in a more complex way.  In these
cases we can add additional decorators:

.. code-block:: python

    import bionic as bn

    builder = FlowBuilder('my_flow')

    builder.assign('x', 1)

    @builder
    @bn.outputs('x_plus_one', 'x_plus_two')
    @bn.persist(False)
    def some_function(x):
        return (x + 1), (x + 2)

    print(builder.build().get('x_plus_one'))  # Prints "2".
    print(builder.build().get('x_plus_two'))  # Prints "3".

These decorators tell Bionic that our function actually generates two values
for two different entities (``x_plus_one`` and ``x_plus_two``), and these
values should not be persisted to disk.

All Bionic decorators should be placed *after* the initial ``@builder``
decorator, but *before* any regular (non-Bionic) decorators.  Finally, the
``@builder`` decorator returns the original function, so it can be called
normally, as if it had been defined without any of the Bionic decorators.
E.g.:

.. code-block:: python

    @builder
    @bn.persist(False)
    def f(x):
        return x + 1

    assert f(7) == 8

Built-In Decorators
-------------------

.. autofunction:: bionic.run_in_aip
.. autofunction:: bionic.changes_per_run
.. autofunction:: bionic.docs
.. autofunction:: bionic.gather
.. autofunction:: bionic.immediate
.. autofunction:: bionic.memoize
.. autofunction:: bionic.output
.. autofunction:: bionic.outputs
.. autofunction:: bionic.persist
.. autofunction:: bionic.pyplot
.. autofunction:: bionic.version

