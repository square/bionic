====================
Flow and FlowBuilder
====================

Introduction
------------

``FlowBuilder`` and ``Flow`` are the primary interfaces for constructing and
running Bionic flows.  Either of them can be used to represent
the collection of interdependent entities that make up a single analysis.  The
difference is that a ``FlowBuilder`` is a mutable object which can be updated,
while a ``Flow`` is an immutable object which can perform computation.

The typical pattern is to start with an empty ``FlowBuilder``, incrementally
add entity definitions to it, then use ``FlowBuilder.build()`` to generate a
``Flow``.  This ``Flow`` can be used immediately to compute entity values, or
passed to other code, which might reconfigure or extend it.

Although ``Flow`` objects are immutable, there is a mechanism for modifying
them: instead of a method like ``set`` that mutates the ``Flow``, there is a
``setting`` method that returns a new copy with the requested change.  This
allows ``Flow``\ s to be easily customized without worrying about shared state.
However, this API can only be used to update existing entities; if you want to
define new entities, you'll need to convert the ``Flow`` back to a
``FlowBuilder`` using ``to_builder``.

See `the Concepts documentation
<../concepts.rst#flows-flowbuilders-and-entities>`_ for more details.

FlowBuilder API
---------------

.. autoclass:: bionic.FlowBuilder
    :members:

FlowCase API
............

.. autoclass:: bionic.flow.FlowCase
    :members:

Flow API
--------

.. autoclass:: bionic.Flow
    :members:

