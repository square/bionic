====================
Utilities
====================

FileCopier
------------
When called with the ``mode='FileCopier'`` argument,
:meth:`Flow.get <bionic.Flow.get>` can return a
:class:`FileCopier <bionic.util.FileCopier>` instance.  This is simply a
utility class that exposes a
:meth:`copy <bionic.util.FileCopier.copy>` method, enabling the
user to copy files around without knowing any internal details about where
Bionic stores them.

FileCopier API
---------------

.. autoclass:: bionic.util.FileCopier
    :members: