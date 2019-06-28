===========
Protocols
===========

Introduction
------------

Protocols are special cases of Bionic decorators; their effect is to specify
the `Serialization Protocol <../concepts.rst#serialization-protocols>`_ for the
entity being defined.  For example:

.. code-block:: python

    # This entity should only have values equal to "short" or "long".
    @builder
    @bn.protocol.enum('short', 'long')
    def name_length(name):
        if len(name) < 10:
            return 'short'
        else:
            return 'long'

    # This entity's value will always be a ``pandas.DataFrame``.
    @builder
    @bn.protocol.DataFrame
    def raw_df():
        from sklearn import datasets
        dataset = datasets.load_breast_cancer()
        df = pd.DataFrame(
            data=dataset.data,
        )
        df['target'] = dataset.target
        return df

Protocols are used to tell Bionic how to serialize, deserialize, and validate
entity values.  In most cases, Bionic's default protocol can figure out an
appropriate way to handle each value, so explicit protocol decorators are
usually not required.  However, they can be useful for data types that need
special handling, or just to add clarity, safety, or documentation to a
entity definition.

Protocols can also be used when creating new entities with ``declare`` or
``assign``:

.. code-block:: python

    builder.assign('name_length', 'short', bn.protocol.enum('short', 'long'))
    builder.declare('raw_df', bn.protocol.DataFrame)

Custom Protocols
----------------

If you need to control how an entity is serialized, you can write your own
custom protocol.  (However, since Bionic is still at an early stage, future
API changes may break your implementation.)

.. code-block:: python

    class MyProtocol(BaseProtocol):
        def get_fixed_file_extension(self):
            """Returns the extension that persisted files should end with."""
            raise NotImplementedError()

        def write(self, value, file_):
            """Write the contents of ``value`` to file object ``file_``."""
            raise NotImplementedError()

        def read(self, file_):
            """Read an object from file object ``file_``, and return it."""
            raise NotImplementedError()

Protocol Decorators
-------------------

.. autofunction:: bionic.protocol.dillable
.. autofunction:: bionic.protocol.enum
.. autofunction:: bionic.protocol.frame
.. autofunction:: bionic.protocol.image
.. autofunction:: bionic.protocol.picklable
.. autofunction:: bionic.protocol.type

