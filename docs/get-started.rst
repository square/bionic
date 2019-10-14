===========
Get Started
===========

Installation
------------

Bionic can be installed using ``pip``:

.. code-block:: bash

    pip install bionic[standard]

The ``bionic[standard]`` package includes the core framework as well as the
most commonly-used dependencies.  There are several other subpackages offering
different dependencies, documented :ref:`below<extra-packages>`.

You will probably also want to install `Graphviz <https://www.graphviz.org/>`_,
which Bionic uses to generate visualizations of its workflow graph.
Unfortunately Graphviz is not written in Python and can't be installed by
``pip``.  On Mac OS X, you can use `Homebrew <https://brew.sh/>`_ to install
it:

.. code-block:: bash

    brew install graphviz

If you want your data to be automatically `cached to Google Cloud Storage`_,
you'll also need to have the `Google Cloud SDK`_ installed, have access to a
GCS bucket, and install the ``bionic[gcp]`` subpackage.

.. _cached to Google Cloud Storage: concepts.rst#caching-in-google-cloud-storage
.. _Google Cloud SDK : https://cloud.google.com/sdk/

Finally, installing `LibYAML <https://github.com/yaml/libyaml>`_ will improve
performance for some workloads.  LibYAML is also available via Homebrew:

.. code-block:: bash

    brew install libyaml

Bionic supports Python 3.6 and above.

.. _extra-packages:

Extra Packages
..............

The default ``bionic`` PyPI package installs only the minimal dependencies for
building and running flows.  However, many other dependency configurations are
available.  Most users will want the ``bionic[standard]`` package, which
supports common integrations like `Matplotlib <https://matplotlib.org/>`_,
as well as `graph visualization`_.

.. _graph visualization: concepts.rst#visualizing-flows

The full set of subpackages is as follows:

========== ================================== =================================
Subpackage  Installation Command              Enables
========== ================================== =================================
dev        ``pip install bionic[dev]``        every feature; testing; building
                                              documentation
---------- ---------------------------------- ---------------------------------
dask       ``pip install bionic[dask]``       the ``@dask`` decorator
---------- ---------------------------------- ---------------------------------
dill       ``pip install bionic[dill]``       the ``@dillable`` decorator
---------- ---------------------------------- ---------------------------------
examples   ``pip install bionic[examples]``   the tutorial example code
---------- ---------------------------------- ---------------------------------
full       ``pip install bionic[full]``       every non-development feature
---------- ---------------------------------- ---------------------------------
gcp        ``pip install bionic[gcp]``        caching to GCS
---------- ---------------------------------- ---------------------------------
image      ``pip install bionic[image]``      automatic de/serialization of
                                              ``PIL.Image`` objects
---------- ---------------------------------- ---------------------------------
matplotlib ``pip install bionic[matplotlib]`` the ``@pyplot`` decorator
---------- ---------------------------------- ---------------------------------
standard   ``pip install bionic[standard]``   graph visualization; ``Image``
                                              handling; ``@pyplot``
---------- ---------------------------------- ---------------------------------
viz        ``pip install bionic[viz]``        graph visualization
========== ================================== =================================

Tutorials
---------

These two worked examples illustrate the basic mechanics of Bionic.

.. toctree::
    :maxdepth: 1

    tutorials/hello_world.ipynb
    tutorials/ml_workflow.ipynb
