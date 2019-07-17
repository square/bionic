===========
Get Started
===========

Installation
------------

If you're using `Square's internal PyPI server
<REDACTED-URL>`_, you
can install Bionic using ``pip``.

.. code-block:: bash

    pip install bionic

You will probably also want to install Graphviz, which Bionic uses to generate
visualizations of its workflow graph.  Unfortunately Graphviz is not written
in Python and can't be installed by ``pip``.  On Mac OS X, you can use Homebrew
to install it:

.. code-block:: bash

    brew install graphviz

If you want your data to be automatically `cached to Google Cloud Storage`_,
you'll also need to have the `Google Cloud SDK`_ installed and have access to a
GCS bucket.

.. _cached to Google Cloud Storage: concepts.rst#caching-in-google-cloud-storage
.. _Google Cloud SDK : https://cloud.google.com/sdk/

Bionic should work on both Python 2.7 and Python 3.x.

Tutorials
---------

These two worked examples illustrate the basic mechanics of Bionic.

.. toctree::
    :maxdepth: 1

    tutorials/hello_world.ipynb
    tutorials/ml_workflow.ipynb
