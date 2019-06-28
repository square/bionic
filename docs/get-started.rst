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

Bionic should work on both Python 2.7 and Python 3.x.

Tutorials
---------

These two worked examples illustrate the basic mechanics of Bionic.

.. toctree::
    :maxdepth: 1

    tutorials/hello_world.ipynb
    tutorials/ml_workflow.ipynb
