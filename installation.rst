============
Installation
============

If you're using `Square's internal PyPI server
<https://wiki.sqcorp.co/display/IS/Data+Science%3A+Internal+Pip+Server>`_, you
can install Bionic using ``pip``.

.. code-block:: bash

    pip install bionic

You will probably also want to install Graphviz, which Bionic uses to generate
visualizations of its workflow graph.  Unfortunately Graphviz is not written
in Python and can't be installed by ``pip``.  On Mac OS X, you can use Homebrew
to install it:

.. code-block:: bash

    brew install graphviz

Bionic supports Python 3.6 and above.
