======================
Contributing to Bionic
======================

Bionic's source is maintained on `GitHub <https://github.com/square/bionic>`_.
You can clone it with:

.. code-block:: bash

    git clone git@github.com:square/bionic.git

Pull requests are welcome!  (However, for large changes, we recommend
discussing the proposed change on our `Issues page
<https://github.com/square/bionic/issues>`_ first.)  Because Bionic is
supported by Square, all new contributors will be asked to sign `Square's
Contributor License Agreement
<https://gist.github.com/square-cla/0dac5a22575ecf5e4f40825e7de51d5d>`_ as part
of the pull request process.

For Bionic core developers, our internal processes are documented :doc:`here
<maintaining>`.

Submitting a Pull Request
-------------------------

To maintain a baseline level of correctness, readability, and design
coherence, every pull request to Bionic is reviewed by a maintainer.
Maintainers typically check at least the following:

1. If you're making changes to Bionic's behavior, include tests if possible,
   and add an entry to the `Release Notes <release-notes.html>`_.
2. If you're updating Bionic's user-facing API, :ref:`update the
   documentation <docs>`.
3. Make sure all existing :ref:`tests <tests>` and :ref:`style checks
   <style>` pass. (This will be automatically checked by our :ref:`continuous
   integration <ci>`.)
4. Try to conform to the style of the surrounding code.

To make your review go as smoothly as possible, we also suggest writing clean
and helpful commit messages.

Setting Up Your Development Environment
---------------------------------------

Bionic has some additional dependencies required for running tests and building
documentation.  Most of these can be installed by running this from the root
of the repo:

.. code-block:: bash

    pip install -e '.[dev]'

If you want to build the documentation, you also need to install `pandoc
<https://pandoc.org/>`_, which is used to convert notebook files into Sphinx
documents.  On OS X you can do this with Homebrew:

.. code-block:: bash

    brew install pandoc

.. _tests :

Tests
-----

Bionic has a suite of automated tests using the
`pytest <https://docs.pytest.org/en/latest/>`_ framework. You can run most of
them like this:

.. code-block:: bash

    pytest

Our :ref:`Continuous Integration <ci>` system will also run them
automatically when you submit a pull request.

All functional changes to Bionic should be accompanied by new or updated tests.

Extra Tests
...........

Pytest doesn't run all of our tests run by default. Some are slow, and only
run if you specifically request them:

.. code-block:: bash

    pytest --slow

Pytest will also skip the Google Cloud Storage tests unless you pass a
command line option telling it which bucket to use:

.. code-block:: bash

    pytest --bucket=gs://MYBUCKET

.. _style :

Code Style
----------

Bionic follows the `PEP 8 <https://www.python.org/dev/peps/pep-0008/>`_
standard. We use `Black <https://black.readthedocs.io/en/stable/>`_ to
automatically format our code and `Flake8
<https://flake8.pycqa.org/en/latest/>`_ to identify additional style errors.
Our :ref:`Continuous Integration <ci>` system runs these tools on all pull
requests, but your life will be easier if you run them yourself before
submitting a pull request:

.. code-block:: bash

    black .
    flake8

.. _ci :

Continuous Integration
----------------------

We use `GitHub Actions <https://github.com/features/actions>`_ to run our
tests and style checks on every branch pushed to GitHub. If you submit a pull
request, you should see the results show up automatically in the "checks"
section.

.. _docs :

Updating the Documentation
--------------------------

Bionic's documentation is built with `Sphinx
<http://www.sphinx-doc.org/en/master/>`_.  You can build it from the ``docs``
directory:

.. code-block:: bash

    make html

Alternatively, you can use `sphinx-autobuild
<https://pypi.org/project/sphinx-autobuild/>`_, which watches your document
source files, automatically rebuilds them when they change, and runs a web
server with the latest version:

.. code-block:: bash

    make livehtml
    # Leave this running and open localhost:8000 in your browser to see the docs.

Some of the documentation pages are built from Jupyter notebooks.  When editing
these, you need to remember two things:

1. Don't run any of the cells yourself; let Sphinx do that at build time.  If
   you do run a cell, you can clear it with ``Edit > Clear All Outputs``.  If
   you leave any cell output in the notebook, Sphinx won't try to run any of
   the cells itself.
2. The "raw" text cells have special metadata that tells Sphinx that their
   contents are in the ReStructured Text format.  As far as I know, this
   metadata can't be changed by current versions of Jupyter Notebook or Jupyter
   Lab; you have to manually edit the ``.ipynb`` file.  If you add any new text
   cells to a notebook, you'll probably want to add this metadata as well.
