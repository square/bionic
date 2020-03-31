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

Running the Tests
-----------------

Bionic uses `pytest <https://docs.pytest.org/en/latest/>`_ for tests and
`flake8 <http://flake8.pycqa.org/en/latest/>`_ for linting.  You can run them
like this:

.. code-block:: bash

    pytest
    flake8

Our continuous integration system, `Travis <https://travis-ci.com/>`_, should
automatically run these for you when you submit a PR.

Pytest will skip the Google Cloud Storage tests unless you pass a command line
option telling it which bucket to use:

.. code-block:: bash

    pytest --bucket=gs://MYBUCKET

It will also skip some other slow tests unless you specifically include them:

.. code-block:: bash

    pytest --slow

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
