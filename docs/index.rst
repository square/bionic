======
Bionic
======

.. note::
    Bionic is in alpha and evolving rapidly.  We recommend it for research
    projects where the dataset fits in memory.  We do not recommend it for
    pipelines running in production.

**Release:** v\ |version| ---
**Quick Links:** `Source <https://github.com/square/bionic>`_ | `Issues <https://github.com/square/bionic/issues>`_ | `Installation <get-started.html#Installation>`_ | `Example <tutorials/ml_workflow.html>`_

Bionic is a framework for analyzing and modeling data in Python.  It's designed
to help you **iterate faster on your research**, and help your colleagues
**reuse your code more easily**.

You define the *entities* you care about -- dataframes, parameters, models,
plots -- using individual Python functions.  Then Bionic assembles your
definitions into a *flow*: a custom Python object that can efficiently compute
any of your entities, and can be modified on the fly to test out new
variations.

This approach has several benefits:

* Bionic automatically glues your functions into a coherent program, so your
  **code stays modular** but behaves like an **integrated end-to-end tool**.
* You can compute any entity with one function call, so it's **easy to iterate
  and debug**.
* Everything you compute is automatically cached, so you spend **less time
  waiting** and **zero time managing data files**.
* Flows are easy to use from a notebook, so you can **work interactively** but
  keep your code in a **version-controlled** Python file.
* Any part of a flow can be modified dynamically, so you can **quickly try
  experiments**, and your colleagues can **reuse your code** without rewriting
  it.

..
    This is super annoying, but it's the only way I've found to make a bold
    internal link in RST.  (I really want the link to be bold so you can see
    the example link easily when you're scanning.)

Check out an |bold link|!

.. |bold link| raw:: html

   <a class="reference internal" href="tutorials/ml_workflow.html">
   <strong>example here</strong></a>

Documentation Contents
----------------------

.. toctree::
    :maxdepth: 2

    what
    get-started
    concepts
    warnings
    api/index.rst
    get-help
    contributing
    future
    release-notes
