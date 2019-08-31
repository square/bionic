========================
Why Should I Use Bionic?
========================

Bionic lets you automate your data analysis by combining the *entities* you
care about into a *workflow*.  This has the following benefits:

* **File-Object Mapping**: Bionic knows how to convert each entity between
  a Python object and a file.  You never have to read or write intermediate files
  or remember what your file naming scheme is; you just ask for an entity by
  name and Bionic gets it for you.
* **Automatic Caching**: Every entity is automatically cached on disk
  or in the cloud, so you only have to compute it once.  If you
  redefine an entity, Bionic figures out which entities need to be recomputed.
* **Automatic Plumbing**: Each entity is specified independently and only knows
  about its immediate dependencies.  You don't need to manually assemble the
  dependency graph: Bionic does it automatically, making refactoring easy.
* **Variation Tracking**: Bionic allows multiple instances of any entity, so
  so you can easily compare multiple variations within a single workflow.
* **Reuseable Packaging**: Your workflow is packaged into a single Python
  object that provides a nice API, making it easy for your colleagues to reuse,
  reconfigure, and recombine your work.
* **Notebook-Friendly API**: Bionic is designed for use in both Python module
  files and notebooks -- you can build your flow in a Python file and easily
  use it from a notebook.  This lets you combine the version control and
  reproducibility of files with the rapid iteration environment of notebooks.
* **Scalable Execution (Work In Progress)**: In future versions of Bionic, entities
  will be computed in parallel, either on your local machine or on a cluster.

When Should I Use It?
---------------------

Bionic can help you if you have any of these needs:

* You have expensive computation steps you'd like to cache.

* Your workflow is getting too complex to fit in one notebook.

* You want to replicate part of your workflow multiple times.

* You're creating a lot of data files and are having trouble keeping track
  of which file goes where.

* You want a reproducible, one-click way to generate your entire set of plots,
  reports, models, etc.

* You want your analysis code to be easy to refactor, reuse, and extend.

But you probably need to meet *all* of these requirements:

* You're using Python.

* You're willing to organize your code around Bionic.

* You're comfortable using alpha software that might change in the future
  [#f_alpha]_.

* You don't need more than one machine's worth of memory or CPU [#f_single]_.

These things don't matter:

* Whether you're doing "data science", "data analysis", "machine learning", or
  "statistics".

* Whether you're using a laptop, a desktop, or a cloud host.

* Whether you're working on a small one-off analysis or a large research
  project.

.. [#f_alpha] This won't always be the case, of course.

.. [#f_single] This will also be fixed in the `future
  <future.rst#distributed-computation>`__.

Why Not Just Use...?
--------------------

* `Make <https://www.gnu.org/software/make/>`_ (or another build system): Make
  is ubiquitous on UNIX systems and supports dependency management and
  invalidation.  However, Make forces you to work in terms of files rather than
  objects, which means (a) you have to maintain the translation between files
  and objects yourself, and (b) you'll want to represent each transformation
  function as a separate file, which imposes a lot of constraints on your
  program structure.  Make is also missing many of the features Bionic provides
  to help with data science concerns and to manage workflow complexity.

* `Joblib <https://joblib.readthedocs.io/en/latest/>`_: Joblib is a library
  that provides powerful drop-in APIs for caching and parallelism.  Joblib is
  designed to be integrated into code with minimal changes, whereas Bionic is a
  framework that you need to organize your code around.  In exchange, Bionic
  helps you manage the complexity of your workflow and share it with others.

* `Dask <https://dask.org/>`_: Dask has two parts: generic infrastructure for
  executing task graphs on multiple compute hosts, and a collection of data
  structures that mimic existing APIs like Pandas and Scikit-Learn but
  transparently use Dask's infrastructure.  Dask doesn't attempt to manage or
  organize workflows like Bionic does.  However, Dask's infrastructure could be
  used as a task execution backend for Bionic, and its data structures can be
  used as Bionic entities (like any other Python object).

* `Airflow <https://airflow.apache.org/>`_ and `Luigi <https://github.com/spotify/luigi>`_:
  These are both platforms for building and running workflows.  However, they
  are targeted more at production ETL workflows rather than experimental
  research workflows.  They also have a more manual and low-level API for
  constructing workflows: users have to construct the dependency graph and
  manage file-object mapping themselves.  Either of them might make sense as
  part of a task execution backend for Bionic.

* `Databolt Flow <https://github.com/d6t/d6tflow/blob/master/README.md>`_:
  "``d6tflow``", part of the Databolt family of data science tools, is probably
  the open-source tool closest to Bionic.  It builds on top of Luigi but also
  manages file-object mapping and provides some automatic plumbing via
  parameter inheritance.  Unlike Bionic, it also supports dynamic specification
  of dependencies [#f_dependency]_.  However, it has a more rigid concept of
  parameters and data, whereas Bionic handles parameters, data, visualizations,
  and infrastructure components with a unified dependency injection model.  In
  Bionic, any of these can be built into a flow and then swapped out later.
  In particular, since Databolt Flow is built on top of Luigi, it is unlikely
  to support any other execution platforms.

* Your Own Custom Code: You can certainly just write all of these components
  yourself as they become necessary.  However, it will take time, and you'll
  have a higher risk of bugs.  Your implementation will probably also bake in
  many assumptions about your problem domain, which will make your code harder
  to refactor when those assumptions change.  Finally, your code will be hard
  to reuse unless you do the additional work of packaging it up nicely -- which
  will make it even harder to refactor later.

.. [#f_dependency] Dynamic dependency specification will probably be added to
  Bionic in some form, at some point.

