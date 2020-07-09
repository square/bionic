==================
Maintaining Bionic
==================

This page documents project maintenance processes followed by Bionic’s core developers.
If you’re not a core developer but need a new release for some reason, please contact
one of the developers `listed here
<https://github.com/square/bionic/blob/master/.github/CODEOWNERS>`_.

Release Process
---------------

We use `bumpversion <https://pypi.org/project/bumpversion/>`_ to manage our version
strings and `GitHub Releases <https://github.com/square/bionic/releases>`_ to publish
releases to PyPI. Follow these steps to release a new version of Bionic:

1. Merge a PR with updates to our version strings and release notes.

   a. Check out the current master branch.
   b. Create a new branch.
   c. Run ``bumpversion minor`` or ``bumpversion patch`` to bump the version. Running
      this command will create a new commit with all the version strings updated to the
      new version.
   d. Follow the commented instructions near ``Upcoming Version`` in the
      `release-notes.rst
      <https://github.com/square/bionic/blob/master/docs/release-notes.rst>`_ file and
      update the upcoming version section.
   e. Amend the commit to add the release notes changes.
   f. Open a PR for your branch and merge it after approval from another core
      developer.

2. Once your PR is merged, create a release from the GitHub Releases page.

   a. On the `GitHub Releases <https://github.com/square/bionic/releases>`_ page, click
      ``Draft a new release``.
   b. Specify the bumped version as the ``Tag version`` and ``Release title``. Don't
      forget to prefix the version with ``v``. E.g., if the new version is ``0.8.0``,
      your tag and title should both be ``v0.8.0``.
   c. Click ``Publish Release``.
   d. Verify that the `Upload Python Package Action
      <https://github.com/square/bionic/actions?query=workflow%3A%22Upload+Python+Package%22>`_
      workflow was completed successfully and the new release is visible on `PyPI
      <https://pypi.org/project/bionic/#history>`_.
