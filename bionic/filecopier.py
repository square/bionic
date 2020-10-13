"""
Contains the ``FileCopier`` class, which is essentially a file path with a useful
``copy`` method attached to it.
"""

import subprocess

from bionic.gcs import upload_to_gcs


class FileCopier:
    """
    A wrapper for a Path object, exposing a ``copy`` method that will copy
    the underlying file to a local or cloud destination.

    Parameters
    ----------
    src_file_path: Path
        A path to a file.
    """

    def __init__(self, src_file_path):
        self.src_file_path = src_file_path

    def copy(self, destination):
        """
        Copies file that FileCopier represents to `destination`

        This supports both local and GCS destinations. For the former, we follow cp's
        conventions and for the latter we follow fsspec's put / put_file APIs which
        can be found at
        https://filesystem-spec.readthedocs.io/en/latest/api.html#fsspec.spec.AbstractFileSystem.

        Parameters
        ----------

        destination: Path or str
            Where to copy the underlying file
        """

        #  handle gcs
        if str(destination).startswith("gs://"):
            upload_to_gcs(self.src_file_path, str(destination))
        else:
            subprocess.check_call(
                ["cp", "-R", str(self.src_file_path), str(destination)]
            )
