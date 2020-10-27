"""
Utilities for working with Google Cloud Storage.
"""

import logging
import warnings

from .deps.optdep import import_optional_dependency

logger = logging.getLogger(__name__)


_cached_gcs_fs = None


def get_gcs_fs_without_warnings(cache_value=True):
    # TODO It's not expensive to create the gcs filesystem, but caching this enables
    # us to mock the cached gcs_fs with a mock implementation in tests. We should
    # change the tests to inject the filesystem in a different way and get rid of
    # this caching.
    if cache_value:
        global _cached_gcs_fs
        if _cached_gcs_fs is None:
            _cached_gcs_fs = get_gcs_fs_without_warnings(cache_value=False)
        return _cached_gcs_fs

    fsspec = import_optional_dependency("fsspec", purpose="caching to GCS")

    with warnings.catch_warnings():
        # Google's SDK warns if you use end user credentials instead of a
        # service account.  I think this warning is intended for production
        # server code, where you don't want GCP access to be tied to a
        # particular user.  However, this code is intended to be run by
        # individuals, so using end user credentials seems appropriate.
        # Hence, we'll suppress this warning.
        warnings.filterwarnings(
            "ignore", "Your application has authenticated using end user credentials"
        )
        logger.info("Initializing GCS filesystem ...")
        return fsspec.filesystem("gcs")


# TODO: Consider using persistence.GcsFilesystem instead of exposing this function.
def upload_to_gcs(path, url):
    """
    Copy a local path to GCS URL.
    """
    gcs_fs = get_gcs_fs_without_warnings()
    if path.is_dir():
        gcs_fs.put(str(path), url, recursive=True)
    else:
        # If the GCS URL is a folder, we want to write the file in the folder.
        # There seems to be a bug in fsspec due to which, the file is uploaded
        # as the url, instead of inside the folder. What this means is, writing
        # a file c.json to gs://a/b/ would result in file gs://a/b instead of
        # gs://a/b/c.json.
        #
        # The `put` API is supposed to write the file inside the folder but it
        # strips the ending "/" at the end in fsspec's `_strip_protocol` method.
        # See https://github.com/intake/filesystem_spec/issues/448 for more
        # details and tracking this issue.
        if url.endswith("/"):
            url = url + path.name
        gcs_fs.put_file(str(path), url)
