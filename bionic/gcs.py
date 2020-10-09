"""
Utilities for working with Google Cloud Storage.
"""

import subprocess
import warnings

from .deps.optdep import import_optional_dependency

import logging

logger = logging.getLogger(__name__)


_cached_gcs_fs = None


def get_gcs_fs_without_warnings(cache_value=True):
    # TODO It's not expensive to create gcs filesystem, but caching this enables
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


_cached_gcs_client = None


def get_gcs_client_without_warnings(cache_value=True):
    # TODO This caching saves a lot of time, especially in tests.  But it would
    # be better if Bionic were able to re-use its in-memory cache when creating
    # new flows, instead of resetting the cache each time.
    if cache_value:
        global _cached_gcs_client
        if _cached_gcs_client is None:
            _cached_gcs_client = get_gcs_client_without_warnings(cache_value=False)
        return _cached_gcs_client

    gcs = import_optional_dependency("google.cloud.storage", purpose="caching to GCS")

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
        logger.info("Initializing GCS client ...")
        return gcs.Client()


def copy_to_gcs(src, dst):
    """Copy a local file at src to GCS at dst"""
    bucket = dst.replace("gs://", "").split("/")[0]
    prefix = f"gs://{bucket}"
    path = dst[len(prefix) + 1 :]

    client = get_gcs_client_without_warnings()
    blob = client.get_bucket(bucket).blob(path)
    blob.upload_from_filename(src)


def gsutil_cp(src_url, dst_url):
    """
    This method is a proxy for _gsutil_cp which does the actual work. The proxy
    exists so that _gsutil_cp can be replaced for testing.
    """
    _gsutil_cp(str(src_url), str(dst_url))


def _gsutil_cp(src_url, dst_url):
    args = [
        "gsutil",
        "-q",  # Don't log anything but errors.
        "-m",  # Transfer files in parallel.
        "cp",
        "-r",  # Recursively sync sub-directories.
        src_url,
        dst_url,
    ]
    logger.debug("Running command: %s" % " ".join(args))
    subprocess.check_call(args)
    logger.debug("Finished running gsutil")
