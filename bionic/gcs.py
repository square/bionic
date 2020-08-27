"""
Utilities for working with Google Cloud Storage.
"""

import subprocess
import warnings

from .deps.optdep import import_optional_dependency
from .utils.urls import bucket_and_object_names_from_gs_url

import logging

logger = logging.getLogger(__name__)

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


class GcsTool:
    """
    A helper object providing utility methods for accessing GCS.  Maintains
    a GCS client, and a prefix defining a default namespace to read/write on.
    """

    _GS_URL_PREFIX = "gs://"

    def __init__(self, url):
        if url.endswith("/"):
            url = url[:-1]
        self.url = url
        bucket_name, object_prefix = bucket_and_object_names_from_gs_url(url)

        self._bucket_name = bucket_name
        self._object_prefix = object_prefix
        self._init_client()

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        state = self.__dict__.copy()
        # Remove the unpicklable entries.
        del state["_client"]
        del state["_bucket"]
        return state

    def __setstate__(self, state):
        # Restore instance attributes.
        self.__dict__.update(state)
        # Restore the client and bucket.
        self._init_client()

    def _init_client(self):
        self._client = get_gcs_client_without_warnings()
        self._bucket = self._client.get_bucket(self._bucket_name)

    def blob_from_url(self, url):
        object_name = self._validated_object_name_from_url(url)
        return self._bucket.blob(object_name)

    def url_from_object_name(self, object_name):
        return self._GS_URL_PREFIX + self._bucket.name + "/" + object_name

    def blobs_matching_url_prefix(self, url_prefix):
        obj_prefix = self._validated_object_name_from_url(url_prefix)
        return self._bucket.list_blobs(prefix=obj_prefix)

    def gsutil_cp(self, src_url, dst_url):
        args = [
            "gsutil",
            "-q",  # Don't log anything but errors.
            "-m",  # Transfer files in paralle.
            "cp",
            "-r",  # Recursively sync sub-directories.
            src_url,
            dst_url,
        ]
        logger.debug("Running command: %s" % " ".join(args))
        subprocess.check_call(args)
        logger.debug("Finished running gsutil")

    def _validated_object_name_from_url(self, url):
        bucket_name, object_name = bucket_and_object_names_from_gs_url(url)
        assert bucket_name == self._bucket.name
        assert object_name.startswith(self._object_prefix)
        return object_name
