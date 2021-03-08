"""
Utilities for working with AWS S3.
"""

import logging
import warnings

from .deps.optdep import import_optional_dependency

logger = logging.getLogger(__name__)

_cached_s3_fs = None

def get_s3_fs(cache_value=True):
    if cache_value:
        global _cached_s3_fs
        if _cached_s3_fs is None:
            _cached_s3_fs = get_s3_fs(cache_value=False)
        return _cached_s3_fs

    fsspec = import_optional_dependency("fsspec", purpose="caching to S3")

    with warnings.catch_warnings():
        # warnings.filterwarnings(
        #     "ignore", "Your application has authenticated using end user credentials"
        # )
        logger.info("Initializing S3 filesystem ...")
        return fsspec.filesystem("s3")

def upload_to_s3(path, url):
    """
    Copy a local path to S3 URL.
    """
    s3_fs = get_s3_fs()
    if path.is_dir():
        s3_fs.put(str(path), url, recursive=True)
    else:
        if url.endswith("/"):
            url = url + path.name
        s3_fs.put_file(str(path), url)
