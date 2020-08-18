"""
Utilities for working with URLs.
"""

import os
from pathlib import Path
from urllib.parse import unquote, urlparse

FILE_SCHEME = "file"
GCS_SCHEME = "gs"
SUPPORTED_SCHEMES = [FILE_SCHEME, GCS_SCHEME]


def is_file_url(url):
    result = urlparse(url)
    return result.scheme == FILE_SCHEME


def is_gcs_url(url):
    result = urlparse(url)
    return result.scheme == GCS_SCHEME


def is_absolute_url(url):
    result = urlparse(url)
    if not result.scheme:
        return False
    if result.scheme not in SUPPORTED_SCHEMES:
        raise ValueError(f"Found a URL with unsupported scheme {result.scheme!r}.")
    return True


def path_from_url(url):
    result = urlparse(url)
    return Path(unquote(result.path))


def url_from_path(path):
    return Path(path).as_uri()


def bucket_and_object_names_from_gs_url(url):
    if not is_gcs_url(url):
        raise ValueError(f'url must have schema "{GCS_SCHEME}": got {url}')
    result = urlparse(url)
    result_path = result.path
    return result.netloc, result_path[1:]


def relativize_url(absolute_url, base_url):
    """
    Converts an absolute file URL to one relative to a base file URL.

    If either URL is not a file URL, this returns the original absolute URL.
    """

    if not is_file_url(absolute_url) or not is_file_url(base_url):
        return absolute_url
    absolute_path = path_from_url(absolute_url)
    base_path = path_from_url(base_url)
    # Using str(absolute_path.relative_to(base_path.parent)) doesn't work as well here,
    # because it throws an exception if base_path is not a parent of absolute_path.
    return os.path.relpath(absolute_path, base_path.parent)


def derelativize_url(relative_url, base_url):
    """
    Given a URL relative to another base URL, returns an absolute URL.

    If the first URL is not relative, it is returned unchanged.
    """

    if is_absolute_url(relative_url):
        return relative_url
    base_path = path_from_url(base_url)
    relative_path = path_from_url(relative_url)
    absolute_path = os.path.normpath(base_path.parent.joinpath(relative_path))
    return url_from_path(absolute_path)
