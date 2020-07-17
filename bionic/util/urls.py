"""
Utilities for working with URLs.
"""

import os
from pathlib import Path
from urllib.parse import urlparse

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
    return Path(result.path)


def url_from_path(path):
    return Path(path).as_uri()


def bucket_and_object_names_from_gs_url(url):
    if not is_gcs_url(url):
        raise ValueError(f'url must have schema "{GCS_SCHEME}": got {url}')
    result = urlparse(url)
    result_path = result.path
    return result.netloc, result_path[1:]


# TODO Make these arg names and docstrings more general.
def relativize_url(artifact_url, metadata_url):
    """Returns the relative artifact url wrt to metadata url
    when both urls are file urls. Otherwise, returns the original url."""
    if not is_file_url(artifact_url) or not is_file_url(metadata_url):
        return artifact_url
    artifact_path = path_from_url(artifact_url)
    metadata_path = path_from_url(metadata_url)
    return os.path.relpath(artifact_path, metadata_path.parent)


def derelativize_url(artifact_url, metadata_url):
    """Returns the absolute artifact url when it is relative wrt metadata url.
    Otherwise returns the original url."""
    if is_absolute_url(artifact_url):
        return artifact_url
    metadata_path = path_from_url(metadata_url)
    artifact_path = path_from_url(artifact_url)
    abspath = os.path.normpath(metadata_path.parent.joinpath(artifact_path))
    return url_from_path(abspath)
