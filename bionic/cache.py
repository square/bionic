"""
Contains the PersistentCache class, which handles persistent local caching.
This including artifact naming, invalidation, and saving/loading.

The PersistentCache is backed by two "file caches", which can be used to
replicate files to a persistent store.  Both classes implement the same
interface, which is based on copying temporary directories in and out of a
managed cache.  Directories within the cache are referred to by "virtual
paths", which the cache translates into either filesystem paths or GCS object
prefixes.
"""

from __future__ import absolute_import
from __future__ import division

from builtins import object
import shutil
import subprocess
import tempfile
import textwrap
import yaml

import six
from pathlib2 import Path, PurePosixPath

from .datatypes import Result
from .util import (
    check_exactly_one_present, hash_to_hex, get_gcs_client_without_warnings)

import logging
logger = logging.getLogger(__name__)

CACHE_SOURCE_NAME_LOCAL = 'local'
CACHE_SOURCE_NAME_CLOUD = 'cloud'

VALUE_FILENAME_STEM = 'value.'
DESCRIPTOR_FILENAME = 'descriptor.yaml'


class PersistentCache(object):
    """
    Provides a persistent mapping between Queries (things we could compute) and
    Results (computed Queries).
    """

    def __init__(self, tmp_working_dir_str, local_cache, cloud_cache):
        self._tmp_working_dir_str = tmp_working_dir_str
        self._local_cache = local_cache
        self._cloud_cache = cloud_cache

    def save(self, result):
        working_dir_path = self._create_tmp_dir_path()
        try:
            query = result.query
            virtual_path = self._virtual_path(query)

            value = result.value
            extension = query.protocol.file_extension_for_value(value)
            value_filename = VALUE_FILENAME_STEM + extension
            value_path = working_dir_path / value_filename

            with value_path.open('wb') as f:
                query.protocol.write(value, f)

            descriptor = ArtifactDescriptor.from_content(
                entity_name=query.entity_name,
                value_filename=value_filename,
                provenance=query.provenance,
            )
            descriptor_path = working_dir_path / DESCRIPTOR_FILENAME
            descriptor_path.write_text(descriptor.to_yaml())

            if not self._local_cache.has_dir(virtual_path):
                active_cache = self._local_cache
                active_cache.copy_in(virtual_path, working_dir_path)
            if (
                    self._cloud_cache is not None and
                    not self._cloud_cache.has_dir(virtual_path)):
                logger.info(
                    'Uploading   %s to cloud file cache ...',
                    query.task_key)
                self._cloud_cache.copy_in(virtual_path, working_dir_path)

        finally:
            self._remove_tmp_dir_path(working_dir_path)

    def load(self, query):
        working_dir_path = self._create_tmp_dir_path()
        try:
            virtual_path = self._virtual_path(query)

            active_cache = None
            if self._local_cache.has_dir(virtual_path):
                active_cache = self._local_cache
                cache_source_name = CACHE_SOURCE_NAME_LOCAL
            elif (
                    self._cloud_cache is not None and
                    self._cloud_cache.has_dir(virtual_path)):
                active_cache = self._cloud_cache
                cache_source_name = CACHE_SOURCE_NAME_CLOUD
                logger.info(
                    'Downloading %s from cloud file cache ...',
                    query.task_key)
            else:
                return None

            try:
                active_cache.copy_out(virtual_path, working_dir_path)

                descriptor_path = working_dir_path / DESCRIPTOR_FILENAME
                if not descriptor_path.is_file():
                    raise InvalidCacheStateError(
                        "Couldn't find descriptor file: %s" %
                        DESCRIPTOR_FILENAME)

                descriptor_yaml = descriptor_path.read_text()
                try:
                    descriptor = ArtifactDescriptor.from_yaml(descriptor_yaml)
                except YamlRecordParsingError as e:
                    raise InvalidCacheStateError(
                        "Couldn't parse descriptor file: %s" % e)

                value_filename = descriptor.value_filename
                value_path = working_dir_path / value_filename
                extension = value_path.name[len(VALUE_FILENAME_STEM):]

                try:
                    with value_path.open('rb') as f:
                        value = query.protocol.read(f, extension)
                except Exception as e:
                    raise InvalidCacheStateError(
                        "Unable to load value %s due to %s: %s" % (
                            query.task_key, e.__class__.__name__, e))

            except InvalidCacheStateError as e:
                raise InvalidCacheStateError(
                    "Cached data was in an invalid state; "
                    "this should be impossible but could have resulted from "
                    "either a bug or a change to the cached files.  You "
                    "should be able to repair the problem by removing '%s'."
                    "\nDetails: %s" % (
                        active_cache.export_path_str(virtual_path), e))

            cache_path_str = active_cache.export_path_str(
                virtual_path / value_filename)

            result = Result(
                query=query,
                value=value,
                cache_source_name=cache_source_name,
                cache_path_str=cache_path_str,
            )

            self.cache_source_name = cache_source_name

        finally:
            self._remove_tmp_dir_path(working_dir_path)

        return result

    def _create_tmp_dir_path(self):
        Path(self._tmp_working_dir_str).mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(dir=self._tmp_working_dir_str))

    def _remove_tmp_dir_path(self, path):
        if (path.is_file() or path.is_symlink()):
            path.unlink()
        else:
            shutil.rmtree(str(path))

    def _virtual_path(self, query):
        artifact_dir_str = query.provenance.hashed_value
        return Path(query.entity_name) / artifact_dir_str


class LocalFileCache(object):
    """
    Replicates files to another location on the local filesystem.
    """

    def __init__(self, root_path):
        self.root_path = root_path

    def has_dir(self, virtual_path):
        """Checks whether a directory is present in the cache."""
        dir_path = self.root_path / virtual_path
        return dir_path.is_dir()

    def copy_in(self, virtual_path, src_path):
        """
        Copies a (temporary) directory into the cache filesystem.

        If the directory is not already a symlink, it will simply get moved
        into the managed filesystem and replaced with a symlink to the moved
        directory.  Since we're assuming the original directory is temporary,
        this is safe and cheaper than copying the files.
        """

        dst_path = self.root_path / virtual_path
        if dst_path.is_file():
            dst_path.unlink()
        dst_path.mkdir(parents=True, exist_ok=True)

        if src_path.is_symlink():
            shutil.copytree(src_path, dst_path)
        else:
            src_path.rename(dst_path)
            src_path.symlink_to(dst_path.absolute(), target_is_directory=True)

    def copy_out(self, virtual_path, dst_path):
        """
        Creates new directory which is a symlink to a directory in the cache
        filesystem.
        """
        src_path = self.root_path / virtual_path
        if src_path.is_file():
            raise InvalidCacheStateError(
                "Expected %s to be a directory, but it was a file" % dst_path)

        if dst_path.is_dir():
            dst_path.rmdir()
        dst_path.symlink_to(src_path.absolute(), target_is_directory=True)

    def export_path_str(self, virtual_path):
        """Returns a real path corresponding to the provided virtual path."""
        return str(self.root_path / virtual_path)


def normalize_path(path):
    """
    Convert a Path to a slash-separated string, suitable for use in cloud
    object names.  (We don't just use ``str(path)`` because our paths might
    look different if we ever run on Windows.)
    """
    return str(PurePosixPath(path))


class GcsFileCache(object):
    """
    Replicates files to and from Google Cloud Storage.
    """

    DIR_MARKER_NAME = '__dir_marker__'
    DIR_MARKER_CONTENTS = textwrap.dedent('''
    The presence of this file indicates that the containing directory
    has been completely and successfully written.
    ''')

    def __init__(self, url):
        URL_PREFIX = 'gs://'
        if not url.startswith(URL_PREFIX):
            raise ValueError('url must start with "%s"' % URL_PREFIX)
        url_parts = url[len(URL_PREFIX):].split('/', 1)
        if len(url_parts) == 1:
            bucket_name, = url_parts
            object_prefix = ''
        else:
            bucket_name, object_prefix = url_parts

        logger.info('Initializing GCS client ...')
        client = get_gcs_client_without_warnings()

        self._bucket = client.get_bucket(bucket_name)
        if not object_prefix.endswith('/'):
            object_prefix = object_prefix + '/'
        self._object_prefix = object_prefix

    def has_dir(self, virtual_path):
        """Checks whether a directory is present in the cache."""
        return self._marker_blob(virtual_path).exists()

    def copy_in(self, virtual_path, src_path):
        """
        Copies the contents of a directory to GCS.

        This uses gsutils rsync, but also adds a special marker blob to
        indicate that a sync has been successfully completed.  (Otherwise,
        if the rsync termined in the middle of an upload, there would be no
        way to tell that the uploaded objects were incomplete.)
        """

        if (src_path / self.DIR_MARKER_NAME).exists():
            raise ValueError(
                "Found a file with the special name %r; we can't cache this" %
                self.DIR_MARKER_NAME)

        self._gsutil_rsync(
            src_url=str(src_path),
            dst_url=self._gs_url_from_path(virtual_path),
        )

        self._marker_blob(virtual_path).upload_from_string(
            self.DIR_MARKER_CONTENTS)

    def copy_out(self, virtual_path, dst_path):
        """Copies a cached directory into a local file directory."""
        dst_path.mkdir(parents=True, exist_ok=True)
        self._gsutil_rsync(
            src_url=self._gs_url_from_path(virtual_path),
            dst_url=str(dst_path),
        )

    def export_path_str(self, virtual_path):
        """Returns a ``gs://` URL corresponding to a virtual path."""
        return self._gs_url_from_path(virtual_path)

    def _gs_url_from_path(self, virtual_path):
        return 'gs://%s/%s' % (
            self._bucket.name,
            normalize_path(self._object_prefix + str(virtual_path) + '/'))

    def _gsutil_rsync(self, src_url, dst_url):
        args = [
            'gsutil',
            '-q',  # Don't log anything but errors.
            '-m',  # Transfer files in paralle.
            'rsync',
            '-d',  # Delete any items in the dst dir that aren't in the src dir.
            '-R',  # Recursively sync sub-directories.
            src_url, dst_url
        ]
        logger.debug('Running command: %s' % ' '.join(args))
        subprocess.check_call(args)
        logger.debug('Finished running gsutil')

    def _marker_blob(self, virtual_path):
        return self._bucket.blob(
            self._object_prefix +
            normalize_path(virtual_path / self.DIR_MARKER_NAME)
        )


class InvalidCacheStateError(Exception):
    pass


CACHE_SCHEMA_VERSION = 2

if six.PY2:
    PYTHON_MAJOR_VERSION = 2
elif six.PY3:
    PYTHON_MAJOR_VERSION = 3
else:
    raise AssertionError("Can't figure out what Python version we're using")


class YamlDictRecord(object):
    """
    A base class for classes wrapping a simple dictionary that can easily
    be converted to/from YAML.
    """

    @classmethod
    def from_yaml(cls, yaml_str):
        return cls(yaml_str=yaml_str)

    def __init__(self, body_dict=None, yaml_str=None):
        check_exactly_one_present(body_dict=body_dict, yaml_str=yaml_str)

        if body_dict is not None:
            self._body_dict = body_dict
            self._yaml_str = yaml.dump(
                body_dict,
                default_flow_style=False,
                encoding=None,
                sort_keys=True,
            )
        else:
            try:
                self._body_dict = yaml.full_load(yaml_str)
            except yaml.error.YAMLError as e:
                raise YamlRecordParsingError(
                    "Couldn't parse %s: %s" % (
                        self.__class__.__name__, str(e)))
            self._yaml_str = yaml_str

    def to_yaml(self):
        return self._yaml_str

    def to_dict(self):
        return self._body_dict


class YamlRecordParsingError(Exception):
    pass


class ArtifactDescriptor(YamlDictRecord):
    """
    Describes a persisted artifact.  Intended to be stored as a YAML file.
    """

    @classmethod
    def from_content(cls, entity_name, value_filename, provenance):
        return cls(body_dict=dict(
            entity=entity_name,
            filename=value_filename,
            provenance=provenance.to_dict(),
        ))

    def __init__(self, body_dict=None, yaml_str=None):
        super(ArtifactDescriptor, self).__init__(body_dict, yaml_str)

        try:
            self.entity_name = self.to_dict()['entity']
            self.value_filename = self.to_dict()['filename']
        except KeyError as e:
            raise YamlRecordParsingError(
                "YAML for ArtifactDescriptor was missing field: %s" % e)

    def is_valid(self):
        cache_schema_version =\
            self.to_dict()['provenance']['cache_schema_version']
        return cache_schema_version == CACHE_SCHEMA_VERSION

    def __repr__(self):
        return 'ArtifactDescriptor(%s)' % self.entity_name


class Provenance(YamlDictRecord):
    """
    A compact, unique hash of a (possibly yet-to-be-computed) value.
    Can be used to determine whether a value needs to be recomputed.
    """

    @classmethod
    def from_computation(cls, code_id, case_key, dep_provenances_by_task_key):
        return cls(body_dict=dict(
            cache_schema_version=CACHE_SCHEMA_VERSION,
            python_major_version=PYTHON_MAJOR_VERSION,
            code_id=code_id,
            case_key=dict(case_key),
            dependencies=[
                dict(
                    entity=task_key.entity_name,
                    case_key=dict(task_key.case_key),
                    provenance=provenance.hashed_value,
                )
                for task_key, provenance
                # We need to sort this to make sure our final YAML has
                # deterministic contents.  (When we write to YAML, dict entries
                # are automatically sorted by sort_keys, but lists will keep
                # their original order.)
                in sorted(dep_provenances_by_task_key.items())
            ],
        ))

    def __init__(self, body_dict=None, yaml_str=None):
        super(Provenance, self).__init__(body_dict, yaml_str)
        self.hashed_value = hash_to_hex(self.to_yaml().encode('utf-8'))

    def __repr__(self):
        return 'Provenance(%s...)' % self.hashed_value[:8]
