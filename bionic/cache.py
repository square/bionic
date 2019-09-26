"""
Contains the PersistentCache class, which handles persistent local caching.
This including artifact naming, invalidation, and saving/loading.

"""

from __future__ import absolute_import
from __future__ import division

from builtins import object
import shutil
import subprocess
import tempfile
import textwrap
import yaml
import warnings

import six
from pathlib2 import Path, PurePosixPath

from bionic.exception import UnsupportedSerializedValueError
from .datatypes import Result
from .util import (
    check_exactly_one_present, hash_to_hex, get_gcs_client_without_warnings)

import logging
logger = logging.getLogger(__name__)

try:
    # The C-based YAML emitter is much faster, but requires separate bindings
    # which may not be installed.
    # I'm not sure if it's possible to use a C-based loader, since we're using
    # yaml.full_load.  This is less important since we dump much more than we
    # load.
    YamlDumper = yaml.CDumper
except AttributeError:
    warnings.warn(
        "Failed to find LibYAML bindings; "
        "falling back to slower Python implementation. "
        "This may reduce performance on large flows. "
        "Installing LibYAML should resolve this.")
    YamlDumper = yaml.Dumper

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
        translator = self.translator_for_query(result.query)
        translator.save_result_in_local_if_needed(result)
        translator.copy_local_to_cloud_if_needed()

    def load(self, query):
        translator = self.translator_for_query(query)

        if translator.has_data_in_local():
            result = translator.load_result_from_local()
            # We don't copy to the cloud until we've successfully loaded from
            # local -- just in case the local data is corrupt.
            translator.copy_local_to_cloud_if_needed()
            return result

        elif translator.has_data_in_cloud():
            translator.copy_cloud_to_local()
            return translator.load_result_from_local(
                data_originated_in_cloud=True)

        else:
            return None

    def translator_for_query(self, query):
        return Translator(self, query)


class Translator(object):
    """
    Translates the value of a query among three representations:
    - in memory, as a Python object
    - on the local filesystem
    - in Google Cloud Storage
    """

    def __init__(self, parent_cache, query):
        self._tmp_working_dir_str = parent_cache._tmp_working_dir_str
        self._local_cache = parent_cache._local_cache
        self._cloud_cache = parent_cache._cloud_cache
        self._query = query

        artifact_dir_str = query.provenance.hashed_value
        self._virtual_path = Path(query.entity_name) / artifact_dir_str

    def save_result_in_local_if_needed(self, result):
        working_dir = self._create_tmp_dir()

        try:
            self._save_result_to_dir(result, working_dir)

            if not self.has_data_in_local():
                self._local_cache.move_in(self._virtual_path, working_dir)

        finally:
            self._remove_dir(working_dir)

    def copy_local_to_cloud_if_needed(self):
        if self._cloud_cache is None:
            return
        if self._cloud_cache.has_dir(self._virtual_path):
            return

        logger.info(
            'Uploading   %s to cloud file cache ...',
            self._query.task_key)
        local_dir = self._local_cache.get_real_path(self._virtual_path)
        self._cloud_cache.copy_in(self._virtual_path, local_dir)

    def copy_cloud_to_local(self):
        working_dir = self._create_tmp_dir()

        try:
            logger.info(
                'Downloading %s from cloud file cache ...',
                self._query.task_key)
            self._cloud_cache.copy_out(self._virtual_path, working_dir)
            self._local_cache.move_in(self._virtual_path, working_dir)

        finally:
            self._remove_dir(working_dir)

    def has_data_in_local(self):
        return self._local_cache.has_dir(self._virtual_path)

    def has_data_in_cloud(self):
        return self._cloud_cache is not None and\
            self._cloud_cache.has_dir(self._virtual_path)

    def load_result_from_local(self, data_originated_in_cloud=False):
        local_dir = self._local_cache.get_real_path(self._virtual_path)
        return self._load_result_from_dir(local_dir)

    def _create_tmp_dir(self):
        Path(self._tmp_working_dir_str).mkdir(parents=True, exist_ok=True)
        return Path(tempfile.mkdtemp(dir=self._tmp_working_dir_str))

    def _remove_dir(self, dir_path):
        shutil.rmtree(str(dir_path), ignore_errors=True)

    def _save_result_to_dir(self, result, working_dir):
        value = result.value
        extension = self._query.protocol.file_extension_for_value(value)
        value_filename = VALUE_FILENAME_STEM + extension
        value_path = working_dir / value_filename

        self._query.protocol.write(value, value_path)

        descriptor = ArtifactDescriptor.from_content(
            entity_name=self._query.entity_name,
            value_filename=value_filename,
            provenance=self._query.provenance,
        )
        descriptor_path = working_dir / DESCRIPTOR_FILENAME
        descriptor_path.write_text(descriptor.to_yaml())

    def _load_result_from_dir(
            self, working_dir, data_originated_in_cloud=False):
        try:
            descriptor_path = working_dir / DESCRIPTOR_FILENAME
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
            value_path = working_dir / value_filename
            extension = value_path.name[len(VALUE_FILENAME_STEM):]

            try:
                value = self._query.protocol.read(value_path, extension)
            except UnsupportedSerializedValueError:
                raise
            except Exception as e:
                raise InvalidCacheStateError(
                    "Unable to load value %s due to %s: %s" % (
                        self._query.task_key, e.__class__.__name__, e))

        except InvalidCacheStateError as e:
            problem_sources = [
                str(self._local_cache.get_real_path(self._virtual_path))]
            if data_originated_in_cloud:
                problem_sources.append(
                    self._cloud_cache.get_url(self._virtual_path))

            raise InvalidCacheStateError(
                "Cached data was in an invalid state; "
                "this should be impossible but could have resulted from "
                "either a bug or a change to the cached files.  You "
                "should be able to repair the problem by removing %s."
                "\nDetails: %s" % (' and '.join(problem_sources), e))

        result = Result(
            query=self._query,
            value=value,
            local_cache_path=value_path,
        )

        return result


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

    def move_in(self, virtual_path, src_path):
        """
        Moves a directory into the cache filesystem.
        """

        dst_path = self.root_path / virtual_path
        if dst_path.is_file():
            dst_path.unlink()
        dst_path.mkdir(parents=True, exist_ok=True)

        src_path.rename(dst_path)

    def get_real_path(self, virtual_path):
        """Returns the real path corresponding to the provided virtual path."""
        return self.root_path / virtual_path


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
            dst_url=self.get_url(virtual_path),
        )

        self._marker_blob(virtual_path).upload_from_string(
            self.DIR_MARKER_CONTENTS)

    def copy_out(self, virtual_path, dst_path):
        """Copies a cached directory into a local file directory."""
        dst_path.mkdir(parents=True, exist_ok=True)
        self._gsutil_rsync(
            src_url=self.get_url(virtual_path),
            dst_url=str(dst_path),
        )

    def get_url(self, virtual_path):
        """Returns a ``gs://` URL corresponding to a virtual path."""
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
                Dumper=YamlDumper,
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
