'''
Contains the PersistentCache class, which handles persistent local caching.
This including artifact naming, invalidation, and saving/loading.  We may want
to separate these concepts later.
'''
from __future__ import absolute_import
from __future__ import division

from builtins import str, range, object
import os
from random import Random
import errno

import pathlib2 as pl

from .datatypes import Result, Provenance

import logging
logger = logging.getLogger(__name__)


class PersistentCache(object):
    def __init__(self, root_path_str):
        self._root_path = pl.Path(root_path_str)
        self._random = Random()

    def save(self, result):
        logger.debug('Saving result for %r', result.query)

        query = result.query

        new_content_entry = self._candidate_entry_for_query(query)
        official_symlink_entry = self._entry_for_query(query)
        if official_symlink_entry.dir_path.exists():
            old_content_entry = ResourceEntry(
                pl.Path(os.readlink(str(official_symlink_entry.dir_path))))
        else:
            old_content_entry = None

        succeeded = False
        try:
            value_path = new_content_entry.value_path_for_result(result)
            with value_path.open('wb') as f:
                query.protocol.write(result.value, f)

            with new_content_entry.provenance_path.open('w') as f:
                f.write(query.provenance.to_yaml())

            n_attempts = 3
            for i in range(n_attempts):
                tmp_symlink_path = new_content_entry.dir_path.parent / (
                    'tmp_symlink_' + self._random_str())
                try:
                    # We need to make this an absolute path; symlinking to
                    # a relative path will leave the link in a broken state!
                    abs_content_path = new_content_entry.dir_path.resolve()
                    tmp_symlink_path.symlink_to(abs_content_path)
                    break
                except OSError as e:
                    if e.errno != errno.EEXIST:
                        raise
                    logger.warning(
                        'Unable to create symlink %s to dir %s',
                        tmp_symlink_path, abs_content_path, exc_info=True)
                    continue
            else:
                raise AssertionError('Unable to create unique symlink file')

            # TODO This will not work atomically (maybe not at all?) on
            # Windows.
            tmp_symlink_path.rename(official_symlink_entry.dir_path)

            self._check_entry_is_valid(official_symlink_entry)

            succeeded = True

        finally:
            if not succeeded:
                try:
                    self._erase_entry(new_content_entry)
                except Exception as e:
                    # If we're here, there must have been an exception in the
                    # previous try block, and we don't want to lose that
                    # exception, so we'll just log this and move on.
                    logger.exception(
                        'Failed to erase entry while cleaning up after '
                        'exception %r', e)

        if old_content_entry is not None:
            self._erase_entry(old_content_entry)

    def load(self, query):
        logger.debug('Loading %r ...', query)

        if not self._query_is_cached(query):
            logger.debug('... Query is not cached')
            return None

        cached_provenance = self._load_provenance(query)

        if cached_provenance.hashed_value != query.provenance.hashed_value:
            logger.debug('... Query is cached but not up to date')
            return None

        value = self._load_value(query)
        logger.debug('... Query successfully retrieved from cache')
        return Result(
            query=query,
            value=value,
            cache_path=self._entry_for_query(query).existing_value_path(),
        )

    def _query_is_cached(self, query):
        entry = self._entry_for_query(query)

        if not entry.dir_path.exists():
            return False

        self._check_entry_is_valid(entry)

        return True

    def _load_provenance(self, query):
        entry = self._entry_for_query(query)
        with entry.provenance_path.open('r') as f:
            return Provenance.from_yaml(f.read())

    def _load_value(self, query):
        entry = self._entry_for_query(query)
        with entry.existing_value_path().open('rb') as f:
            return query.protocol.read(f, entry.existing_value_extension())

    def _erase_entry(self, entry):
        if entry.existing_value_path() is not None:
            entry.existing_value_path().unlink()
        if entry.provenance_path.exists():
            entry.provenance_path.unlink()
        if entry.dir_path.exists():
            entry.dir_path.rmdir()

    def _path_for_query(self, query):
        path = self._root_path / query.name
        for name, token in query.case_key.iteritems():
            for item in (name, token):
                path = path / item
        return path

    def _entry_for_query(self, query):
        return ResourceEntry(self._path_for_query(query) / 'cur')

    def _candidate_entry_for_query(self, query):
        query_path = self._path_for_query(query)
        n_attempts = 3
        for i in range(n_attempts):
            tmp_name = self._random_str()
            entry = ResourceEntry(query_path / tmp_name)

            try:
                entry.dir_path.mkdir(parents=True)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise
                logger.warn(
                    'Unable to create path %s -- it already exists',
                    entry.dir_path)
                continue

            return entry

        raise AssertionError('Unable to create a unique temp directory')

    def _check_entry_is_valid(self, entry):
        assert entry.dir_path.is_dir()
        assert entry.existing_value_path() is not None
        assert entry.provenance_path.exists()

    def _random_str(self):
        return '%08x' % self._random.getrandbits(32)

    def __repr__(self):
        return 'PersistentCache(%r)' % self._root_path


class ResourceEntry(object):
    VALUE_FILE_STEM = 'value.'

    def __init__(self, path):
        self.dir_path = path

        self.provenance_path = self.dir_path / 'provenance.yaml'

    def value_path_for_result(self, result):
        extension = result.query.protocol.file_extension_for_value(
            result.value)
        return self.dir_path / (self.VALUE_FILE_STEM + extension)

    def existing_value_path(self):
        value_filenames = [
            path.name
            for path in self.dir_path.iterdir()
            if path.is_file() and path.name.startswith(self.VALUE_FILE_STEM)
        ]
        if len(value_filenames) == 0:
            return None
        if len(value_filenames) > 1:
            raise AssertionError('Found too many value files in %s: %r' % (
                self.dir_path, value_filenames))
        filename, = value_filenames
        return self.dir_path / filename

    def existing_value_extension(self):
        filename = self.existing_value_path().name
        return filename[len(self.VALUE_FILE_STEM):]

    def __repr__(self):
        return 'ResourceEntry(path=%s)' % self.dir_path
