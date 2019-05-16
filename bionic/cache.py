'''
Contains the StorageCache class, which handles persistent local caching.  This
including artifact naming, invalidation, and saving/loading.  We may want to
separate these concepts later.
'''

import os
from random import Random
import errno
import operator

import pathlib2 as pl

from entity import Result, Provenance
import protocol as proto

import logging
logger = logging.getLogger(__name__)

CACHEABLE_NAME_PROTOCOL = proto.cleanly_stringable()


class StorageCache(object):
    def __init__(self, root_path_str):
        self._root_path = pl.Path(root_path_str)
        self._random = Random()

    def save(self, result):
        logger.info('Saving result for %r', result.query)

        query = result.query

        new_content_entry = self._candidate_entry_for_query(query)
        official_symlink_entry = self._entry_for_query(query)
        if official_symlink_entry.dir_path.exists():
            old_content_entry = ResourceEntry(
                query.protocol,
                pl.Path(os.readlink(str(official_symlink_entry.dir_path))),
            )
        else:
            old_content_entry = None

        succeeded = False
        try:
            with new_content_entry.value_path.open('wb') as f:
                query.protocol.write(result.value, f)

            # TODO We are writing the hash values as "!!binary" -- can we
            # avoid that?
            with new_content_entry.provenance_path.open('wb') as f:
                f.write(query.provenance.to_yaml())

            n_attempts = 3
            for i in xrange(n_attempts):
                tmp_symlink_path = new_content_entry.dir_path.parent / (
                    'tmp_symlink_' + self._random_str())
                try:
                    # We need to make this an absolute path; symlinking to
                    # a relative path will leave the link in a broken state!
                    abs_content_path = new_content_entry.dir_path.resolve()
                    tmp_symlink_path.symlink_to(abs_content_path)
                    break
                except OSError, e:
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
        logger.info('Loading %r ...', query)

        if not self._query_is_cached(query):
            logger.info('... Query is not cached')
            return None

        cached_provenance = self._load_provenance(query)

        if cached_provenance.hashed_value != query.provenance.hashed_value:
            logger.info('... Query is cached but not up to date')
            return None

        value = self._load_value(query)
        logger.info('... Query successfully retrieved from cache')
        return Result(
            query=query,
            value=value,
        )

    def _query_is_cached(self, query):
        entry = self._entry_for_query(query)

        if not entry.dir_path.exists():
            return False

        self._check_entry_is_valid(entry)

        return True

    def _load_provenance(self, query):
        entry = self._entry_for_query(query)
        with entry.provenance_path.open('rb') as f:
            return Provenance.from_yaml(f.read())

    def _load_value(self, query):
        entry = self._entry_for_query(query)
        with entry.value_path.open('rb') as f:
            return query.protocol.read(f)

    def _erase_entry(self, entry):
        if entry.value_path.exists():
            entry.value_path.unlink()
        if entry.provenance_path.exists():
            entry.provenance_path.unlink()
        if entry.dir_path.exists():
            entry.dir_path.rmdir()

    def _path_for_query(self, query):
        dir_names = [
            item
            for name, value in sorted(query.case_key.iteritems())
            for item in (name, CACHEABLE_NAME_PROTOCOL.stringify(value))
        ]

        return reduce(
            operator.div,
            dir_names,
            self._root_path / query.name,
        )

    def _entry_for_query(self, query):
        return ResourceEntry(
            query.protocol,
            self._path_for_query(query) / 'cur',
        )

    def _candidate_entry_for_query(self, query):
        query_path = self._path_for_query(query)
        n_attempts = 3
        for i in xrange(n_attempts):
            tmp_name = self._random_str()
            entry = ResourceEntry(query.protocol, query_path / tmp_name)

            try:
                entry.dir_path.mkdir(parents=True)
            except OSError, e:
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
        assert entry.value_path.exists()
        assert entry.provenance_path.exists()

    def _random_str(self):
        return '%08x' % self._random.getrandbits(32)


class ResourceEntry(object):
    def __init__(self, protocol, path):
        # TODO Taking a protocol as an argument just for the file suffix feels
        # a little clunky.
        self.protocol = protocol
        self.dir_path = path

        self.value_path = self.dir_path / (
            'value' + self.protocol.file_suffix)
        self.provenance_path = self.dir_path / 'provenance.yaml'

    def __repr__(self):
        return 'ResourceEntry(path=%s)' % self.dir_path
