"""
This module provides local and cloud storage of computed values.  The main
point of entry is the PersistentCache, which encapsulates this functionality.
"""

import attr
import cattr
import os
import shutil
import tempfile
from typing import List, Optional, Tuple
import yaml
import warnings
from uuid import uuid4
from pathlib import Path

from .datatypes import CodeFingerprint, Artifact
from .utils.files import (
    ensure_dir_exists,
    ensure_parent_dir_exists,
    recursively_copy_path,
)
from .utils.misc import hash_simple_obj_to_hex, oneline
from .utils.urls import (
    bucket_and_object_names_from_gs_url,
    derelativize_url,
    path_from_url,
    relativize_url,
    url_from_path,
)

import logging

logger = logging.getLogger(__name__)

try:
    # The C-based YAML emitter is much faster, but requires separate bindings
    # which may not be installed.
    YamlDumper = yaml.CDumper
    YamlLoader = yaml.CLoader
except AttributeError:
    running_under_readthedocs = os.environ.get("READTHEDOCS") == "True"
    if not running_under_readthedocs:
        warnings.warn(
            oneline(
                """
            Failed to find LibYAML bindings;
            falling back to slower Python implementation.
            This may reduce performance on large flows.
            Installing LibYAML should resolve this."""
            )
        )
    YamlDumper = yaml.Dumper
    YamlLoader = yaml.Loader


class PersistentCache:
    """
    Provides a persistent mapping between Provenances (things we could compute) and
    Artifacts (serialized values from those computations). You use it by getting a
    CacheAccessor for your specific Provenance, and then performing load/save
    operations on the accessor.

    When looking up a Provenance, the cache searches for a saved artifact with a
    matching Provenance. The match may not be exact: two Provenances can match at
    different levels of precision, from a "functional" match to an "exact" one. A
    functional match is sufficient to treat two artifacts as interchangeable; the
    finer levels of matching are only used by the "assisted versioning" system, which
    tries to detect situations where a function's bytecode has changed but its
    version hasn't.

    The cache has two tiers: a "local" tier on disk, which is cheap to access,
    and an optional "cloud" tier backed by GCS, which is more expensive to
    access (but globally accessible).  For load operations, the cache returns
    the cheapest artifact that functionally matches the Provenance.  For save
    operations, the cache records an exact entry in both tiers.

    The cache actually has two distinct responsibilities: (a) translating
    between in-memory Python objects and serialized files or blobs, and (b)
    maintaining an "inventory" of these files and blobs.  Currently it makes
    sense to group these responsibilities together at each tier, where the
    local inventory tracks the local files and the cloud inventory tracks the
    cloud blobs.  Each of these tiers is handled by a "store" class.  However,
    in the future we may have other types of persistent artifacts (like
    database tables) which don't have their own inventory type.  In this case
    we might want to split these responsibilities out.
    """

    def __init__(self, local_store, cloud_store):
        self._local_store = local_store
        self._cloud_store = cloud_store

    def get_accessor(self, task_key, provenance):
        return CacheAccessor(self, task_key, provenance)


class CacheAccessor:
    """
    Provides a reference to the cache entries for a specific provenance.  This
    interface is convenient, and it also allows us to maintain some memoized
    state for each provenance, saving redundant lookups.
    """

    def __init__(self, parent_cache, task_key, provenance):
        self.task_key = task_key
        self.provenance = provenance

        self._local = parent_cache._local_store
        self._cloud = parent_cache._cloud_store

        # These values are memoized to avoid roundtrips.
        self._stored_local_entry = None
        self._stored_cloud_entry = None

    def flush_stored_entries(self):
        """
        Flushes the stored local and cloud cached entries.
        """
        self._stored_local_entry = None
        self._stored_cloud_entry = None

    def can_load(self):
        """
        Indicates whether there are any cached artifacts for this provenance.
        """

        try:
            return self._get_nearest_entry_with_artifact() is not None
        except InternalCacheStateError as e:
            self.raise_state_error_with_explanation(e)

    def load_provenance(self):
        """
        Returns the provenance of the nearest cached artifact for this provenance,
        if one exists.
        """

        try:
            entry = self._get_nearest_entry_with_artifact()
            if entry is None:
                return None
            return entry.provenance
        except InternalCacheStateError as e:
            self.raise_state_error_with_explanation(e)

    def load_artifact(self):
        """
        Returns the nearest cached artifact for this provenance, if one exists.
        """

        try:
            entry = self._get_nearest_entry_with_artifact()

            if entry is None:
                return None

            return entry.artifact

        except InternalCacheStateError as e:
            self.raise_state_error_with_explanation(e)

    def replicate_and_load_local_artifact(self):
        """
        If any cached artifact exists, replicates it to each cache layer and returns
        the local copy; otherwise returns None.
        """

        try:
            entry = self._get_nearest_entry_with_artifact()

            if entry is None:
                return None

            if entry.tier == "local":
                local_artifact = entry.artifact

            elif entry.tier == "cloud":
                local_artifact = self._local_artifact_from_cloud(entry.artifact)

            else:
                raise AssertionError("Unrecognized tier: " + entry.tier)

            self._save_or_reregister_artifact(local_artifact)
            return local_artifact

        except InternalCacheStateError as e:
            self.raise_state_error_with_explanation(e)

    def save_local_artifact(self, artifact):
        """
        Saves a local artifact in each cache layer that doesn't already have an exact
        match.
        """

        try:
            self._save_or_reregister_artifact(artifact)
        except InternalCacheStateError as e:
            self.raise_state_error_with_explanation(e)

    def update_provenance(self):
        """
        Adds an entry to each cache layer that doesn't already have an exact
        match for this provenance.  There must be already be at least one cached
        functional match -- i.e., ``can_load()`` must already return True.
        """

        try:
            self._save_or_reregister_artifact(None)
        except InternalCacheStateError as e:
            self.raise_state_error_with_explanation(e)

    # TODO Exposing these two methods publicly is kind of awkward, but it feels too
    # early to try to settle on a cleaner architecture. As we migrate more
    # functionality out of this class, a better organization will hopefully become
    # clearer.
    def generate_unique_local_dir_path(self):
        """
        Generates a random directory path in our local file store, suitable for writing
        an artifact into.
        """
        return self._local.generate_unique_dir_path(self.provenance)

    def raise_state_error_with_explanation(self, source_exc, preamble_message=None):
        """
        Wraps an exception with a message explaining that the exception may originate
        from cache corruption, and how to resolve the problem.
        """
        stores = [self._local]
        if self._cloud:
            stores.append(self._cloud)
        inventory_root_urls = " and ".join(store.inventory.root_url for store in stores)

        message = f"""
        {preamble_message}
        Cached data may be in an invalid state; this should be
        impossible but could have resulted from either a bug or a
        change to the cached files. You should be able to repair
        the problem by removing all cached files under
        {inventory_root_urls}."""
        if preamble_message is None:
            final_message = oneline(message)
        else:
            final_message = oneline(preamble_message) + "\n" + oneline(message)
        raise InvalidCacheStateError(final_message) from source_exc

    def _save_or_reregister_artifact(self, artifact):
        """
        If ``artifact`` is present, saves it in each cache layer that doesn't already
        have an exact match. If not, locates the nearest exactly-matching artifact and
        copies it to the other layers. (If no exactly-matching artifact exists, we
        throw an assertion exception.)
        """

        local_entry = self._get_local_entry()
        cloud_entry = self._get_cloud_entry()
        self.flush_stored_entries()

        local_artifact = artifact
        cloud_artifact = None

        if local_artifact is None:
            if local_entry.artifact is not None:
                local_artifact = local_entry.artifact
            else:
                if cloud_entry is None or cloud_entry.artifact is None:
                    raise AssertionError(
                        oneline(
                            """
                        Attempted to register metadata with no artifact
                        argument and no previously saved values;
                        this suggests we called update_provenance() without
                        previously finding a cached value, which shouldn't
                        happen."""
                        )
                    )
                cloud_artifact = cloud_entry.artifact
                local_artifact = self._local_artifact_from_cloud(cloud_artifact)

        if not local_entry.exactly_matches_provenance:
            local_entry = self._local.inventory.register_artifact(
                self.provenance,
                local_artifact,
            )
        self._stored_local_entry = local_entry

        if self._cloud:
            assert cloud_entry is not None
            if not cloud_entry.exactly_matches_provenance:
                if cloud_artifact is None:
                    if cloud_entry.artifact is not None:
                        cloud_artifact = cloud_entry.artifact
                    else:
                        cloud_artifact = self._cloud_artifact_from_local(local_artifact)
                cloud_entry = self._cloud.inventory.register_artifact(
                    self.provenance,
                    cloud_artifact,
                )
            self._stored_cloud_entry = cloud_entry

    def _get_nearest_entry_with_artifact(self):
        """
        Returns the "nearest" -- i.e., most local -- cache entry for this
        provenance.
        """

        local_entry = self._get_local_entry()
        if local_entry.artifact is not None:
            return local_entry

        cloud_entry = self._get_cloud_entry()
        if cloud_entry is not None and cloud_entry.artifact is not None:
            return cloud_entry

        return None

    def _get_local_entry(self):
        if self._stored_local_entry is None:
            self._stored_local_entry = self._local.inventory.find_entry(self.provenance)
        return self._stored_local_entry

    def _get_cloud_entry(self):
        if self._stored_cloud_entry is None:
            if self._cloud is None:
                return None
            self._stored_cloud_entry = self._cloud.inventory.find_entry(self.provenance)
        return self._stored_cloud_entry

    def _local_artifact_from_cloud(self, cloud_artifact):
        dir_path = self._local.generate_unique_dir_path(self.provenance)
        filename = path_from_url(cloud_artifact.url).name
        file_path = dir_path / filename

        ensure_parent_dir_exists(file_path)

        logger.info("Downloading %s from GCS ...", self.task_key)
        try:
            self._cloud.download(file_path, cloud_artifact.url)
        except Exception as e:
            raise InternalCacheStateError.from_failure(
                "artifact blob",
                cloud_artifact.url,
                e,
            )

        return Artifact(
            url=url_from_path(file_path),
            content_hash=cloud_artifact.content_hash,
        )

    def _cloud_artifact_from_local(self, local_artifact):
        url_prefix = self._cloud.generate_unique_url_prefix(self.provenance)
        file_path = path_from_url(local_artifact.url)
        blob_url = url_prefix + "/" + file_path.name

        logger.info("Uploading %s to GCS ...", self.task_key)
        try:
            self._cloud.upload(file_path, blob_url)
        except Exception as e:
            raise InternalCacheStateError.from_failure("artifact file", file_path, e)

        return Artifact(
            url=blob_url,
            content_hash=local_artifact.content_hash,
        )


@attr.s(frozen=True)
class NullableWrapper:
    """
    A simple wrapper for a value that might be None.  We use this when we want
    to distinguish between "we have a value which is None" from "we don't have a
    value".
    """

    value = attr.ib()


@attr.s(frozen=True)
class InventoryEntry:
    """
    Represents a saved artifact tracked by an Inventory; returned by Inventory
    to CacheAccessor.
    """

    tier = attr.ib()
    provenance = attr.ib()
    exactly_matches_provenance = attr.ib()
    artifact = attr.ib()


@attr.s(frozen=True)
class MetadataMatch:
    """
    Represents a match between a provenance and a saved artifact.  `level` is a string
    describing the match level, ranging from "functional" to "exact".
    """

    metadata_url = attr.ib()
    level = attr.ib()


# TODO Should we merge this with InventoryEntry?
@attr.s(frozen=True)
class ExternalCacheItem:
    """
    Represents an inventory entry, but contains data intended to be exposed to users
    via the Cache class.
    """

    inventory = attr.ib()
    abs_artifact_url = attr.ib()
    abs_metadata_url = attr.ib()
    descriptor = attr.ib()


class Inventory:
    """
    Maintains a persistent mapping from Queries to artifact URLs.  An Inventory
    is backed by a "file system", which could correspond to either a local disk
    or a cloud storage service.  This file system is used to store
    metadata records, each of which describes a provenance and an artifact URL that
    satisfies it. Metadata records are stored using a hierarchical naming
    scheme whose levels correspond to the different levels of Provenance
    matching.
    """

    def __init__(self, name, tier, filesystem):
        self.name = name
        self.tier = tier
        self._fs = filesystem
        self.root_url = filesystem.root_url

    def register_artifact(self, provenance, artifact):
        """
        Records metadata indicating that the provided Provenance is satisfied
        by the provided Artifact, and returns a corresponding InventoryEntry.
        """

        logger.debug(
            "In     %s inventory for %r, saving artifact URL %s ...",
            self.tier,
            provenance,
            artifact.url,
        )

        expected_metadata_url = self._exact_metadata_url_for_provenance(provenance)
        metadata_record = None
        if self._fs.exists(expected_metadata_url):
            # This shouldn't happen, because the CacheAccessor shouldn't write
            # to this inventory if we already have an exact match.
            logger.warn(
                "In %s cache, attempted to create duplicate entry mapping %r " "to %s",
                self.tier,
                provenance,
                artifact.url,
            )
            metadata_record = self._load_metadata_if_valid_else_delete(
                expected_metadata_url,
            )

        if metadata_record is None:
            metadata_url, metadata_record = self._create_and_write_metadata(
                provenance, artifact
            )

            assert metadata_url == expected_metadata_url
            logger.debug(
                "... in %s inventory for %r, created metadata record at %s",
                self.tier,
                provenance,
                metadata_url,
            )

        return InventoryEntry(
            tier=self.tier,
            provenance=metadata_record.provenance,
            exactly_matches_provenance=True,
            artifact=artifact,
        )

    def find_entry(self, provenance):
        """
        Returns an InventoryEntry describing the closest match to the provided
        Provenance.
        """

        logger.debug("In     %s inventory for %r, searching ...", self.tier, provenance)

        n_prior_attempts = 0
        while True:
            if n_prior_attempts in (10, 100, 1000, 10000, 100000, 1000000):
                message = f"""
                While searching in the {self.tier} cache for an entry matching
                {provenance!r}, found {n_prior_attempts} invalid metadata files;
                either a lot of artifact files were manually deleted,
                or there's a bug in the cache code
                """
                if n_prior_attempts == 1000000:
                    raise AssertionError("Giving up: " + oneline(message))
                else:
                    logger.warn(oneline(message))
            n_prior_attempts += 1

            match = self._find_best_match(provenance)
            if not match:
                logger.debug(
                    "... in %s inventory for %r, found no match", self.tier, provenance
                )

                return InventoryEntry(
                    tier=self.tier,
                    provenance=None,
                    exactly_matches_provenance=False,
                    artifact=None,
                )

            metadata_record = self._load_metadata_if_valid_else_delete(
                match.metadata_url
            )
            if metadata_record is None:
                continue

            logger.debug(
                "... in %s inventory for %r, found %s match at %s",
                self.tier,
                provenance,
                match.level,
                match.metadata_url,
            )

            return InventoryEntry(
                tier=self.tier,
                provenance=metadata_record.provenance,
                exactly_matches_provenance=(match.level == "exact"),
                artifact=Artifact(
                    url=metadata_record.artifact_url,
                    content_hash=metadata_record.value_hash,
                ),
            )

    def list_items(self):
        metadata_urls = [
            url for url in self._fs.search(self.root_url) if url.endswith(".yaml")
        ]

        for metadata_url in metadata_urls:
            metadata_record = self._load_metadata_if_valid_else_delete(metadata_url)
            if metadata_record is None:
                continue
            artifact_url = metadata_record.artifact_url
            yield ExternalCacheItem(
                inventory=self,
                abs_artifact_url=derelativize_url(artifact_url, metadata_url),
                abs_metadata_url=metadata_url,
                descriptor=metadata_record.descriptor,
            )

    def delete_url(self, url):
        if not self._fs.exists(url):
            return False
        self._fs.rm(url)
        return True

    def _find_best_match(self, provenance):
        equivalent_url_prefix = self._equivalent_metadata_url_prefix_for_provenance(
            provenance
        )
        possible_urls = self._fs.search(equivalent_url_prefix)
        equivalent_urls = [url for url in possible_urls if url.endswith(".yaml")]
        if len(equivalent_urls) == 0:
            return None

        exact_url = self._exact_metadata_url_for_provenance(provenance)
        if exact_url in equivalent_urls:
            return MetadataMatch(
                metadata_url=exact_url,
                level="exact",
            )

        nominal_url_prefix = self._nominal_metadata_url_prefix_for_provenance(
            provenance
        )
        nominal_urls = [
            url for url in equivalent_urls if url.startswith(nominal_url_prefix)
        ]
        if len(nominal_urls) > 0:
            return MetadataMatch(
                metadata_url=nominal_urls[0],
                level="nominal",
            )

        return MetadataMatch(
            metadata_url=equivalent_urls[0],
            level="equivalent",
        )

    def _equivalent_metadata_url_prefix_for_provenance(self, provenance):
        return (
            self._fs.root_url
            + "/"
            + valid_filename_from_provenance(provenance)
            + "/"
            + provenance.functional_hash
        )

    def _nominal_metadata_url_prefix_for_provenance(self, provenance):
        return (
            self._equivalent_metadata_url_prefix_for_provenance(provenance)
            + "/"
            + provenance.nominal_hash
        )

    def _exact_metadata_url_for_provenance(self, provenance):
        filename = f"metadata_{provenance.exact_hash}.yaml"
        return (
            self._nominal_metadata_url_prefix_for_provenance(provenance)
            + "/"
            + filename
        )

    def _load_metadata_if_valid_else_delete(self, url):
        try:
            metadata_yaml = self._fs.read_bytes(url).decode("utf8")
            metadata_record = ArtifactMetadataRecord.from_relativized_yaml(
                metadata_yaml,
                url,
            )
        except Exception as e:
            raise InternalCacheStateError.from_failure("metadata record", url, e)

        if not self._fs.exists(metadata_record.artifact_url):
            logger.info(
                "Found invalid metadata record at %s, "
                "referring to nonexistent artifact at %s; "
                "deleting metadata record",
                url,
                metadata_record.artifact_url,
            )
            self.delete_url(url)
            return None

        else:
            return metadata_record

    def _create_and_write_metadata(self, provenance, artifact):
        metadata_url = self._exact_metadata_url_for_provenance(provenance)

        metadata_record = ArtifactMetadataRecord(
            descriptor=provenance.descriptor,
            artifact_url=artifact.url,
            provenance=provenance,
            value_hash=artifact.content_hash,
        )
        metadata_yaml = metadata_record.to_relativized_yaml(metadata_url)
        self._fs.write_bytes(metadata_yaml.encode("utf8"), metadata_url)

        return metadata_url, metadata_record


class LocalStore:
    """
    Represents the local disk cache.  Provides both an Inventory that manages
    artifact (file) URLs, and a method to generate those URLs (for creating
    new files).
    """

    def __init__(self, root_path_str):
        root_path = Path(root_path_str).absolute()
        self._artifact_root_path = root_path / "artifacts"

        inventory_root_path = root_path / "inventory"
        tmp_root_path = root_path / "tmp"
        self.inventory = Inventory(
            "local disk", "local", LocalFilesystem(inventory_root_path, tmp_root_path)
        )

    def generate_unique_dir_path(self, provenance):
        n_attempts = 0
        while True:
            # TODO This path can be anything as long as it's unique, so we
            # could make it more human-readable.
            path = (
                self._artifact_root_path
                / valid_filename_from_provenance(provenance)
                / str(uuid4())
            )

            if not path.exists():
                return path
            else:
                n_attempts += 1
                if n_attempts > 3:
                    raise AssertionError(
                        oneline(
                            f"""
                        Repeatedly failed to randomly generate a novel
                        directory name; {path} already exists"""
                        )
                    )


class GcsCloudStore:
    """
    Represents the GCS cloud cache.  Provides both an Inventory that manages
    artifact (blob) URLs, and a method to generate those URLs (for creating
    those blobs).
    """

    def __init__(self, gcs_fs, url):
        self._fs = GcsFilesystem(gcs_fs, url, "/inventory")

        self.inventory = Inventory("GCS", "cloud", self._fs)
        self._artifact_root_url_prefix = url + "/artifacts"

    def generate_unique_url_prefix(self, provenance):
        n_attempts = 0
        while True:
            # TODO This path can be anything as long as it's unique, so we
            # could make it more human-readable.
            url_prefix = "/".join(
                [
                    str(self._artifact_root_url_prefix),
                    valid_filename_from_provenance(provenance),
                    str(uuid4()),
                ]
            )

            if not self._fs.exists(url_prefix):
                return url_prefix
            else:
                n_attempts += 1
                if n_attempts > 3:
                    raise AssertionError(
                        oneline(
                            f"""
                        Repeatedly failed to randomly generate a novel
                        blob name; {self._artifact_root_url_prefix}
                        already exists"""
                        )
                    )

    def upload(self, path, url):
        if path.is_dir():
            self._fs.put_dir(path, url)
        else:
            assert path.is_file()
            self._fs.put_file(path, url)

    def download(self, path, url):
        if self._fs.isdir(url):
            self._fs.get_dir(url, path)
        else:
            self._fs.get_file(url, path)


class FakeCloudStore(LocalStore):
    """
    A mock version of the GcsCloudStore that's actually backed by local files.
    Useful for running tests without setting up a GCS connection, which is
    slow and requires some configuration.
    """

    def __init__(self, root_path_str):
        super(FakeCloudStore, self).__init__(root_path_str)

    def generate_unique_url_prefix(self, provenance):
        return url_from_path(self.generate_unique_dir_path(provenance))

    def upload(self, path, url):
        src_path = path
        dst_path = path_from_url(url)

        recursively_copy_path(src_path, dst_path)

    def download(self, path, url):
        src_path = path_from_url(url)
        dst_path = path

        recursively_copy_path(src_path, dst_path)


class LocalFilesystem:
    """
    Implements a generic "FileSystem" interface for reading/writing small files
    to local disk.
    """

    def __init__(self, inventory_dir, tmp_dir):
        self.root_url = url_from_path(inventory_dir)
        self.tmp_root_path = tmp_dir

    def exists(self, url):
        return path_from_url(url).exists()

    def search(self, url_prefix):
        path_prefix = path_from_url(url_prefix)
        if not path_prefix.is_dir():
            return []

        return [
            url_from_path(path_prefix / sub_path)
            for sub_path in path_prefix.glob("**/*")
        ]

    def rm(self, url):
        path = path_from_url(url)
        path.unlink()

    def write_bytes(self, content_bytes, url):
        path = path_from_url(url)
        ensure_parent_dir_exists(path)
        ensure_dir_exists(self.tmp_root_path)
        working_dir = Path(tempfile.mkdtemp(dir=str(self.tmp_root_path)))
        try:
            working_path = working_dir / "tmp_file"
            working_path.write_bytes(content_bytes)

            working_path.rename(path)

        finally:
            shutil.rmtree(str(working_dir))

    def read_bytes(self, url):
        return path_from_url(url).read_bytes()


class GcsFilesystem:
    """
    Wrapper around fsspec's GCS "FileSystem" that validates the bucket
    and object_prefix. It also exposes extra APIs, namely, search,
    write_bytes, and read_bytes for convenience and consistency with
    LocalFilesystem.
    """

    def __init__(self, gcs_fs, url, object_prefix_extension):
        if url.endswith("/"):
            url = url[:-1]
        self.root_url = url + object_prefix_extension

        bucket_name, object_prefix = bucket_and_object_names_from_gs_url(url)
        self._bucket_name = bucket_name
        self._object_prefix = object_prefix
        self._fs = gcs_fs

    def exists(self, url):
        self._validate_object_name_from_url(url)
        return self._fs.exists(url)

    def search(self, url_prefix):
        # This endpoint is a glob **/* search.
        glob_url = url_prefix + "**/*"
        return ["gs://" + url for url in self._fs.glob(glob_url)]

    def rm(self, url):
        self._validate_object_name_from_url(url)
        self._fs.rm(url)

    def write_bytes(self, content_bytes, url):
        self._validate_object_name_from_url(url)
        self._fs.pipe(url, content_bytes)

    def read_bytes(self, url):
        self._validate_object_name_from_url(url)
        return self._fs.cat_file(url)

    def get_dir(self, url, path):
        self._validate_object_name_from_url(url)
        return self._fs.get(url, str(path), recursive=True)

    def get_file(self, url, path):
        self._validate_object_name_from_url(url)
        return self._fs.get_file(url, str(path))

    def put_dir(self, path, url):
        self._validate_object_name_from_url(url)
        return self._fs.put(str(path), url, recursive=True)

    def put_file(self, path, url):
        self._validate_object_name_from_url(url)
        # If gcs url is a folder, we want to write the file in the folder.
        # There seems to be a bug in fsspec due to which, the file is uploaded
        # as the url, instead of inside the folder. What this means is, writing
        # a file c.json to gs://a/b/ would result in file gs://a/b instead of
        # gs://a/b/c.json.
        #
        # `put` API is supposed to write the file inside the folder but it strips
        # the ending "/" at the end in fsspec's `_strip_protocol` method.
        if url.endswith("/"):
            url = url + path.name
        return self._fs.put_file(str(path), url)

    def isdir(self, url):
        self._validate_object_name_from_url(url)
        return self._fs.isdir(url)

    def _validate_object_name_from_url(self, url):
        bucket_name, object_name = bucket_and_object_names_from_gs_url(url)
        assert bucket_name == self._bucket_name
        assert object_name.startswith(self._object_prefix)


class InternalCacheStateError(Exception):
    """
    Indicates a problem with the integrity of our cached data.  Before this is
    surfaced to a user, it should be converted to an InvalidCacheStateError.
    """

    @classmethod
    def from_failure(cls, artifact_type, location, exc):
        return cls(f"Unable to read {artifact_type} {location!r} in cache: {exc}")


class InvalidCacheStateError(Exception):
    """
    Indicates that the cache state may have been corrupted.
    """


# TODO Do we ever use this for descriptors that are not just entity names? If so, should
# we worry about other special characters in descriptors?
def valid_filename_from_provenance(provenance):
    """
    Generates a filename from a provenance.

    This just converts the node to a descriptor string and replaces any spaces with
    hyphens.
    """
    return provenance.descriptor.replace(" ", "-")


# Next time we change this, it would be nice to also update ArtifactMetadataRecord to
# contain an Artifact directly.
CACHE_SCHEMA_VERSION = 10


class YamlRecordParsingError(Exception):
    pass


# We don't use frozen=True because Provenance has list fields, which are not hashable.
# We could replace them with tuples, but those result in annoying `!!python/tuple`
# annotations when serialized to YAML.
@attr.s
class Provenance:
    """
    Describes the code and data used to generate (possibly-yet-to-be-computed)
    value.  Provides a set of hashes that can be used to determine if two
    such values are meaningfully different, without actually examining the
    values.

    A provenance summarizes the following information:
    1. The code used to generate the value for a descriptor.
    2. Digests (ProvenanceDigest objects, containing various hashes) of each
      dependency value consumed by the code.

    These digests may correspond to either persisted or non-persisted value; in the
    latter case, the digest contains a reference to that value's provenance as well.
    This means that each provenance incorporates information not just about its own
    value's code, but also the code of any immediate non-persisted ancestors. The
    point of this is to allow us to detect and diagnose "versioning errors":
    situations where a user has modified the code of one of their functions but has
    forgotten to update its @version decorator. When we load a persisted value, we
    can compare its persisted provenance to its expected provenance to determine if
    any of the code used to generate that value has changed.

    Provenances can "match" at several different levels of precision.

    1. Functional match: the code generating this value has the same major version,
    and all dependency data functionally matches. This is the lowest level of
    matching, but it's a sufficient condition to treat two artifacts as
    interchangeable. The only purpose of the higher levels is to allow recursive
    searches for possible versioning errors.

    2. Nominal match: the code generating this value has the same version (both major
    and minor), and all dependency data nominally matches. If two provenances *don't*
    nominally match, then code of different versions was used to generate their
    values, which rules out the possibility of a versioning error.

    3. Exact match: The code generating this value has the same version and the same
    bytecode, and all dependency data exactly matches. If two provenances *do*
    exactly match, then the same code should have been used to generate their values,
    so again there must not be a versioning error.

    If two provenances *do* match nominally but *don't* match exactly, we know there
    must be a versioning error.
    """

    descriptor: str = attr.ib()
    # We don't use this for anything; it's just included to allow humans to distinguish
    # between different cases when examining a serialized YAML file.
    case_key_elements: List[Tuple[str, str]] = attr.ib()

    dep_digests: List["ProvenanceDigest"] = attr.ib()

    code_fingerprint: CodeFingerprint = attr.ib()

    functional_hash: str = attr.ib()
    nominal_hash: str = attr.ib()
    exact_hash: str = attr.ib()

    @classmethod
    def from_computation(
        cls,
        task_key,
        code_fingerprint,
        dep_provenance_digests_by_task_key,
        treat_bytecode_as_functional,
        can_functionally_change_per_run,
        flow_instance_uuid,
    ):
        sorted_dep_digests = [
            dep_digest
            for _, dep_digest in sorted(dep_provenance_digests_by_task_key.items())
        ]

        if code_fingerprint.is_identity:
            (dep_digest,) = sorted_dep_digests

            functional_hash = dep_digest.functional_hash
            nominal_hash = dep_digest.nominal_hash
            exact_hash = dep_digest.exact_hash

        else:
            functional_hash = hash_simple_obj_to_hex(
                [
                    [dep_digest.functional_hash for dep_digest in sorted_dep_digests],
                    code_fingerprint.orig_flow_name,
                    code_fingerprint.version.major,
                    CACHE_SCHEMA_VERSION,
                    (
                        code_fingerprint.bytecode_hash
                        if treat_bytecode_as_functional
                        else None
                    ),
                    (flow_instance_uuid if can_functionally_change_per_run else None),
                ]
            )
            nominal_hash = hash_simple_obj_to_hex(
                [
                    functional_hash,
                    [dep_digest.nominal_hash for dep_digest in sorted_dep_digests],
                    code_fingerprint.version.minor,
                ]
            )
            exact_hash = hash_simple_obj_to_hex(
                [
                    nominal_hash,
                    [dep_digest.exact_hash for dep_digest in sorted_dep_digests],
                    code_fingerprint.bytecode_hash,
                ]
            )

        return cls(
            descriptor=task_key.dnode.to_descriptor(),
            case_key_elements=list(sorted(task_key.case_key.items())),
            dep_digests=sorted_dep_digests,
            code_fingerprint=code_fingerprint,
            functional_hash=functional_hash,
            nominal_hash=nominal_hash,
            exact_hash=exact_hash,
        )

    def __repr__(self):
        hash_fn = self.functional_hash[:8]
        v_maj = self.code_version_major
        v_min = self.code_version_minor
        hash_ex = self.exact_hash[:8]
        return f"Provenance[{hash_fn}/{v_maj}.{v_min}/{hash_ex}]"

    @property
    def code_version_major(self):
        return self.code_fingerprint.version.major

    @property
    def code_version_minor(self):
        return self.code_fingerprint.version.minor

    @property
    def bytecode_hash(self):
        return self.code_fingerprint.bytecode_hash

    def nominally_matches(self, prov):
        return self.nominal_hash == prov.nominal_hash

    def exactly_matches(self, prov):
        return self.exact_hash == prov.exact_hash


@attr.s
class ProvenanceDigest:
    """
    A collection of values used by Provenance for different chained hashes.
    These hashes depend on the entities and can come from either another
    provenance or the value hash of a result.
    """

    functional_hash: str = attr.ib()
    nominal_hash: str = attr.ib()
    exact_hash: str = attr.ib()
    provenance: Optional[Provenance] = attr.ib()

    @classmethod
    def from_provenance(cls, provenance):
        return cls(
            functional_hash=provenance.functional_hash,
            nominal_hash=provenance.nominal_hash,
            exact_hash=provenance.exact_hash,
            provenance=provenance,
        )

    @classmethod
    def from_value_hash(cls, value_hash):
        return cls(
            functional_hash=value_hash,
            nominal_hash=value_hash,
            exact_hash=value_hash,
            provenance=None,
        )


# This converts the field Provenance.dep_digests from a forward reference to a real
# type; if we don't do this, cattr will fail to handle it properly. (This could also be
# resolved by adding a structure hook for ForwardReference('ProvenanceDigest'), but
# that isn't available in Python 3.6. It can be accessed as _ForwardReference, but that
# seemed to cause the interpreter to abort unpredictably in our tests so I didn't want
# to use it. Similarly, we can use `from __future__ import annotations` to remove
# the need forward references, but that also isn't available in 3.6.)
attr.resolve_types(Provenance)
METADATA_CONVERTER = cattr.Converter()


# TODO It would be nice if this contained an Artifact object instead of the separate
# URL and hash, but that will require changing the cache schema. Maybe we can do this
# along with the next schema-breaking change.
@attr.s
class ArtifactMetadataRecord:
    """
    Describes a persisted artifact.  Intended to be stored as a YAML file.
    """

    descriptor: str = attr.ib()
    artifact_url: str = attr.ib()
    provenance: Provenance = attr.ib()
    value_hash: str = attr.ib()

    def to_relativized_yaml(self, metadata_url):
        """
        Serializes this object to a YAML string suitable for storage.

        The YAML representation directly maps to the structure of this class, except
        that the ``artifact_url`` field will be converted to be relative to
        ``metadata_url``. This allows the artifact and metadata files to be moved
        around, as long as their relative position remains the same.
        """
        relativized_record = self._relativize(metadata_url)
        body_dict = METADATA_CONVERTER.unstructure(relativized_record)
        return yaml.dump(
            body_dict,
            default_flow_style=False,
            encoding=None,
            Dumper=YamlDumper,
        )

    @classmethod
    def from_relativized_yaml(cls, yaml_str, metadata_url):
        """
        Deserializes this object from a YAML string.

        This is the inverse operation to ``to_relativized_yaml``, and it correspondingly
        reverses the relativization of the artifact URL: the deserialized object will
        have an absolute ``artifact_url`` field.
        """
        try:
            body_dict = yaml.load(yaml_str, Loader=YamlLoader)
            relativized_record = METADATA_CONVERTER.structure(
                body_dict, ArtifactMetadataRecord
            )
        except Exception as e:
            raise YamlRecordParsingError(f"Couldn't parse {cls.__name__}: {e}") from e

        return relativized_record._derelativize(metadata_url)

    def __repr__(self):
        return f"ArtifactMetadataRecord({self.descriptor!r})"

    def _relativize(self, metadata_url):
        return attr.evolve(
            self,
            artifact_url=relativize_url(self.artifact_url, metadata_url),
        )

    def _derelativize(self, metadata_url):
        return attr.evolve(
            self,
            artifact_url=derelativize_url(self.artifact_url, metadata_url),
        )
