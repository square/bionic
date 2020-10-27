import pytest

import json

import bionic as bn
from bionic import interpret
from bionic.utils.urls import (
    path_from_url,
    is_file_url,
    is_gcs_url,
)


class CacheTester:
    """
    A helper class for testing changes to Bionic's cache.

    Tracks the current and previous states of the Cache API, allow us to express tests
    in terms of changes between states.
    """

    def __init__(self, flow, tier=["local", "cloud"], gcs_fs=None):
        self.flow = flow
        self.gcs_fs = gcs_fs
        self._old_entries = set()

        self._tiers = interpret.str_or_seq_as_list(tier)

    def expect_new_entries(self, *expected_new_entity_names):
        cur_entries = set(self._get_entries())
        assert cur_entries.issuperset(self._old_entries)
        new_entries = set(cur_entries) - self._old_entries
        self._old_entries = cur_entries

        new_entity_names = {entry.entity for entry in new_entries}
        assert new_entity_names == set(expected_new_entity_names)

        for entry in new_entries:
            self._validate_entry(entry)

    def expect_removed_entries(self, *expected_removed_entity_names):
        cur_entries = set(self._get_entries())
        assert cur_entries.issubset(self._old_entries)
        removed_entries = self._old_entries - set(cur_entries)
        self._old_entries = cur_entries

        removed_entity_names = {entry.entity for entry in removed_entries}
        assert removed_entity_names == set(expected_removed_entity_names)

    def expect_same_entries(self):
        assert set(self._get_entries()) == self._old_entries

    def expect_zero_entries(self):
        assert list(self._get_entries()) == []

    def _get_entries(self):
        return [
            entry
            for entry in self.flow.cache.get_entries()
            if entry.tier in self._tiers
        ]

    def _validate_entry(self, entry):
        artifact_bytes = read_bytes_from_url(entry.artifact_url, self.gcs_fs)
        value = json.loads(artifact_bytes)
        assert value == self.flow.get(entry.entity)

        if entry.tier == "local":
            artifact_path_bytes = entry.artifact_path.read_bytes()
            assert artifact_path_bytes == artifact_bytes
        else:
            assert entry.artifact_path is None

        # We won't make too many assumptions about the format of the metadata, but we
        # can check that it contains the entity name. (Unfortunately it won't
        # necessarily contain the absolute artifact URL; it may be a relative URL
        # instead.)
        # TODO Hmm, is the above true? On closer inspection, it looks like artifact URLs
        # are derelativized right away when we load the metadata YAML.
        metadata_str = read_bytes_from_url(entry.metadata_url, self.gcs_fs).decode(
            "utf-8"
        )
        assert entry.entity in metadata_str


def read_bytes_from_url(url, gcs_fs):
    """Reads the contents of a URL and returns them as a bytes object."""

    if is_file_url(url):
        path = path_from_url(url)
        return path.read_bytes()
    elif is_gcs_url(url):
        return gcs_fs.cat_file(url)
    else:
        raise AssertionError(f"Unexpected scheme in URL: {url}")


@pytest.fixture
def preset_flow(builder):
    builder.assign("x", 2)
    builder.assign("y", 3)

    @builder
    def xy(x, y):
        return x * y

    @builder
    def xy_squared(xy):
        return xy ** 2

    return builder.build()


def test_get_entries(preset_flow):
    tester = CacheTester(preset_flow)

    tester.expect_zero_entries()

    tester.flow.get("x")
    tester.expect_new_entries("x")

    tester.flow.get("xy")
    tester.expect_new_entries("y", "xy")

    tester.flow.get("xy_squared")
    tester.expect_new_entries("xy_squared")

    tester.flow = tester.flow.setting("x", 4)
    tester.flow.get("xy_squared")
    tester.expect_new_entries("x", "xy", "xy_squared")

    builder = tester.flow.to_builder()

    @builder  # noqa: F811
    @bn.version(1)
    def xy(x, y):  # noqa: F811
        return x ** y

    tester.flow = builder.build()

    tester.flow.get("xy_squared")
    tester.expect_new_entries("xy", "xy_squared")


def test_entry_delete(preset_flow):
    tester = CacheTester(preset_flow)

    tester.flow.get("xy_squared")
    tester.expect_new_entries("x", "y", "xy", "xy_squared")

    (x_entry,) = [
        entry for entry in tester.flow.cache.get_entries() if entry.entity == "x"
    ]
    assert x_entry.delete()
    tester.expect_removed_entries("x")
    assert not x_entry.delete()

    (xy_entry,) = [
        entry for entry in tester.flow.cache.get_entries() if entry.entity == "xy"
    ]
    assert xy_entry.delete()
    tester.expect_removed_entries("xy")
    assert not xy_entry.delete()

    tester.flow = tester.flow.to_builder().build()
    tester.flow.get("xy_squared")
    tester.expect_new_entries("x", "xy")


def test_flow_handles_delete_gracefully(builder):
    builder.assign("a", 1)

    @builder
    @bn.memoize(False)
    def b(a):
        return a + 1

    @builder
    @bn.memoize(False)
    def c(b):
        return b + 1

    flow = builder.build()

    flow.get("b")

    (b_entry,) = [entry for entry in flow.cache.get_entries() if entry.entity == "b"]
    b_entry.delete()

    # The goal here is to make sure that Bionic correctly updates its cache state,
    # detects that `b` is deleted, and recomputes it.
    flow.get("c")


def test_delete_artifact_with_multiple_metadata_files(builder):
    builder.assign("a", 1)

    @builder
    @bn.memoize(False)
    def b(a):
        return 2

    # Define `c` several times, each with non-functional version differences. This means
    # each defintion will have a new metadata entry but the same artifact, so we'll have
    # many entries pointing to the same artifact.
    c_entries = []
    for i in range(4):

        @builder
        @bn.memoize(False)
        @bn.version(minor=i)
        def c(b):
            return b + 1

        flow = builder.build()
        flow.get("c")

        (c_entry,) = [
            entry
            for entry in flow.cache.get_entries()
            if entry.entity == "c" and entry not in c_entries
        ]
        c_entries.append(c_entry)

    # All the entries should have different metadata files.
    assert len(set(entry.metadata_url for entry in c_entries)) == len(c_entries)
    # But they should all share the same artifact file.
    assert len(set(entry.artifact_url for entry in c_entries)) == 1

    # This deletes the artifact and returns True.
    assert c_entries[0].delete()
    # The artifact is already deleted, so this returns False.
    assert not c_entries[1].delete()

    # This should attempt to load the last entry, detect that the artifact is missing,
    # and recompute `c`.
    flow.get("c")

    # Finally, when we look at the cache again, the last of the original entries should
    # be dropped, leaving only the most recent entry. (We computed a new artifact file,
    # but it will have a different (random) URL, so the old entry will still be
    # invalid.)
    (final_c_entry,) = [
        entry for entry in flow.cache.get_entries() if entry.entity == "c"
    ]
    assert final_c_entry.artifact_path.exists()


# It would be nice if we could parameterize the above tests to run with or without GCS.
# However, it doesn't seem to be possible to have a parametrized fixture where only some
# of the variations depend on other fixtures; this is important because the GCS fixtures
# have important setup/teardown properties that we only want to trigger if GCS is
# enabled. (In theory it seems like `request.getfixturevalue` should be able to do
# this, but it has some kind of interaction with the parametrization of
# `parallel_execution_enabled` and breaks.) I think the way forward might be to make
# the GCS setup/teardown into `autouse` fixtures that are directly activated by the GCS
# command line flag.
@pytest.mark.needs_gcs
def test_cache_on_gcs(gcs_builder, gcs_fs):
    builder = gcs_builder

    builder.assign("a", 1)

    @builder
    def b(a):
        return a + 1

    @builder
    def c(b):
        return b + 1

    flow = builder.build()

    local_tester = CacheTester(flow, tier="local", gcs_fs=gcs_fs)
    cloud_tester = CacheTester(flow, tier="cloud", gcs_fs=gcs_fs)
    total_tester = CacheTester(flow, tier=["local", "cloud"], gcs_fs=gcs_fs)

    local_tester.expect_zero_entries()
    cloud_tester.expect_zero_entries()
    total_tester.expect_zero_entries()

    flow.get("b")
    local_tester.expect_new_entries("a", "b")
    cloud_tester.expect_new_entries("a", "b")
    total_tester.expect_new_entries("a", "a", "b", "b")

    flow.get("c")
    local_tester.expect_new_entries("c")
    cloud_tester.expect_new_entries("c")
    total_tester.expect_new_entries("c", "c")

    (local_b_entry,) = [
        entry
        for entry in flow.cache.get_entries()
        if entry.entity == "b" and entry.tier == "local"
    ]
    local_b_entry.delete()
    local_tester.expect_removed_entries("b")
    cloud_tester.expect_same_entries()
    total_tester.expect_removed_entries("b")

    (cloud_c_entry,) = [
        entry
        for entry in flow.cache.get_entries()
        if entry.entity == "c" and entry.tier == "cloud"
    ]
    cloud_c_entry.delete()
    local_tester.expect_same_entries()
    cloud_tester.expect_removed_entries("c")
    total_tester.expect_removed_entries("c")
