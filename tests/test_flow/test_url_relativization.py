from bionic.cache import relativize_url, derelativize_url


rel_artifact_url = '../artifacts/artifact.pkl'
abs_artifact_url = 'file:///Users/User/cache/artifacts/artifact.pkl'
abs_metadata_url = 'file:///Users/User/cache/metadata/metadata.yaml'
gcs_artifact_url = 'gs://my_bucket/cache/artifacts/artifact.pkl'
gcs_metadata_url = 'gs://my_bucket/cache/metadata/metadata.yaml'


# file url tests
def test_relativize_abs_file_urls():
    assert relativize_url(abs_artifact_url, abs_metadata_url) == \
        rel_artifact_url


def test_relativize_relative_file_urls():
    assert relativize_url(rel_artifact_url, abs_metadata_url) == \
        rel_artifact_url


def test_derelativize_abs_file_urls():
    assert derelativize_url(abs_artifact_url, abs_metadata_url) == \
        abs_artifact_url


def test_derelativize_relative_file_urls():
    assert derelativize_url(rel_artifact_url, abs_metadata_url) == \
        abs_artifact_url


# gcs url tests
def test_relativize_gcs_urls():
    assert relativize_url(gcs_artifact_url, gcs_metadata_url) == \
        gcs_artifact_url


def test_derelativize_gcs_urls():
    assert relativize_url(gcs_artifact_url, gcs_metadata_url) == \
        gcs_artifact_url
