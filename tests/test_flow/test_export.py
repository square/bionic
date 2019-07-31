import pytest
import pickle
from pathlib2 import Path
from subprocess import check_call
from ..helpers import skip_unless_gcs, GCS_TEST_BUCKET


@pytest.fixture(scope='function')
def preset_builder(builder):
    builder.assign('x', 2)
    builder.assign('y', 3)

    @builder
    def f(x, y):
        return x + y

    return builder


@pytest.fixture(scope='function')
def flow(preset_builder):
    return preset_builder.build()


def test_export_filename(flow):
    assert pickle.loads(flow.export('f').read_bytes()) == 5


def test_export_to_new_directory(flow, tmp_path):
    dir_path = tmp_path / 'output'
    flow.export('f', dir_path=dir_path)

    expected_file_path = dir_path / 'f.pkl'
    assert pickle.loads(expected_file_path.read_bytes()) == 5


def test_export_to_existing_directory(flow, tmp_path):
    dir_path = tmp_path / 'output'
    dir_path.mkdir()
    flow.export('f', dir_path=dir_path)

    expected_file_path = dir_path / 'f.pkl'
    assert pickle.loads(expected_file_path.read_bytes()) == 5


def test_export_to_file(flow, tmp_path):
    file_path = tmp_path / 'data.pkl'
    flow.export('f', file_path=file_path)

    assert pickle.loads(file_path.read_bytes()) == 5


@skip_unless_gcs
def test_export_to_gcs_dir(flow, tmp_path):
    flow.export('f', dir_path='gs://' + GCS_TEST_BUCKET)
    src = Path(GCS_TEST_BUCKET) / 'f.pkl'
    dst = tmp_path / 'f.pkl'
    check_call('gsutil -m cp gs://{} {}'.format(src, dst), shell=True)
    assert pickle.loads(dst.read_bytes()) == 5
    check_call('gsutil -m rm gs://{}'.format(src), shell=True)


@skip_unless_gcs
def test_export_to_gcs_file(flow, tmp_path):
    src = str(Path(GCS_TEST_BUCKET) / 'f.pkl')
    flow.export('f', file_path='gs://' + src)
    dst = tmp_path / 'f.pkl'
    check_call('gsutil -m cp gs://{} {}'.format(src, dst), shell=True)
    assert pickle.loads(dst.read_bytes()) == 5
    check_call('gsutil -m rm gs://{}'.format(src), shell=True)
