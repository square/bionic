import pytest

import pickle
from pathlib import Path
from subprocess import check_call

import dask.dataframe as dd

from ..helpers import (
    skip_unless_gcs, GCS_TEST_BUCKET, df_from_csv_str,
    equal_frame_and_index_content)

import bionic as bn


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


@pytest.fixture(scope='function')
def expected_dask_df():
    df_value = df_from_csv_str('''
    color,number
    red,1
    blue,2
    green,3
    ''')
    return dd.from_pandas(df_value, npartitions=1)


@pytest.fixture(scope='function')
def dask_flow(builder, expected_dask_df):
    @builder
    @bn.protocol.dask
    def dask_df():
        return expected_dask_df

    return builder.build()


def test_copy_file_to_existing_local_dir(flow, tmp_path):
    dir_path = tmp_path / 'output'
    dir_path.mkdir()
    flow.get('f', mode='FileCopier').copy(destination=dir_path)

    expected_file_path = dir_path / 'f.pkl'
    assert pickle.loads(expected_file_path.read_bytes()) == 5


def test_copy_file_to_local_file(flow, tmp_path):
    file_path = tmp_path / 'data.pkl'
    flow.get('f', mode='FileCopier').copy(destination=file_path)

    assert pickle.loads(file_path.read_bytes()) == 5


def test_copy_file_to_local_file_using_str(flow, tmp_path):
    file_path = tmp_path / 'data.pkl'
    file_path_str = str(file_path)
    flow.get('f', mode='FileCopier').copy(destination=file_path_str)
    assert pickle.loads(file_path.read_bytes()) == 5


# TODO Add separate setup and teardown steps (similar to test_persistence_gcs)
@skip_unless_gcs
def test_copy_file_to_gcs_dir(flow, tmp_path):
    flow.get('f', mode='FileCopier').copy(destination='gs://' + GCS_TEST_BUCKET)
    cloud_path = Path(GCS_TEST_BUCKET) / 'f.pkl'
    local_path = tmp_path / 'f.pkl'
    check_call(f'gsutil -m cp gs://{cloud_path} {local_path}', shell=True)
    assert pickle.loads(local_path.read_bytes()) == 5
    check_call(f'gsutil -m rm gs://{cloud_path}', shell=True)


@skip_unless_gcs
def test_copy_file_to_gcs_file(flow, tmp_path):
    cloud_path = str(Path(GCS_TEST_BUCKET) / 'f.pkl')
    flow.get('f', mode='FileCopier').copy(destination='gs://' + cloud_path)
    local_path = tmp_path / 'f.pkl'
    check_call(f'gsutil -m cp gs://{cloud_path} {local_path}', shell=True)
    assert pickle.loads(local_path.read_bytes()) == 5
    check_call(f'gsutil -m rm gs://{cloud_path}', shell=True)


def test_copy_dask_to_dir(tmp_path, expected_dask_df, dask_flow):
    destination = tmp_path / 'output'
    destination.mkdir()
    expected_dir_path = destination / 'dask_df.pq.dask'

    dask_flow.get('dask_df', mode='FileCopier').copy(destination=destination)

    actual = dd.read_parquet(expected_dir_path)
    assert equal_frame_and_index_content(actual.compute(), expected_dask_df.compute())


@skip_unless_gcs
def test_copy_dask_to_gcs_dir(tmp_path, expected_dask_df, dask_flow):
    cloud_path = str(Path(GCS_TEST_BUCKET) / 'output')
    dask_flow.get('dask_df', mode='FileCopier').copy(destination='gs://' + cloud_path)

    check_call(f'gsutil -m cp -r gs://{cloud_path} {tmp_path}', shell=True)
    actual = dd.read_parquet(tmp_path / 'output')
    assert equal_frame_and_index_content(actual.compute(), expected_dask_df.compute())
    check_call(f'gsutil -m rm -r gs://{cloud_path}', shell=True)


def test_get_multi_value_entity(builder):
    my_set = {"oscar", "the", "grouch"}
    builder.assign('val', values=my_set)

    @builder
    def multi_entity(val):
        return val

    flow = builder.build()
    results = flow.get('multi_entity', collection=set, mode=Path)
    results = {pickle.loads(res.read_bytes()) for res in results}

    assert results == my_set
