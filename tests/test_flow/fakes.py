import io
import logging
import os
import shutil
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock
from uuid import uuid4

from bionic import gcs
from bionic.aip import client as aip_client
from bionic.aip.future import Future as AipFuture
from bionic.aip.main import _run as run_aip
from bionic.aip.task import Task as AipTask
from bionic.utils.files import ensure_parent_dir_exists
from bionic.utils.urls import path_from_url


class FakeAipFuture(AipFuture):
    """
    A mock implementation of Future that finished successfully.
    """

    def __init__(self, project_name: str, job_id: str, output: str):
        self.project_name = project_name
        self.job_id = job_id
        self.output = output

    def _get_state_and_error(self):
        from bionic.aip.future import State

        return State.SUCCEEDED, ""


class FakeAipTask(AipTask):
    """
    A mock implementation of Task that runs the job locally using a
    subprocess instead of running it on AIP.
    """

    def submit(self) -> AipFuture:
        self._stage()
        spec = self._ai_platform_job_spec()

        logging.info(f"Submitting test {self.config.project_name}: {self}")
        import subprocess

        subprocess.check_call(spec["trainingInput"]["args"])
        return FakeAipFuture(self.config.project_name, self.job_id(), self.output_uri())


class FakeAipExecutor:
    """
    A mock version of AipExecutor to submit FakeTasks that uses
    subprocess to execute the tasks instead of using AIP. Useful for
    running tests locally without using AIP.
    """

    def __init__(self, aip_config):
        self._aip_config = aip_config

    def submit(self, task_config, fn, *args, **kwargs):
        return FakeAipTask(
            name="a" + str(uuid4()).replace("-", ""),
            config=self._aip_config,
            task_config=task_config,
            function=partial(fn, *args, **kwargs),
        ).submit()


# TODO It would be nice to use a different fsspec implementation here instead
# of writing a custom one. We tried fsspec.implementations.memory.MemoryFileSystem
# implementation, but that has a few problems.
# 1) The glob method returns the protocol gs:// whereas gcsfs implementation doesn't
# 2) There is a bug in the `rm` method and the tests fails to wipe the gcs cache.
#    If you have a dir foo with only one file bar.py and you delete the foo dir
#    recursively, fsspec throws an error after deleting bar.py with a
#    FileNotFoundError for foo dir.
#
# We can override 1) but we need to get 2) fixed in fsspec before we can replace
# this implementation.
class FakeGcsFs:
    def __init__(self):
        self._files_by_url = {}

    def cat_file(self, url):
        with self.open(url, "rb") as f:
            return f.read()

    def exists(self, url):
        return url in self._files_by_url or self.isdir(url)

    def get(self, url, str_path, recursive):
        # We only use get for dirs in Bionic with recursive=True.
        assert recursive
        assert self.isdir(url)

        if str_path.endswith("/"):
            str_path = str_path[:-1]

        file_urls = self._search_files(url)
        for file_url in file_urls:
            relative_path = file_url.replace(url, "")
            file_path = str_path + relative_path
            self.get_file(file_url, file_path)

    def get_file(self, url, str_path):
        ensure_parent_dir_exists(Path(str_path))
        with open(str_path, "wb") as f:
            f.write(self.cat_file(url))

    def _search_files(self, url):
        return ["gs://" + glob_url for glob_url in self.glob(url + "**/*")]

    def glob(self, url):
        # We only use glob in Bionic for urls with **.
        assert url.endswith("**/*")
        url = url[:-4]
        # Glob doesn't return the protocol, so we strip "gs://".
        urls = []
        for file_url in self._files_by_url:
            assert file_url.startswith("gs://")
            if url in file_url:
                urls.append(file_url[5:])
        return urls

    def isdir(self, url):
        if not url.endswith("/"):
            url = url + "/"
        return any(key for key in self._files_by_url if key.startswith(url))

    @contextmanager
    def open(self, url, mode):
        assert mode in ("rb", "wb")
        if mode == "rb" and url not in self._files_by_url:
            raise FileNotFoundError(f"no file found for url {url}")

        f = io.BytesIO(self._files_by_url.get(url, b""))
        yield f
        self._files_by_url[url] = f.getvalue()

    def put(self, str_path, url, recursive):
        # We only use put for dirs in Bionic with recursive=True.
        assert recursive
        path = path_from_url(str_path)
        assert path.is_dir()

        if url.endswith("/"):
            url = url[:-1]

        for file_path in path.glob("**/*"):
            if not file_path.is_file():
                continue
            file_path_str = str(file_path)
            sub_path_str = file_path_str.replace(str_path, "")
            self.put_file(file_path_str, url + sub_path_str)

    def put_file(self, str_path, url):
        with self.open(url, "wb") as f:
            f.write(open(str_path, "rb").read())

    def rm(self, url, recursive=False):
        if recursive:
            for file_url in self._search_files(url):
                del self._files_by_url[file_url]
        else:
            del self._files_by_url[url]


@contextmanager
def temporarily_clean_dir(target_dir):
    """
    Temporarily backs up a directory for the duration of a context. Once the
    context ends, the directory is restored to its original state.
    """
    assert not target_dir.endswith("/")
    with TemporaryDirectory() as tmp_backup_dir:
        try:
            shutil.move(target_dir, tmp_backup_dir)
            yield
        finally:
            shutil.rmtree(target_dir)
            shutil.move(
                os.path.join(tmp_backup_dir, os.path.basename(target_dir)), target_dir
            )


@contextmanager
def run_in_fake_gcp(fake_gcs_fs: FakeGcsFs, caplog, persistent_cache_dir):
    """
    Use fake GCP by mocking out GCS and AIP.
    """

    def create_aip_job(body, parent):
        # AIP executions do not transmit logs to local instance of bionic.
        # Emulate this behavior by setting the log level; this should filter out
        # all (or almost all) the logs. Tests that check the log output will
        # only test against logs which are printed locally.
        #
        # Additionally, AIP executions run in an isolated environment and do not
        # have access to disk cache from local bionic instance. It must retrieve
        # the data from GCS. Emulate this behavior by temporarily removing all
        # files in the tmp_path directory. The files are restored after the
        # AIP computation is completed.
        with caplog.at_level(logging.CRITICAL), temporarily_clean_dir(
            persistent_cache_dir
        ):
            run_aip(body["trainingInput"]["args"][3])
        return Mock()

    mock_aip_client = Mock()
    mock_aip_client.projects().jobs().create = create_aip_job
    mock_aip_client.projects().jobs().get().execute.return_value = {
        "state": "SUCCEEDED"
    }

    cached_gcs_fs = gcs._cached_gcs_fs
    cached_aip_client = aip_client._cached_aip_client
    try:
        gcs._cached_gcs_fs = fake_gcs_fs
        aip_client._cached_aip_client = mock_aip_client
        yield
    finally:
        gcs._cached_gcs_fs = cached_gcs_fs
        aip_client._cached_aip_client = cached_aip_client
