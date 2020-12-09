import io
import re
from concurrent.futures import Future
from concurrent.futures.process import ProcessPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from tempfile import mkdtemp
from typing import Dict
from unittest.mock import Mock

import cloudpickle

from bionic.aip.main import _run as run_aip
from bionic.persistence import LocalStore
from bionic.utils.files import ensure_parent_dir_exists
from bionic.utils.urls import path_from_url


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
    def __init__(self, shared_dict):
        self._files_by_url: dict = shared_dict

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
        for file_url in self._files_by_url.keys():
            assert file_url.startswith("gs://")
            if url in file_url:
                urls.append(file_url[5:])
        return urls

    def isdir(self, url):
        if not url.endswith("/"):
            url = url + "/"
        return any(key for key in self._files_by_url.keys() if key.startswith(url))

    @contextmanager
    def open(self, url, mode):
        assert url.startswith("gs://")

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

    def pipe(self, url, content_bytes):
        with self.open(url, "wb") as f:
            f.write(content_bytes)


class FakeAipClient:
    def __init__(self, gcs_fs, tmp_path):
        self._gcs_fs = gcs_fs
        self._tmp_path = tmp_path
        self._jobs: Dict[str, Future] = {}

    def _create_aip_job(self, body, parent):
        assert body["trainingInput"]["args"][:3] == [
            "python",
            "-m",
            "bionic.aip.main",
        ]

        input_uri = body["trainingInput"]["args"][3]

        # AIP execution does not have access to the disk cache of the local
        # bionic instance. In order to emulate disk filesystem isolation, we can
        # change location of disk cache for the AIP job.
        self._modify_disk_cache_location(input_uri)

        self._jobs[body["jobId"]] = ProcessPoolExecutor(max_workers=1).submit(
            run_aip, input_uri, self._gcs_fs
        )

        return Mock()

    def _modify_disk_cache_location(self, input_uri):
        assert self._gcs_fs.exists(input_uri)
        with self._gcs_fs.open(input_uri, "rb") as f:
            task = cloudpickle.load(f)

        task.function.args[0]._context.core.persistent_cache._local_store = LocalStore(
            mkdtemp(dir=self._tmp_path)
        )

        with self._gcs_fs.open(input_uri, "wb") as f:
            cloudpickle.dump(task, f)

    def _get_aip_job(self, name):
        job_id = name.split("/")[-1]

        assert job_id in self._jobs
        future = self._jobs[job_id]

        if future.done():
            try:
                future.result()
                response = {"state": "SUCCEEDED"}
            except Exception as e:
                # Exception information is only added for convenience. In real
                # GCP, if an AIP job fails, the error message will contain a url
                # instead.
                response = {"state": "FAILED", "errorMessage": repr(e)}
        else:
            response = {"state": "RUNNING"}

        job = Mock()
        job.execute.return_value = response
        return job

    def projects(self):
        # TODO: check that aip_task_config tasks were actually sent to AIP.
        projects = Mock()
        projects.jobs().create = self._create_aip_job
        projects.jobs().get = self._get_aip_job
        return projects


class InstrumentedFilesystem:
    """
    Wraps an fsspec filesystem and counts the number of upload ("put") and download
    ("get") operations.
    """

    def __init__(self, wrapped_fs, make_list):
        self._wrapped_fs = wrapped_fs
        self._downloaded_urls: list = make_list()
        self._uploaded_urls: list = make_list()

    def get_file(self, url, path):
        self._downloaded_urls.append(url)
        return self._wrapped_fs.get_file(url, path)

    def put_file(self, path, url):
        self._uploaded_urls.append(url)
        return self._wrapped_fs.put_file(path, url)

    def __getattr__(self, attr):
        if "_wrapped_fs" not in vars(self):
            raise AttributeError
        return getattr(self._wrapped_fs, attr)

    def matching_urls_downloaded(self, url_regex):
        """
        Returns the number of times a URL matching the provided regex has been
        downloaded since the last time this method was called.
        """
        return self._count_and_delete_matches(self._downloaded_urls, url_regex)

    def matching_urls_uploaded(self, url_regex):
        """
        Returns the number of times a URL matching the provided regex has been
        uploaded since the last time this method was called.
        """
        return self._count_and_delete_matches(self._uploaded_urls, url_regex)

    def _count_and_delete_matches(self, items, regex):
        pattern = re.compile(regex)
        non_matching_items = [item for item in items if not pattern.match(item)]
        match_count = len(items) - len(non_matching_items)
        items[:] = non_matching_items
        return match_count
