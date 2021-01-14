"""
Builds a docker image for Google AI Platform execution using the current Python
environment.
"""
import pathlib
import subprocess
from concurrent.futures import Future
from concurrent.futures.thread import ThreadPoolExecutor
from textwrap import dedent

from bionic.deps.optdep import import_optional_dependency

import hashlib
import sys
import tempfile
import re
import logging

logger = logging.getLogger(__name__)


_cached_docker_module = None
_cached_docker_client = None


def get_docker_module():
    global _cached_docker_module

    if _cached_docker_module is None:
        _cached_docker_module = import_optional_dependency(
            "docker", purpose="Build Docker images"
        )

    return _cached_docker_module


def get_docker_client():
    global _cached_docker_client

    if _cached_docker_client is None:
        docker = get_docker_module()
        logger.info("Initializing Docker client ...")
        _cached_docker_client = docker.from_env()

    return _cached_docker_client


def fix_pip_requirements(pip_requirements: str) -> str:
    # Pip freeze may contain entries with editable installs pointing to remote
    # git repositories. This can happen when doing Bionic development. Docker
    # service is not able to access repositories using the git+git protocol.
    # Hence, any entries containing git+git is converted to use git+https.
    #
    # Example entry:
    # -e git+git@github.com:square/bionic.git@f13f5405e928d92b553d2cbee41084eecccf7de3#egg=bionic
    #
    # Converted entry:
    # -e git+https://github.com/square/bionic.git@f13f5405e928d92b553d2cbee41084eecccf7de3#egg=bionic

    def fix_line(line: str) -> str:
        if line.startswith("-e git+git"):
            return re.sub(r"-e ([^@]*)@", "-e git+https://", line.replace(":", "/"))
        else:
            return line

    return "\n".join([fix_line(line) for line in pip_requirements.split("\n")])


def get_pip_freeze() -> str:
    return subprocess.run(
        ["pip", "freeze"], capture_output=True, check=True, encoding="utf-8"
    ).stdout


def get_pip_requirements() -> str:
    return fix_pip_requirements(get_pip_freeze())


def get_image_uri(project_name: str, pip_requirements: str) -> str:
    m = hashlib.sha256()
    m.update(pip_requirements.encode("utf-8"))
    m.update(str(sys.version_info).encode("utf-8"))

    image_tag = f"bionic_{m.hexdigest()}"

    return f"gcr.io/{project_name}/bionic:{image_tag}"


def build_image(
    docker_client,
    pip_requirements: str,
    image_uri: str,
):
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = pathlib.Path(tmp_dir)

        (tmp_path / "requirements.txt").write_text(pip_requirements)

        container_image = f"python:{sys.version_info[0]}.{sys.version_info[1]}"

        (tmp_path / "Dockerfile").write_text(
            dedent(
                f"""
                    FROM {container_image}
                    COPY requirements.txt requirements.txt
                    RUN pip install -r requirements.txt
                """
            )
        )

        logger.info(f"Building {image_uri} using {container_image}")
        image, _ = docker_client.images.build(path=tmp_dir, tag=f"{image_uri}")

        logger.info(f"Pushing {image_uri}")
        for line in docker_client.images.push(f"{image_uri}", stream=True, decode=True):
            logger.debug(line)

        logger.info(f"Uploaded {image_uri}")


def build_image_if_missing(project_name: str) -> str:
    pip_requirements = get_pip_requirements()
    image_uri = get_image_uri(project_name, pip_requirements)

    docker = get_docker_module()
    docker_client = get_docker_client()

    try:
        docker_client.images.get_registry_data(image_uri)
        logger.info(f"{image_uri} already exists")
    except docker.errors.NotFound:
        build_image(
            docker_client=docker_client,
            pip_requirements=pip_requirements,
            image_uri=image_uri,
        )

    return image_uri


def build_image_if_missing_async(project_name: str) -> Future:
    return ThreadPoolExecutor(max_workers=1).submit(
        build_image_if_missing, project_name
    )
