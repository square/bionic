"""
Utilities for working with files.
"""

import shutil


def ensure_parent_dir_exists(path):
    ensure_dir_exists(path.parent)


def ensure_dir_exists(path):
    path.mkdir(parents=True, exist_ok=True)


def recursively_copy_path(src_path, dst_path):
    if not src_path.exists():
        raise ValueError(f"Path does not exist: {src_path}")
    ensure_parent_dir_exists(dst_path)

    if src_path.is_file():
        shutil.copyfile(str(src_path), str(dst_path))
    else:
        shutil.copytree(str(src_path), str(dst_path))


def recursively_delete_path(path):
    if not path.exists():
        raise ValueError(f"Path does not exist: {path}")

    if path.is_file():
        path.unlink()
    else:
        shutil.rmtree(path)
