"""Utility module with functions to assist with copying, moving and removing files and
directories."""

from __future__ import annotations
from pathlib import Path
import shutil

from hpcflow.sdk.typing import PathLike


def copy_file_or_dir(src: Path, dst: Path):
    """Copy a file or directory to the specified destination."""
    shutil.copytree(src, dst) if src.is_dir() else shutil.copy2(src, dst)


def copy_dir_contents(src: PathLike, dst: PathLike, overwrite: bool = False):
    """Copy the contents of a source directory to a destination directory.

    Parameters
    ----------
    src
        Source directory path.
    dst
        Destination directory path.
    overwrite
        If True, overwrite (by first removing) any items that are already within the
        data cache directory. If False (the default), a `FileExistsError` exception
        will be raised if an item already exists.
    """
    src = Path(src)
    dst = Path(dst)

    dst.mkdir(parents=True, exist_ok=True)

    for item in src.iterdir():
        target = dst / item.name

        if target.exists():
            if not overwrite:
                raise FileExistsError(f"Destination item already exists: {target!r}.")

            # remove before overwriting
            if target.is_dir() and not target.is_symlink():
                shutil.rmtree(target)
            else:
                target.unlink()

        if item.is_dir() and not item.is_symlink():
            shutil.copytree(item, target)
        else:
            shutil.copy2(item, target)


def delete_file_or_dir(path: Path):
    """Delete the specified file or directory."""
    shutil.rmtree(path) if path.is_dir() else path.unlink()
