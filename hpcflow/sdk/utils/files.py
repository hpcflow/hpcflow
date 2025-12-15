"""Utility module with functions to assist with copying, moving and removing files and
directories."""

from __future__ import annotations
from pathlib import Path
import shutil

from hpcflow.sdk.typing import PathLike


def copy_file_or_dir(src: Path, dst: Path):
    """Copy a file or directory to the specified destination."""
    shutil.copytree(src, dst) if src.is_dir() else shutil.copy2(src, dst)


def delete_file_or_dir(path: Path):
    """Delete the specified file or directory."""
    shutil.rmtree(path) if path.is_dir() else path.unlink()
