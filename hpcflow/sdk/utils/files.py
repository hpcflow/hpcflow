"""Utility module with functions to assist with file and directory IO."""

from __future__ import annotations
from pathlib import Path
import shutil
import os
from logging import Logger
import requests
import io
import zipfile

from ..core.utils import write_YAML_file


def copy_file_or_dir(src: Path, dst: Path):
    """Copy a file or directory to the specified destination."""
    shutil.copytree(src, dst) if src.is_dir() else shutil.copy2(src, dst)


def delete_file_or_dir(path: Path):
    """Delete the specified file or directory."""
    shutil.rmtree(path) if path.is_dir() else path.unlink()


def overwrite_YAML_file(
    path: Path,
    new_contents,
    description: str = "",
    logger: Logger | None = None,
    typ: str = "safe",
):
    """
    Update the contents of the specified YAML file as atomically as possible.
    """
    if description and not description.endswith(" "):
        description += " "

    # write a new temporary config file
    tmp_file = path.with_suffix(path.suffix + ".tmp")
    if logger:
        logger.debug(f"Creating temporary {description}file: {tmp_file!r}.")
    write_YAML_file(new_contents, tmp_file, typ=typ)

    # atomic rename, overwriting original:
    if logger:
        logger.debug(f"Replacing original {description}file with temporary file.")
    os.replace(src=tmp_file, dst=path)


def download_github_repo(org: str, repo: str, sha: str, local_path: str | Path = "."):
    """Download a GitHub repository to the specified directory.

    Note the contents of the repo will be downloaded within a top-level directory named
    like `<repo>-<sha>` (within the `local_path` directory).

    """
    local_path = Path(local_path)
    assert local_path.is_dir()
    url = f"https://github.com/{org}/{repo}/archive/{sha}.zip"
    r = requests.get(url)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(local_path)
