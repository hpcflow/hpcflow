"""Utility module with functions to assist with file and directory IO."""

from __future__ import annotations
from pathlib import Path
import shutil
import os
from logging import Logger
import requests
import io
import zipfile
import tempfile
import subprocess
import json
from typing import TYPE_CHECKING

from ..core.utils import write_YAML_file

if TYPE_CHECKING:
    from collections.abc import Callable


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
    tmp_file_callback: Callable | None = None,
):
    """
    Update the contents of the specified YAML file as atomically as possible. If the file
    does not exist, it will be created.

    Parameters
    ----------
    path: Path
        The file path at which to create or overwrite the file.
    new_contents: dict
        JSON-like contents to write to the file.
    description: str
        Description of the file to be written used when logging.
    logger: Logger
        Logger to log to.
    typ: str
        YAML typ; e.g. "safe" or "rt" (round-trip).
    tmp_file_callback: Callable
        Callable to pass the temporary file to before moving the temporary file to its
        final location at `path`. This can be used to change the file permissions,
        for example.

    """
    if description and not description.endswith(" "):
        description += " "

    # write a new temporary config file
    tmp_file = path.with_suffix(path.suffix + ".tmp")
    if logger:
        logger.debug(f"Creating temporary {description}file: {tmp_file!r}.")
    write_YAML_file(new_contents, tmp_file, typ=typ)

    if tmp_file_callback:
        try:
            tmp_file_callback(tmp_file)
        except:
            if logger:
                logger.error(
                    "Exception raised when running tmp_file_callback; deleting temporary "
                    "file."
                )
            tmp_file.unlink()
            raise

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


def set_file_permissions_600(path: str | Path):
    """Set the provide file path to permissions 600, or the best equivalent on Windows."""
    path_s = str(path)
    if os.name != "nt":
        os.chmod(path_s, 0o600)
    else:
        # Windows: remove inheritance + grant only current user
        username = os.getlogin()
        subprocess.run(["icacls", path_s, "/inheritance:r"], check=True)
        subprocess.run(["icacls", path_s, "/grant:r", f"{username}:F"], check=True)
