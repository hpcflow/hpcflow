"""
Miscellaneous persistence-related helpers.
"""

from __future__ import annotations
from getpass import getpass
import os
from pathlib import Path
from typing import TYPE_CHECKING
import uuid

from hpcflow.sdk.core.errors import WorkflowNotFoundError

if TYPE_CHECKING:
    from typing import Callable, TypeVar
    from fsspec import AbstractFileSystem  # type: ignore

    T = TypeVar("T")


def ask_pw_on_auth_exc(
    f: Callable[..., T], *args, add_pw_to: str | None = None, **kwargs
) -> tuple[T, str | None]:
    """
    Run the given function on the given arguments and add a password if the function
    fails with an SSHException.
    """
    from paramiko.ssh_exception import SSHException

    try:
        out = f(*args, **kwargs)
        pw = None

    except SSHException:
        pw = getpass()

        if not add_pw_to:
            kwargs["password"] = pw
        else:
            kwargs[add_pw_to] = {**kwargs[add_pw_to], "password": pw}

        out = f(*args, **kwargs)

    return out, pw


def infer_store(path: str, fs: AbstractFileSystem) -> str:
    """Identify the store type using the path and file system parsed by fsspec.

    Parameters
    ----------
    fs
        fsspec file system

    """

    # TODO: raise WorkflowNotFoundError if the path does not exist
    # TODO: raise MalformedWorkflowError if a known store type cannot be inferred

    # try to identify store type just from the path string:
    if path.endswith(".zip"):
        store_fmt = "zip"

    elif path.endswith(".json"):
        store_fmt = "json-single"

    else:
        # look at the directory contents:
        if fs.glob(f"{path}/.zattrs"):
            store_fmt = "zarr"
        elif fs.glob(f"{path}/metadata.json"):
            store_fmt = "json"
        else:
            raise WorkflowNotFoundError(path, fs)

    return store_fmt


def atomic_write(path: Path, data: bytes) -> None:
    """
    Defensively write the provided bytes to the specified path.

    This includes calling fsync on the file and its parent directory. On the Lustre file
    system for example, it is recommended to call flush (see
    https://wiki.lustre.org/Lustre_Common_Mistakes#Not_Checking_Write_Return_Codes).

    """
    tmp_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.partial")
    try:
        with open(tmp_path, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())

        os.replace(tmp_path, path)

        if hasattr(os, "O_DIRECTORY"):
            # not on Windows
            dir_fd = os.open(path.parent, os.O_DIRECTORY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
