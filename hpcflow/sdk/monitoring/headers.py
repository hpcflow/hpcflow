"""Read small monitoring files from directories or read-only workflow archives."""

import json
from pathlib import Path
from zipfile import BadZipFile, ZipFile

from .models import WorkflowHeaderToken
from ..log import TimeIt


def is_archive(path: str) -> bool:
    """Identify a local ZIP workflow without opening or extracting it."""
    return Path(path).suffix.lower() == ".zip"


@TimeIt.decorator
def read_bytes(path: str, member: str) -> bytes:
    """Read a binary workflow-relative file, preserving ZIP archives unchanged."""
    if is_archive(path):
        try:
            with ZipFile(path) as archive:
                return archive.read(member)
        except KeyError as error:
            raise FileNotFoundError(member) from error
        except BadZipFile as error:
            raise ValueError(f"Invalid workflow archive: {path}") from error
    return (Path(path) / member).read_bytes()


@TimeIt.decorator
def read_text(path: str, member: str) -> str:
    """Decode a workflow-relative UTF-8 file without modifying its source."""
    return read_bytes(path, member).decode("utf-8")


@TimeIt.decorator
def read_header(
    path: str, previous: WorkflowHeaderToken | None = None
) -> tuple[dict | None, WorkflowHeaderToken]:
    """Read a supported store header, skipping JSON parsing for an unchanged token.

    ZIP tokens describe the archive file, so replacing any member invalidates the
    token. Never extract files or require write access to the workflow.
    """
    if not Path(path).exists():
        raise FileNotFoundError(path)
    for member in (".zattrs", "metadata.json"):
        source = Path(path) if is_archive(path) else Path(path) / member
        try:
            stat = source.stat()
            token = WorkflowHeaderToken(
                member,
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            )
            if token == previous:
                return None, token
            return json.loads(read_text(path, member)), token
        except FileNotFoundError:
            continue
    raise ValueError("Unsupported workflow format or missing workflow metadata")
