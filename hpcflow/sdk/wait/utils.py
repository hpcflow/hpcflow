import os
from pathlib import Path
import shutil
import time
import uuid


def claim_path_for_removal(path: Path, *, attempts: int = 5) -> Path | None:
    """Claim a path for removal.

    Atomically rename the directory to a uniquely named cleanup path so that only one
    process removes its contents. Concurrent processes may attempt to consolidate the
    same completion node, so a failed claim does not imply that cleanup is required.

    The rename is retried on ``PermissionError`` to accommodate transient filesystem
    contention, particularly on Windows. If the directory no longer exists, another
    process is assumed to have already claimed or removed it.

    Parameters
    ----------
    children_path
        Directory containing completion state that has been superseded by a
        parent completion marker.
    attempts
        Maximum number of attempts to claim the directory.

    Returns
    -------
    Path or None
        The renamed cleanup path if this process successfully claims the
        directory, otherwise ``None``.
    """
    cleanup_path = path.with_name(f"{path.name}.{uuid.uuid4().hex}.cleanup")

    for attempt in range(attempts):
        try:
            os.replace(path, cleanup_path)
        except FileNotFoundError:
            return None
        except PermissionError:
            if attempt == attempts - 1:
                return None
            time.sleep(0.01 * 2**attempt)
        else:
            return cleanup_path

    return None


def remove_superseded_directory(path: Path) -> None:
    """Atomically claim and best-effort remove a superseded path.

    Attempt to claim a directory before recursively removing it. The claim prevents
    multiple processes that concurrently consolidate the same node from recursively
    deleting the same directory.

    Removal is best-effort because the parent completion marker is already
    authoritative when this method is called. Failure to remove redundant child state
    therefore does not affect completion correctness.

    Parameters
    ----------
    path
        Directory containing redundant child completion state.
    """
    cleanup_path = claim_path_for_removal(path)

    if cleanup_path is None:
        return

    try:
        if cleanup_path.is_dir():
            shutil.rmtree(cleanup_path)
        else:
            cleanup_path.unlink()
    except OSError:
        pass
