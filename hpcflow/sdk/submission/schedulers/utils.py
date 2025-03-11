"""
Helper for running a subprocess.
"""

from __future__ import annotations
import subprocess
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence
    from logging import Logger


def run_cmd(
    cmd: str | Sequence[str], logger: Logger | None = None, **kwargs
) -> tuple[str, str]:
    """Execute a command and return stdout, stderr as strings."""
    if logger:
        logger.debug(f"running shell command: {cmd}")
    proc = subprocess.run(
        args=cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs
    )
    stdout = proc.stdout.decode()
    stderr = proc.stderr.decode()
    return stdout, stderr
