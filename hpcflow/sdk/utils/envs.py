"""Utilities used in app environment management."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING


if TYPE_CHECKING:
    from typing import Literal


def norm_env_setup(setup: str | list[str] | None = None) -> list[str]:
    """Normalise the Environment `setup` argument."""
    if setup is None:
        return []
    if isinstance(setup, str):
        return [setup]
    return setup


def get_env_py_exe(shell: Literal["bash", "powershell"]) -> str:
    """Get the Python executable invocation command for the specified shell."""
    return {
        "bash": sys.executable,
        "powershell": f"& '{sys.executable}'",
    }[shell]
