"""Monitoring settings, including defaults for existing configuration files."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..config.config import Config


def get_limit(config: Config, name: str, default: int) -> int:
    """Use the default when a setting is absent or explicitly unset.

    Config.get's default argument only handles missing nested keys; missing root
    settings return None. Preserve an explicit zero rather than treating it as absent.
    """
    value = config.get(name)
    if value is None:
        return default
    if type(value) is not int or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value
