"""Resolve display timezones without changing persisted UTC timestamps."""

from __future__ import annotations

from datetime import datetime, timezone, tzinfo
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from tzlocal import get_localzone

if TYPE_CHECKING:
    from ..config.config import Config


def resolve_timezone(name: str = "local") -> tzinfo:
    """Resolve local, UTC, or an explicit IANA timezone with historical offset rules.

    Use the displaying machine's zone for local. Invalid names raise ValueError
    with configuration guidance. Returned zones can convert each timestamp
    independently across daylight-saving transitions.
    """
    if not isinstance(name, str) or not name.strip():
        raise ValueError("show_timezone must be 'local', 'UTC', or an IANA timezone name")
    if name == "local":
        return get_localzone()
    try:
        return ZoneInfo(name)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise ValueError(
            f"Unknown show_timezone {name!r}; use 'local', 'UTC', or an IANA timezone "
            "name such as 'Europe/London'"
        ) from exc


def configured_timezone(config: Config) -> tzinfo:
    """Resolve show_timezone, defaulting missing or unset settings to local."""
    name = config.get("show_timezone")
    return resolve_timezone("local" if name is None else name)


def local_now() -> datetime:
    """Capture the current instant in the machine's local timezone.

    Use its abbreviation and offset as the reference for omitting display suffixes,
    independently of any configured display-zone override.
    """
    return datetime.now(timezone.utc).astimezone(resolve_timezone())
