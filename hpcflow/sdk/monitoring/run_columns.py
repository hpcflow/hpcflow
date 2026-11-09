"""Selectable columns for individual action runs and their execution metadata."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, tzinfo
from typing import TYPE_CHECKING

from rich.text import Text
from rich.filesize import decimal

from .columns import format_datetime

if TYPE_CHECKING:
    from .runs import RunRecord


@dataclass(frozen=True, slots=True)
class RunRenderContext:
    """One timestamp-formatting reference shared by every row."""

    display_timezone: tzinfo | None = None
    current_local_time: datetime | None = None

    def time(self, value: datetime | None) -> str:
        return format_datetime(
            value.isoformat() if value is not None else "",
            display_timezone=self.display_timezone,
            current_local_time=self.current_local_time,
        )


@dataclass(frozen=True, slots=True)
class RunColumn:
    """A run column's heading and rendering function; no workflow reads here."""

    title: str
    render: Callable[[RunRecord, RunRenderContext], str | Text]


def _optional(value: object) -> str:
    return str(value) if value is not None else "—"


RUN_COLUMNS = {
    "run_id": RunColumn("Run ID", lambda r, c: str(r.run_id)),
    "task": RunColumn("Task", lambda r, c: Text(f"{r.task_id}: {r.task_name}")),
    "task_id": RunColumn("Task ID", lambda r, c: str(r.task_id)),
    "task_idx": RunColumn("Task index", lambda r, c: _optional(r.task_idx)),
    "task_name": RunColumn("Task name", lambda r, c: Text(r.task_name)),
    "element": RunColumn("Element", lambda r, c: str(r.element)),
    "iteration": RunColumn("Iteration", lambda r, c: str(r.iteration)),
    "action": RunColumn("Action", lambda r, c: Text(f"{r.action}: {r.action_name}")),
    "action_idx": RunColumn("Action index", lambda r, c: str(r.action)),
    "action_name": RunColumn("Action name", lambda r, c: Text(r.action_name)),
    "run_idx": RunColumn("Attempt", lambda r, c: str(r.attempt)),
    "submission": RunColumn("Submission", lambda r, c: _optional(r.submission)),
    "jobscript": RunColumn("Jobscript", lambda r, c: _optional(r.jobscript)),
    "array_idx": RunColumn("Array index", lambda r, c: _optional(r.array_idx)),
    "state": RunColumn(
        "State",
        lambda r, c: Text(f"{r.state.symbol} {r.state.name}", style=r.state.colour),
    ),
    "exit_code": RunColumn("Exit code", lambda r, c: _optional(r.exit_code)),
    "host": RunColumn("Run host", lambda r, c: Text(r.host or "—")),
    "start": RunColumn("Start", lambda r, c: c.time(r.start)),
    "end": RunColumn("End", lambda r, c: c.time(r.end)),
    "duration": RunColumn(
        "Duration",
        lambda r, c: (
            str(timedelta(seconds=int(r.duration.total_seconds())))
            if r.duration is not None
            else "—"
        ),
    ),
    "end_size": RunColumn(
        "End size",
        lambda r, c: decimal(r.end_size_bytes) if r.end_size_bytes is not None else "—",
    ),
    "end_size_bytes": RunColumn(
        "End size (bytes)", lambda r, c: _optional(r.end_size_bytes)
    ),
}

DEFAULT_RUN_COLUMNS = (
    "run_id",
    "task",
    "element",
    "iteration",
    "action",
    "run_idx",
    "submission",
    "jobscript",
    "state",
    "exit_code",
)

_ALIASES = {
    "id": "run_id",
    "attempt": "run_idx",
    "element_idx": "element",
    "iteration_idx": "iteration",
    "submission_idx": "submission",
    "jobscript_idx": "jobscript",
    "jobscript_array_idx": "array_idx",
    "run_host": "host",
    "run_hostname": "host",
    "start_time": "start",
    "end_time": "end",
}


def run_columns(value: str | Sequence[str] | None) -> tuple[str, ...]:
    """Validate ordered columns, with ``all`` selecting every available column."""
    if value is None:
        return DEFAULT_RUN_COLUMNS
    values = value.split(",") if isinstance(value, str) else value
    if any(not isinstance(name, str) for name in values):
        raise ValueError("Run column names must be strings")
    names = tuple(_ALIASES.get(name.strip(), name.strip()) for name in values)
    if names == ("all",):
        return tuple(RUN_COLUMNS)
    if not names or any(name not in RUN_COLUMNS for name in names):
        raise ValueError(f"Run columns must be selected from: {', '.join(RUN_COLUMNS)}")
    if len(set(names)) != len(names):
        raise ValueError("Run columns must not contain duplicates")
    return names
