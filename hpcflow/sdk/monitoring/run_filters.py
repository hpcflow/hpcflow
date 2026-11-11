"""Coordinate selections for individual runs, without expanding large ranges."""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
import re
from typing import TYPE_CHECKING, TypeAlias

if TYPE_CHECKING:
    from ..core.actions import ElementActionRun

IndexFilter: TypeAlias = int | str | Sequence[int] | None


@dataclass(frozen=True, slots=True)
class IndexSelection:
    """Sorted, disjoint inclusive ranges of non-negative indices."""

    ranges: tuple[tuple[int, int], ...]

    def __contains__(self, value: int) -> bool:
        return any(start <= value <= end for start, end in self.ranges)

    def within(self, stop: int) -> Iterator[int]:
        """Yield selected IDs inside a workflow's valid half-open ID bounds."""
        for start, end in self.ranges:
            yield from range(start, min(end + 1, stop))


def index_selection(value: IndexFilter, label: str) -> IndexSelection | None:
    """Accept one index, integer sequences, or comma-separated indices/ranges.

    Ranges are inclusive, for example ``0,2-5``. Merge overlaps rather than
    expanding ranges, so even a very large requested upper bound stays cheap.
    Invalid, empty, negative, or reversed selections fail before store reads.
    """
    if value is None:
        return None
    ranges = []
    if isinstance(value, str):
        for part in value.split(","):
            match = re.fullmatch(r"\s*(\d+)(?:\s*[-–]\s*(\d+))?\s*", part)
            if match is None:
                raise ValueError(
                    f"{label} must contain non-negative indices or inclusive ranges, e.g. 0,2-5"
                )
            start = int(match[1])
            end = int(match[2]) if match[2] is not None else start
            if end < start:
                raise ValueError(f"{label} range end must not precede its start")
            ranges.append((start, end))
    else:
        values = [value] if type(value) is int else value
        if (
            not isinstance(values, Sequence)
            or not values
            or any(type(index) is not int or index < 0 for index in values)
        ):
            raise ValueError(f"{label} must contain non-negative integer indices")
        ranges = [(index, index) for index in values]
    merged: list[tuple[int, int]] = []
    for start, end in sorted(ranges):
        if merged and start <= merged[-1][1] + 1:
            merged[-1] = merged[-1][0], max(end, merged[-1][1])
        else:
            merged.append((start, end))
    return IndexSelection(tuple(merged))


def task_names(value: str | Sequence[str] | None) -> frozenset[str] | None:
    """Normalize exact task unique names; None leaves tasks unrestricted."""
    if value is None:
        return None
    values = value.split(",") if isinstance(value, str) else value
    if not values or any(
        not isinstance(name, str) or not name.strip() for name in values
    ):
        raise ValueError("Task names must be non-empty names separated by commas")
    return frozenset(name.strip() for name in values)


@dataclass(frozen=True, slots=True)
class RunFilters:
    """AND coordinates together, with alternatives within each selection."""

    task_idx: IndexSelection | None = None
    task_id: IndexSelection | None = None
    names: frozenset[str] | None = None
    element_idx: IndexSelection | None = None
    iteration_idx: IndexSelection | None = None
    action_idx: IndexSelection | None = None
    run_id: IndexSelection | None = None
    run_idx: IndexSelection | None = None

    def matches(self, run: ElementActionRun) -> bool:
        """Check coordinates before reading the run's state or execution metadata."""
        return (
            (self.run_id is None or run.id_ in self.run_id)
            and (self.run_idx is None or run.index in self.run_idx)
            and (self.task_idx is None or run.task.index in self.task_idx)
            and (self.task_id is None or run.task.insert_ID in self.task_id)
            and (self.names is None or run.task.unique_name in self.names)
            and (self.element_idx is None or run.element.index in self.element_idx)
            and (
                self.iteration_idx is None
                or run.element_iteration.index in self.iteration_idx
            )
            and (
                self.action_idx is None
                or run.element_action.action_idx in self.action_idx
            )
        )
