"""Render aligned, grouped action and element state cells."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from itertools import groupby

from rich.table import Table
from rich.text import Text

from .states import ACTION_STATES
from .models import ActionStatus, ActionSummary, ElementGroup, IterationActionState


def element_ranges(elements: list[int]) -> str:
    """Format sorted, deduplicated element indices as inclusive square-bracket ranges."""
    parts = []
    start = end = None
    for element in sorted(set(elements)):
        if end is not None and element == end + 1:
            end = element
            continue
        if start is not None:
            parts.append(str(start) if start == end else f"{start}–{end}")
        start = end = element
    if start is not None:
        parts.append(str(start) if start == end else f"{start}–{end}")
    return "[" + ",".join(parts) + "]"


@dataclass(frozen=True, slots=True)
class IterationRange:
    """An inclusive consecutive iteration range sharing one action-status pattern."""

    start: int
    end: int
    actions: tuple[ActionStatus, ...]

    @property
    def label(self) -> str:
        """Return a single index or an inclusive range without enclosing braces."""
        return (
            str(self.start) if self.start == self.end else f"{self.start}\u2013{self.end}"
        )


@dataclass(slots=True)
class FullActionRow:
    """The task, element ranges, iteration ranges, and icons for one aligned detail row."""

    task: str
    elements: str
    iterations: Text
    icons: Text


@dataclass(frozen=True, slots=True)
class ActionColumnWidths:
    """Rich display-cell widths shared across tasks and submissions in a detail table."""

    task: int
    elements: int
    iterations: int


def iteration_runs(pattern: Sequence[IterationActionState]) -> Iterator[IterationRange]:
    """Yield consecutive iteration ranges with identical ordered action statuses.

    The input must be ordered by iteration and action. Equal patterns separated by
    a gap remain separate ranges rather than implying that absent iterations exist.
    """
    if not pattern:
        return
    start = end = pattern[0].iteration
    previous: tuple[ActionStatus, ...] = ()
    for iteration, rows in groupby(pattern, key=lambda row: row.iteration):
        actions = tuple(row.status for row in rows)
        if previous and actions == previous and iteration == end + 1:
            end = iteration
            continue
        if previous:
            yield IterationRange(start, end, previous)
        start = end = iteration
        previous = actions
    yield IterationRange(start, end, previous)


def state_icons(actions: Sequence[ActionStatus]) -> Text:
    """Render ordered action-state symbols using their configured Rich colours."""
    text = Text()
    for action in actions:
        status = ACTION_STATES[action.state]
        text.append(status.symbol, style=status.colour)
    return text


def iteration_text(pattern: Sequence[IterationActionState]) -> Text:
    """Render consecutive iteration patterns with italic labels and curly-bracket ranges."""
    text = Text()
    labelled = bool(pattern) and (
        pattern[0].iteration != pattern[-1].iteration or pattern[0].iteration != 0
    )
    if labelled:
        text.append("iters", style="italic")
        text.append(" ")
    for index, iteration_range in enumerate(iteration_runs(pattern)):
        if index:
            text.append(" | ")
        if labelled:
            text.append(f"{{{iteration_range.label}}}: ")
        text.append(state_icons(iteration_range.actions))
    return text


def full_action_rows(
    summary: ActionSummary, group_elements: bool = True, group_iterations: bool = True
) -> list[FullActionRow]:
    """Group matching iteration patterns into detail rows for each element group.

    Combine nonconsecutive equal patterns into comma-separated iteration ranges.
    Show the task and element labels only on the first row of each relevant group;
    keep icons separate so all tasks can share alignment widths. Disable element
    and iteration grouping independently to render individual indices without
    changing the collected summary.
    """
    rows = []
    previous_task = None
    groups = (
        summary.groups
        if group_elements
        else [
            ElementGroup(group.task, group.pattern, [element])
            for group in summary.groups
            for element in sorted(group.elements)
        ]
    )
    for group in groups:
        pattern = group.pattern
        labelled = bool(pattern) and (
            pattern[0].iteration != pattern[-1].iteration or pattern[0].iteration != 0
        )
        task = group.task if group.task != previous_task else ""
        elements = element_ranges(group.elements)
        ranges_by_actions: dict[tuple[ActionStatus, ...], list[str]] = {}
        if group_iterations:
            for iteration_range in iteration_runs(pattern):
                ranges_by_actions.setdefault(iteration_range.actions, []).append(
                    iteration_range.label
                )
            patterns = list(ranges_by_actions.items())
        else:
            patterns = [
                (tuple(step.status for step in steps), [str(iteration)])
                for iteration, steps in groupby(pattern, key=lambda step: step.iteration)
            ]
        for index, (actions, ranges) in enumerate(patterns):
            iterations = Text()
            if labelled:
                iterations.append("iters", style="italic")
                iterations.append(f" {{{','.join(ranges)}}}")
            rows.append(
                FullActionRow(
                    task if index == 0 else "",
                    elements if index == 0 else "",
                    iterations,
                    state_icons(actions),
                )
            )
        previous_task = group.task
    return rows


def action_column_widths(rows: list[FullActionRow]) -> ActionColumnWidths:
    """Measure maximum Rich cell widths for task, element, and iteration labels."""
    return ActionColumnWidths(
        max((Text(row.task).cell_len for row in rows), default=0),
        max((Text(row.elements).cell_len for row in rows), default=0),
        max((row.iterations.cell_len for row in rows), default=0),
    )


def display_summary(
    summary: ActionSummary | None, include_loop_skipped: bool = False
) -> ActionSummary:
    """Select the loop-skip visibility view, returning an empty summary if none exists."""
    if summary is None:
        return ActionSummary()
    return (
        summary
        if include_loop_skipped or summary.without_loop_skipped is None
        else summary.without_loop_skipped
    )


def action_text(
    summary: ActionSummary,
    full: bool,
    column_widths: ActionColumnWidths | None = None,
    include_loop_skipped: bool = False,
    group_elements: bool = True,
    group_iterations: bool = True,
) -> Text | Table:
    """Render compact state counts or full grouped action/element patterns.

    An optional shared width descriptor aligns detail icons across submissions.
    Loop-skipped runs are hidden unless explicitly included or no filtered view exists.
    """
    summary = display_summary(summary, include_loop_skipped)
    if full:
        rows = full_action_rows(summary, group_elements, group_iterations)
        widths = column_widths or action_column_widths(rows)
        table = Table.grid(padding=(0, 1))
        table.add_column(width=widths.task)
        table.add_column(width=widths.elements)
        table.add_column(width=widths.iterations)
        table.add_column()
        for row in rows:
            table.add_row(Text(row.task), Text(row.elements), row.iterations, row.icons)
        return table
    text = Text()
    counts = (
        summary.display_counts if summary.display_counts is not None else summary.counts
    )
    for name in sorted(counts, key=lambda name: ACTION_STATES[name].order):
        status = ACTION_STATES[name]
        if text:
            text.append(" | ")
        text.append(f"{status.symbol}:{counts[name]}", style=status.colour)
    return text
