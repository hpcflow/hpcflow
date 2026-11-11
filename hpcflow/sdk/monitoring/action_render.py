"""Render aligned, grouped action and element state cells."""

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, replace
from itertools import groupby

from rich.table import Table
from rich.text import Text
from rich.measure import Measurement
from rich.console import Console, ConsoleOptions, RenderResult

from .states import ACTION_STATES
from .models import ActionStatus, ActionSummary, ElementGroup, IterationActionState
from .compact import compact_action_text
from ..log import TimeIt


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
    icons: int = 0


class _DetailTable(Table):
    """Fit shared detail labels to the actual cell width, reserving space for icons.

    Element ranges surrender space first, followed by iteration/task labels on
    narrow terminals. Icons wrap when necessary rather than being truncated.
    Building a fresh grid for each render preserves cached rows and Rich styles.
    """

    def __init__(self, rows: list[FullActionRow], widths: ActionColumnWidths):
        super().__init__(show_header=False)
        self._detail_rows = rows
        if not widths.icons:
            widths = replace(
                widths, icons=max((row.icons.cell_len for row in rows), default=0)
            )
        self._detail_widths = widths

    def __rich_measure__(self, console: Console, options: ConsoleOptions) -> Measurement:
        if not self._detail_rows:
            return Measurement(0, 0)
        widths = self._detail_widths
        maximum = min(
            options.max_width,
            widths.task + widths.elements + widths.iterations + widths.icons + 3,
        )
        minimum = min(
            maximum,
            min(widths.task, 8)
            + min(widths.elements, 3)
            + min(widths.iterations, 5)
            + min(widths.icons, 1)
            + 3,
        )
        return Measurement(minimum, maximum)

    def __rich_console__(self, console: Console, options: ConsoleOptions) -> RenderResult:
        if not self._detail_rows:
            return
        if options.max_width <= 3:
            for row in self._detail_rows:
                yield row.icons
            return
        shared = self._detail_widths
        budget = options.max_width - 3  # Three inter-column spaces.
        labels = [shared.task, shared.elements, shared.iterations]
        floors = [min(labels[0], 8), min(labels[1], 3), min(labels[2], 5)]
        icons = min(shared.icons, max(1, budget - sum(floors)))
        excess = max(0, sum(labels) + icons - budget)
        for index in (1, 2, 0):
            shrink = min(excess, labels[index] - floors[index])
            labels[index] -= shrink
            excess -= shrink
        # At very narrow widths even the minimal labels may need truncation.
        for index in (1, 2, 0):
            shrink = min(excess, labels[index])
            labels[index] -= shrink
            excess -= shrink
        grid = Table.grid(padding=(0, 1))
        for width in labels:
            grid.add_column(width=width, no_wrap=True, overflow="ellipsis")
        grid.add_column(width=icons, overflow="fold")
        for row in self._detail_rows:
            elements = Text(row.elements)
            if elements.cell_len > labels[1] and labels[1] >= 3:
                elements = Text(row.elements[1:-1])
                elements.truncate(labels[1] - 2, overflow="ellipsis")
                elements = Text.assemble("[", elements, "]")
            grid.add_row(Text(row.task), elements, row.iterations, row.icons)
        yield grid


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


@TimeIt.decorator
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
        max((row.icons.cell_len for row in rows), default=0),
    )


def needs_compact_layout(rows: list[FullActionRow]) -> bool:
    """Switch fragmented histories to bounded task blocks before labels hide icons.

    Short workflows retain exact patterns. Long element/iteration labels also
    trigger compaction when there are enough rows to make the layout cumbersome.
    """
    if len(rows) > 40:
        return True
    widths = action_column_widths(rows)
    return len(rows) > 8 and (
        widths.elements > 40
        or widths.iterations > 48
        or widths.task + widths.elements + widths.iterations > 100
    )


@TimeIt.decorator
def display_summary(
    summary: ActionSummary | None,
    include_loop_skipped: bool = False,
    *,
    inactive: bool = False,
    unknown: bool = False,
) -> ActionSummary:
    """Select visible runs and infer interrupted runs for inactive submissions.

    Preserve engine counts, timestamps, and the original snapshot. Inactivity gives
    no exit result or finish time, so only display counts and patterns are changed.
    Unknown activity makes recorded running states unknown rather than interrupted.
    Old scheduler-derived states become unknown for either inactive or unknown
    submissions: an earlier running parent never proved its individual run started.
    """
    if summary is None:
        return ActionSummary()
    summary = (
        summary
        if include_loop_skipped or summary.without_loop_skipped is None
        else summary.without_loop_skipped
    )
    counts = dict(
        summary.display_counts if summary.display_counts is not None else summary.counts
    )
    # Older registry snapshots may contain R observations. Never render them as
    # verified running states, even if the submission is currently active.
    legacy_running = "scheduler_running" in counts or (
        summary.display_counts is None
        and any(
            step.state == "scheduler_running"
            for group in summary.groups
            for step in group.pattern
        )
    )
    if not inactive and not unknown and not legacy_running:
        return summary
    replacements = {"scheduler_running": "unknown"}
    if inactive or unknown:
        replacements.update(
            running="interrupted" if inactive else "unknown", scheduler_pending="unknown"
        )
    for name, replacement in replacements.items():
        if count := counts.pop(name, 0):
            counts[replacement] = counts.get(replacement, 0) + count
    return replace(
        summary,
        display_counts=counts,
        groups=[
            replace(
                group,
                pattern=tuple(
                    (
                        replace(step, state=replacements[step.state])
                        if step.state in replacements
                        else step
                    )
                    for step in group.pattern
                ),
            )
            for group in summary.groups
        ],
        without_loop_skipped=None,
    )


@TimeIt.decorator
def action_text(
    summary: ActionSummary,
    full: bool,
    column_widths: ActionColumnWidths | None = None,
    include_loop_skipped: bool = False,
    group_elements: bool = True,
    group_iterations: bool = True,
    compact: bool | None = None,
) -> Text | Table:
    """Render compact state counts or full grouped action/element patterns.

    An optional shared width descriptor aligns detail icons across submissions.
    Loop-skipped runs are hidden unless explicitly included or no filtered view exists.
    Large full-detail layouts automatically use task summaries unless either grouping
    option is disabled. ``compact`` can supply an already computed layout decision.
    """
    summary = display_summary(summary, include_loop_skipped)
    if full:
        if compact and group_elements and group_iterations:
            return compact_action_text(summary)
        rows = full_action_rows(summary, group_elements, group_iterations)
        if (
            compact is None
            and group_elements
            and group_iterations
            and needs_compact_layout(rows)
        ):
            return compact_action_text(summary)
        widths = column_widths or action_column_widths(rows)
        return _DetailTable(rows, widths)
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
