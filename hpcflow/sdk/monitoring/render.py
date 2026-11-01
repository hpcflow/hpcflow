"""Compose monitoring tables from independently rendered, configurable columns."""

from collections.abc import Sequence
from datetime import tzinfo

from rich.console import Console
from rich.table import Table, box

# Retain the existing imports for callers using the action rendering helpers.
from .action_render import (
    ActionColumnWidths as ActionColumnWidths,
    FullActionRow as FullActionRow,
    IterationRange as IterationRange,
    action_column_widths as action_column_widths,
    action_text as action_text,
    display_summary as display_summary,
    element_ranges as element_ranges,
    full_action_rows as full_action_rows,
    iteration_runs as iteration_runs,
    iteration_text as iteration_text,
    state_icons as state_icons,
)
from .columns import (
    DEFAULT_LIST_COLUMNS,
    DEFAULT_SHOW_COLUMNS,
    SHOW_COLUMNS,
    prepare_context,
    validate_columns,
)
from .models import SubmissionRecord


def _render_table(
    records: list[SubmissionRecord],
    columns: Sequence[str],
    full: bool,
    console: Console | None,
    include_loop_skipped: bool,
    list_only: bool,
    display_timezone: tzinfo | None,
) -> None:
    """Compose selected column renderers using a single shared formatting context."""
    names = validate_columns(columns)
    console = console or Console()
    if not records:
        console.print(
            "No registered submissions." if list_only else "No matching submissions."
        )
        return
    context = prepare_context(
        records, names, full, include_loop_skipped, list_only, display_timezone
    )
    selected = [SHOW_COLUMNS[name] for name in names]
    table = Table(box=box.SQUARE)
    for column in selected:
        table.add_column(
            column.list_title if list_only and column.list_title else column.title
        )
    for record in records:
        table.add_row(*(column.render(record, context) for column in selected))
    console.print(table)


def render(
    records: list[SubmissionRecord],
    full: bool = False,
    console: Console | None = None,
    include_loop_skipped: bool = False,
    columns: Sequence[str] | None = None,
    display_timezone: tzinfo | None = None,
) -> None:
    """Print a table of collected submission snapshots to a Rich console.

    ``columns`` selects ordered column identifiers; None uses the live-show defaults.
    ``full`` expands grouped patterns while sharing alignment widths across records.
    ``display_timezone`` defaults to the machine's local zone. This function performs
    no workflow/scheduler reads and writes no monitoring data.
    """
    _render_table(
        records,
        DEFAULT_SHOW_COLUMNS if columns is None else columns,
        full,
        console,
        include_loop_skipped,
        False,
        display_timezone,
    )


def render_registered(
    records: list[SubmissionRecord],
    console: Console | None = None,
    columns: Sequence[str] | None = None,
    full: bool = False,
    include_loop_skipped: bool = False,
    display_timezone: tzinfo | None = None,
) -> None:
    """Print registered history using cached observations and listing headings.

    Use listing defaults when columns is None. Support the same full-detail, loop-skip,
    and timezone formatting as live monitoring without refreshing any data.
    """
    _render_table(
        records,
        DEFAULT_LIST_COLUMNS if columns is None else columns,
        full,
        console,
        include_loop_skipped,
        True,
        display_timezone,
    )
