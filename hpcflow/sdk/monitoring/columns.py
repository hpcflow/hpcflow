"""Composable show columns and ordered configuration of the rendered table."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from typing import TYPE_CHECKING

from rich.console import Group, RenderableType
from rich.text import Text

from .action_render import (
    ActionColumnWidths,
    action_column_widths,
    action_text,
    display_summary,
    full_action_rows,
)
from .elements import element_summary
from .models import ActionSummary, SubmissionRecord
from .timezones import local_now, resolve_timezone

if TYPE_CHECKING:
    from ..config.config import Config

DEFAULT_SHOW_COLUMNS = ("id", "name", "status", "elements")
DEFAULT_LIST_COLUMNS = ("id", "name", "submission", "submit", "status", "path")


@dataclass(slots=True)
class RenderContext:
    """Shared formatting settings and memoized element summaries for one table.

    Capture the current local time once so every datetime cell uses the same
    reference for omitting timezone suffixes. Action and element widths are shared
    across submissions, keeping full-detail status icons aligned.
    """

    full: bool = False
    include_loop_skipped: bool = False
    list_only: bool = False
    action_widths: ActionColumnWidths | None = None
    element_widths: ActionColumnWidths | None = None
    element_summaries: dict[int, ActionSummary] = field(default_factory=dict)
    display_timezone: tzinfo = field(default_factory=resolve_timezone)
    current_local_time: datetime = field(default_factory=lambda: local_now())

    def format_datetime(
        self,
        value: str,
        *,
        same_date_as: str | None = None,
        align_omitted_date: bool = False,
    ) -> str:
        """Format a timestamp using this table's display zone and local-time snapshot."""
        return format_datetime(
            value,
            same_date_as=same_date_as,
            display_timezone=self.display_timezone,
            current_local_time=self.current_local_time,
            align_omitted_date=align_omitted_date,
        )

    def elements(self, record: SubmissionRecord) -> ActionSummary:
        """Return the element summary for a record, computing it at most once.

        The loop-skip visibility policy is applied before aggregating actions across
        iterations. The cache is scoped to this render context and keyed by record identity.
        """
        key = id(record)
        if key not in self.element_summaries:
            self.element_summaries[key] = element_summary(
                display_summary(record.last_action_summary, self.include_loop_skipped)
            )
        return self.element_summaries[key]


@dataclass(frozen=True, slots=True)
class ShowColumn:
    """Describe one selectable column and its rendering requirements.

    ``requires_summary`` tells collection whether action-state data is needed.
    ``list_title`` optionally supplies a different heading for registered history.
    Render callbacks consume snapshots and must not query stores or schedulers.
    """

    name: str
    title: str
    render: Callable[[SubmissionRecord, RenderContext], RenderableType]
    list_title: str | None = None
    requires_summary: bool = False


def _parse_datetime(value: str) -> datetime | None:
    """Parse an ISO timestamp into UTC, treating legacy naive timestamps as UTC.

    Support the trailing Z form on Python 3.10. Return None for invalid input so
    renderers can preserve unrecognized timestamp text instead of failing.
    """
    try:
        # Python 3.10 does not yet accept the ISO UTC Z suffix directly.
        parsed = datetime.fromisoformat(
            value[:-1] + "+00:00" if value.endswith("Z") else value
        )
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, TypeError, OverflowError, AttributeError):
        return None


def format_datetime(
    value: str,
    *,
    same_date_as: str | None = None,
    display_timezone: tzinfo | None = None,
    include_timezone: bool | None = None,
    current_local_time: datetime | None = None,
    align_omitted_date: bool = False,
) -> str:
    """Format a stored timestamp in the chosen display timezone, to seconds.

    Missing values produce an em dash and invalid values are returned unchanged.
    Naive legacy values are interpreted as UTC. Compare dates after timezone
    conversion; ``same_date_as`` omits a matching date and ``align_omitted_date``
    reserves its eleven-character width so clock times still align.

    By default, omit the timezone suffix only when both its abbreviation and UTC
    offset match ``current_local_time`` (or the machine's current local time).
    ``include_timezone`` explicitly overrides this rule. Each timestamp is converted
    independently, preserving historical daylight-saving offsets. Stored values are
    never modified.
    """
    if not value:
        return "—"
    parsed = _parse_datetime(value)
    if parsed is None:
        return str(value)
    zone = display_timezone if display_timezone is not None else resolve_timezone()
    parsed = parsed.astimezone(zone)
    reference = _parse_datetime(same_date_as) if same_date_as else None
    if reference is not None:
        reference = reference.astimezone(zone)
    pattern = "%Y-%m-%d %H:%M:%S"
    if reference is not None and parsed.date() == reference.date():
        pattern = (" " * 11 if align_omitted_date else "") + "%H:%M:%S"
    if include_timezone is None:
        current = current_local_time if current_local_time is not None else local_now()
        include_timezone = (parsed.tzname(), parsed.utcoffset()) != (
            current.tzname(),
            current.utcoffset(),
        )
    if not include_timezone:
        return parsed.strftime(pattern)
    offset = parsed.strftime("%z")
    offset = offset[:3] + ":" + offset[3:5] + (":" + offset[5:] if offset[5:] else "")
    return f"{parsed.strftime(pattern)} {parsed.tzname()} ({offset})"


def status_cell(record: SubmissionRecord, context: RenderContext) -> Text:
    """Render activity and accessibility, adding scheduler or cache failure notes."""
    text = Text(record.activity)
    if context.list_only:
        return text
    if record.accessibility != "readable":
        text.append(f" / {record.accessibility}")
    elif record.scheduler_states:
        text.append(" / " + ", ".join(sorted(set(record.scheduler_states))))
    if record.scheduler_error:
        text.append("\nscheduler unavailable")
    if record.last_action_summary and record.last_action_summary.cache_error:
        text.append("\ncache unavailable")
    return text


def _cached_note(
    cell: RenderableType, record: SubmissionRecord, context: RenderContext
) -> RenderableType:
    """Append the observation time when a cell represents cached or inaccessible data."""
    if record.last_action_summary is not None and (
        context.list_only or record.accessibility != "readable"
    ):
        return Group(
            cell,
            Text(
                "Cached: "
                + context.format_datetime(record.last_action_summary.observed_at),
                style="dim",
            ),
        )
    return cell


def actions_cell(record: SubmissionRecord, context: RenderContext) -> RenderableType:
    """Render action counts or grouped full-detail action patterns for one submission."""
    cell = action_text(
        record.last_action_summary or ActionSummary(),
        context.full,
        context.action_widths,
        include_loop_skipped=context.include_loop_skipped,
    )
    return _cached_note(cell, record, context)


def elements_cell(record: SubmissionRecord, context: RenderContext) -> RenderableType:
    """Render one overall state per element, with a cached-observation note if needed."""
    return _cached_note(
        action_text(context.elements(record), context.full, context.element_widths),
        record,
        context,
    )


def times_cell(record: SubmissionRecord, context: RenderContext) -> Text:
    """Render submit, start, and end timestamps on separately labelled lines.

    Dates matching the submit date in the display timezone are replaced by spaces
    on start/end lines. Each timestamp independently follows the context's timezone
    suffix rule; missing dates and times retain their usual placeholders.
    """
    return Text(
        "\n".join(
            (
                f"submit  {context.format_datetime(record.submitted_at)}",
                f"start   {context.format_datetime(record.started_at, same_date_as=record.submitted_at, align_omitted_date=True)}",
                f"end     {context.format_datetime(record.finished_at, same_date_as=record.submitted_at, align_omitted_date=True)}",
            )
        )
    )


SHOW_COLUMNS = {
    column.name: column
    for column in (
        ShowColumn(
            "id",
            "ID",
            lambda record, context: Text(
                str(record.local_id) if record.local_id >= 0 else "—"
            ),
        ),
        ShowColumn(
            "name",
            "Name",
            lambda record, context: Text(record.workflow_name),
            list_title="Workflow",
        ),
        ShowColumn("status", "Status", status_cell, list_title="Last activity"),
        ShowColumn("actions", "Actions", actions_cell, requires_summary=True),
        ShowColumn("elements", "Elements", elements_cell, requires_summary=True),
        ShowColumn(
            "submit",
            "Submit",
            lambda record, context: Text(context.format_datetime(record.submitted_at)),
            list_title="Submitted",
        ),
        ShowColumn(
            "start",
            "Start",
            lambda record, context: Text(context.format_datetime(record.started_at)),
            requires_summary=True,
        ),
        ShowColumn(
            "end",
            "End",
            lambda record, context: Text(context.format_datetime(record.finished_at)),
            requires_summary=True,
        ),
        ShowColumn("times", "Times", times_cell, requires_summary=True),
        ShowColumn(
            "submission",
            "Submission",
            lambda record, context: Text(str(record.submission_index)),
        ),
        ShowColumn("path", "Path", lambda record, context: Text(record.workflow_path)),
    )
}


def validate_columns(columns: Sequence[str]) -> tuple[str, ...]:
    """Return an ordered tuple of known, unique column names.

    Reject empty selections, strings passed instead of sequences, unknown names,
    and duplicates with ValueError. Hidden columns are simply omitted.
    """
    if isinstance(columns, str) or not columns:
        raise ValueError("Columns must be a non-empty list of column names")
    if any(not isinstance(name, str) or name not in SHOW_COLUMNS for name in columns):
        raise ValueError(f"Unknown show column; choose from: {', '.join(SHOW_COLUMNS)}")
    if len(set(columns)) != len(columns):
        raise ValueError("Show columns must not contain duplicates")
    return tuple(columns)


def configured_columns(
    config: Config, list_only: bool = False, override: Sequence[str] | None = None
) -> tuple[str, ...]:
    """Resolve a per-call column override or the appropriate show configuration.

    Use separate defaults for live monitoring and registry listings. Missing or
    unset settings in older configurations fall back to the default column sets.
    """
    if override is not None:
        return validate_columns(override)
    value = config.get("show_list_columns" if list_only else "show_columns")
    if value is None:
        value = DEFAULT_LIST_COLUMNS if list_only else DEFAULT_SHOW_COLUMNS
    return validate_columns(value)


def prepare_context(
    records: list[SubmissionRecord],
    columns: Sequence[str],
    full: bool,
    include_loop_skipped: bool,
    list_only: bool,
    display_timezone: tzinfo | None = None,
) -> RenderContext:
    """Prepare one table's time reference and widths for selected full-detail columns.

    Only compute action/element layouts when those columns are selected. Rendering
    uses the supplied snapshots without reading workflow stores or scheduler state.
    """
    context = RenderContext(
        full=full,
        include_loop_skipped=include_loop_skipped,
        list_only=list_only,
        display_timezone=(
            display_timezone if display_timezone is not None else resolve_timezone()
        ),
    )
    if full and "actions" in columns:
        context.action_widths = action_column_widths(
            [
                row
                for record in records
                for row in full_action_rows(
                    display_summary(record.last_action_summary, include_loop_skipped)
                )
            ]
        )
    if full and "elements" in columns:
        context.element_widths = action_column_widths(
            [
                row
                for record in records
                for row in full_action_rows(context.elements(record))
            ]
        )
    return context
