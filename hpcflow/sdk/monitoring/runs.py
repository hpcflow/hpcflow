"""Inspect individual workflow action runs independently of submission monitoring."""

from __future__ import annotations

from dataclasses import dataclass
from collections.abc import Sequence
from datetime import datetime, timedelta, timezone
import stat
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table, box

from ..core.enums import EARStatus
from ..log import TimeIt
from .run_filters import IndexFilter, RunFilters, index_selection, task_names
from .run_columns import RUN_COLUMNS, RunRenderContext, run_columns
from .timezones import configured_timezone, local_now

if TYPE_CHECKING:
    from ..core.actions import ElementActionRun
    from ..core.workflow import Workflow


@dataclass(frozen=True, slots=True)
class RunRecord:
    """An individual action attempt and its location in the workflow.

    Keep run IDs distinct from per-action attempt indices. Include the action's
    short name, submission/jobscript indices, and program exit code so failed
    attempts can be located. Jobscript indices are local to each submission.
    Task indices refer to current workflow order; array indices are zero-based
    scheduler array items. Times and host describe the recorded execution, while
    running durations are elapsed at the collection instant.
    """

    run_id: int
    task_id: int
    task_name: str
    element: int
    iteration: int
    action: int
    action_name: str
    attempt: int
    submission: int | None
    jobscript: int | None
    state: EARStatus
    exit_code: int | None
    task_idx: int | None = None
    host: str | None = None
    start: datetime | None = None
    end: datetime | None = None
    duration: timedelta | None = None
    array_idx: int | None = None
    end_size_bytes: int | None = None


@TimeIt.decorator
def _end_snapshot_size(run: ElementActionRun) -> int | None:
    """Sum regular-file sizes from the recorded end snapshot, without live I/O.

    Include recorded files in nested directories but exclude directory sizes and
    other non-regular entries. None means no captured snapshot; an empty captured
    snapshot totals zero. Each recorded path contributes its logical byte size.
    """
    snapshot = getattr(run, "snapshot_end", None)
    if snapshot is None:
        return None
    total = 0
    for path in snapshot.paths:
        info = snapshot.stat_info(path)
        if stat.S_ISREG(info.st_mode):
            total += info.st_size
    return total


@dataclass(frozen=True, slots=True)
class _JobscriptLocation:
    """Stored jobscript coordinates; array indices are zero-based and optional."""

    jobscript: int | None
    array_idx: int | None = None


@TimeIt.decorator
def _jobscript_locations(
    workflow: Workflow, submission: int, wanted: set[int], include_array: bool
) -> dict[int, _JobscriptLocation]:
    """Read parent coordinates once per submission, retaining only selected runs.

    Array positions come from block columns, independently of task element indices.
    Non-array jobs have no array index. Never instantiate a scheduler adapter.
    """
    result: dict[int, _JobscriptLocation] = {}
    for job in workflow.submissions[submission].jobscripts:
        if include_array and getattr(job, "is_array", False):
            for block in job.blocks:
                array = block.EAR_ID
                for column in range(array.shape[1]):
                    location = _JobscriptLocation(job.index, column)
                    for value in array[:, column]:
                        run_id = int(value)
                        if run_id in wanted:
                            previous = result.get(run_id)
                            result[run_id] = (
                                _JobscriptLocation(
                                    job.index if previous.jobscript == job.index else None
                                )
                                if previous is not None and previous != location
                                else location
                            )
        else:
            location = _JobscriptLocation(job.index)
            for value in job.all_EAR_IDs:
                run_id = int(value)
                if run_id in wanted:
                    result[run_id] = location
    return result


def _duration(start: datetime | None, end: datetime | None) -> timedelta | None:
    """Subtract instants in UTC, including legacy naive timestamps and DST changes."""
    if start is None or end is None:
        return None
    start = start.replace(tzinfo=start.tzinfo or timezone.utc).astimezone(timezone.utc)
    end = end.replace(tzinfo=end.tzinfo or timezone.utc).astimezone(timezone.utc)
    value = end - start
    return value if value >= timedelta(0) else None


def resolve_run_state(state: EARStatus | str | None) -> EARStatus | None:
    """Accept an engine state or its name, with None selecting all states."""
    if state is None or isinstance(state, EARStatus):
        return state
    if isinstance(state, str):
        try:
            return EARStatus[state.lower()]
        except KeyError:
            pass
    raise ValueError(
        f"Unknown run state {state!r}; choose from: {', '.join(EARStatus.__members__)}"
    )


@TimeIt.decorator
def collect_runs(
    workflow: Workflow,
    state: EARStatus | str | None = None,
    *,
    task_idx: IndexFilter = None,
    task_id: IndexFilter = None,
    task_name: str | Sequence[str] | None = None,
    element_idx: IndexFilter = None,
    iteration_idx: IndexFilter = None,
    action_idx: IndexFilter = None,
    run_id: IndexFilter = None,
    run_idx: IndexFilter = None,
    columns: str | Sequence[str] | None = None,
) -> list[RunRecord]:
    """Read current run states and return matching attempts in run-ID order.

    Include all iterations, submissions, and attempts, including failed attempts
    subsequently retried successfully. Use a shared store-cache context for bulk
    retrieval. No scheduler queries, registry updates, or terminal-cache writes
    are performed. Invalid state names fail before reading the workflow store.
    Coordinates are ANDed; comma-separated alternatives and inclusive ranges are
    accepted within each numeric selection. Task indices refer to workflow order,
    task IDs are stable insertion IDs, and run indices are per-action attempts.
    An explicit run-ID selection loads only those valid IDs. Array positions are
    read only when requested in ``columns`` (or for direct calls without columns).
    Duration uses recorded endpoints, or one current instant for running runs.
    """
    wanted = resolve_run_state(state)
    selected_columns = run_columns(columns)
    filters = RunFilters(
        task_idx=index_selection(task_idx, "Task indices"),
        task_id=index_selection(task_id, "Task IDs"),
        names=task_names(task_name),
        element_idx=index_selection(element_idx, "Element indices"),
        iteration_idx=index_selection(iteration_idx, "Iteration indices"),
        action_idx=index_selection(action_idx, "Action indices"),
        run_id=index_selection(run_id, "Run IDs"),
        run_idx=index_selection(run_idx, "Run indices"),
    )
    include_array = columns is None or "array_idx" in selected_columns
    records = []
    with workflow._store.cached_load(), workflow._store.cache_ctx():
        if filters.run_id is not None:
            ids = list(filters.run_id.within(workflow.num_EARs))
            candidates = workflow.get_EARs_from_IDs(ids) if ids else []
        else:
            candidates = workflow.get_all_EARs()
        matching = []
        submission_ids: dict[int, set[int]] = {}
        for run in candidates:
            if not filters.matches(run):
                continue
            current = run.status
            if wanted is not None and current is not wanted:
                continue
            submission = run.submission_idx
            matching.append((run, current))
            if submission is not None:
                submission_ids.setdefault(submission, set()).add(run.id_)
        jobscripts = (
            {
                submission: _jobscript_locations(workflow, submission, ids, include_array)
                for submission, ids in submission_ids.items()
            }
            if columns is None
            or {"jobscript", "array_idx"}.intersection(selected_columns)
            else {}
        )
        now = datetime.now(timezone.utc)
        include_duration = columns is None or "duration" in selected_columns
        include_end_size = columns is None or bool(
            {"end_size", "end_size_bytes"}.intersection(selected_columns)
        )
        for run, current in matching:
            submission = run.submission_idx
            location = (
                jobscripts.get(submission, {}).get(run.id_)
                if submission is not None
                else None
            )
            start = getattr(run, "start_time", None)
            end = getattr(run, "end_time", None)
            records.append(
                RunRecord(
                    run_id=run.id_,
                    task_id=run.task.insert_ID,
                    task_name=run.task.unique_name,
                    element=run.element.index,
                    iteration=run.element_iteration.index,
                    action=run.element_action.action_idx,
                    action_name=run.action.short_name(),
                    attempt=run.index,
                    submission=submission,
                    jobscript=(location.jobscript if location is not None else None),
                    state=current,
                    exit_code=run.exit_code,
                    task_idx=getattr(run.task, "index", None),
                    host=getattr(run, "run_hostname", None),
                    start=start,
                    end=end,
                    duration=(
                        _duration(
                            start,
                            (
                                end
                                if end is not None
                                else now if current is EARStatus.running else None
                            ),
                        )
                        if include_duration
                        else None
                    ),
                    array_idx=location.array_idx if location is not None else None,
                    end_size_bytes=_end_snapshot_size(run) if include_end_size else None,
                )
            )
    return sorted(records, key=lambda record: record.run_id)


@TimeIt.decorator
def show_runs(
    workflow: Workflow,
    state: EARStatus | str | None = None,
    console: Console | None = None,
    *,
    task_idx: IndexFilter = None,
    task_id: IndexFilter = None,
    task_name: str | Sequence[str] | None = None,
    element_idx: IndexFilter = None,
    iteration_idx: IndexFilter = None,
    action_idx: IndexFilter = None,
    run_id: IndexFilter = None,
    run_idx: IndexFilter = None,
    columns: str | Sequence[str] | None = None,
) -> None:
    """Print individual action runs, optionally restricted to one engine state.

    Coloured state icons accompany text names. Action cells combine the action
    index with its short name. Missing submission/jobscript indices and exit codes
    use an em dash. An empty selection prints a descriptive message.
    Columns preserve the requested order; ``all`` includes execution metadata.
    Optional coordinate filters use the same selectors as ``collect_runs`` and
    combine with the state filter. Start/end columns use the configured show
    timezone, with historical offsets and the usual local-suffix omission rules.
    """
    wanted = resolve_run_state(state)
    selected_columns = run_columns(columns)
    context = (
        RunRenderContext(configured_timezone(workflow._app.config), local_now())
        if {"start", "end"}.intersection(selected_columns)
        else RunRenderContext()
    )
    records = collect_runs(
        workflow,
        wanted,
        task_idx=task_idx,
        task_id=task_id,
        task_name=task_name,
        element_idx=element_idx,
        iteration_idx=iteration_idx,
        action_idx=action_idx,
        run_id=run_id,
        run_idx=run_idx,
        columns=selected_columns,
    )
    console = console or Console()
    if not records:
        coordinates = any(
            value is not None
            for value in (
                task_idx,
                task_id,
                task_name,
                element_idx,
                iteration_idx,
                action_idx,
                run_id,
                run_idx,
            )
        )
        if coordinates:
            console.print(
                "No runs matching the requested coordinates"
                + (f" with state '{wanted.name}'." if wanted else ".")
            )
        else:
            console.print(
                f"No runs with state '{wanted.name}'." if wanted else "No runs."
            )
        return
    table = Table(box=box.SQUARE)
    for name in selected_columns:
        table.add_column(RUN_COLUMNS[name].title)
    for record in records:
        table.add_row(
            *(RUN_COLUMNS[name].render(record, context) for name in selected_columns)
        )
    console.print(table)
