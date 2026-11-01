"""Inspect individual workflow action runs independently of submission monitoring."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table, box
from rich.text import Text

from ..core.enums import EARStatus

if TYPE_CHECKING:
    from ..core.workflow import Workflow


@dataclass(frozen=True, slots=True)
class RunRecord:
    """An individual action attempt and its location in the workflow.

    Keep run IDs distinct from per-action attempt indices. Include the action's
    short name, submission/jobscript indices, and program exit code so failed
    attempts can be located. Jobscript indices are local to each submission.
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


def collect_runs(
    workflow: Workflow, state: EARStatus | str | None = None
) -> list[RunRecord]:
    """Read current run states and return matching attempts in run-ID order.

    Include all iterations, submissions, and attempts, including failed attempts
    subsequently retried successfully. Use a shared store-cache context for bulk
    retrieval. No scheduler queries, registry updates, or terminal-cache writes
    are performed. Invalid state names fail before reading the workflow store.
    """
    wanted = resolve_run_state(state)
    records = []
    jobscripts: dict[int, dict[int, int]] = {}
    with workflow._store.cached_load(), workflow._store.cache_ctx():
        for run in workflow.get_all_EARs():
            current = run.status
            if wanted is not None and current is not wanted:
                continue
            submission = run.submission_idx
            if submission is not None and submission not in jobscripts:
                jobscripts[submission] = {
                    int(run_id): job.index
                    for job in workflow.submissions[submission].jobscripts
                    for run_id in job.all_EAR_IDs
                }
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
                    jobscript=(
                        jobscripts[submission].get(run.id_)
                        if submission is not None
                        else None
                    ),
                    state=current,
                    exit_code=run.exit_code,
                )
            )
    return sorted(records, key=lambda record: record.run_id)


def show_runs(
    workflow: Workflow,
    state: EARStatus | str | None = None,
    console: Console | None = None,
) -> None:
    """Print individual action runs, optionally restricted to one engine state.

    Coloured state icons accompany text names. Action cells combine the action
    index with its short name. Missing submission/jobscript indices and exit codes
    use an em dash. An empty selection prints a descriptive message.
    """
    wanted = resolve_run_state(state)
    records = collect_runs(workflow, wanted)
    console = console or Console()
    if not records:
        console.print(f"No runs with state '{wanted.name}'." if wanted else "No runs.")
        return
    table = Table(box=box.SQUARE)
    for column in (
        "Run ID",
        "Task",
        "Element",
        "Iteration",
        "Action",
        "Attempt",
        "Submission",
        "Jobscript",
        "State",
        "Exit code",
    ):
        table.add_column(column)
    for record in records:
        table.add_row(
            str(record.run_id),
            Text(f"{record.task_id}: {record.task_name}"),
            str(record.element),
            str(record.iteration),
            Text(f"{record.action}: {record.action_name}"),
            str(record.attempt),
            str(record.submission) if record.submission is not None else "—",
            str(record.jobscript) if record.jobscript is not None else "—",
            Text(f"{record.state.symbol} {record.state.name}", style=record.state.colour),
            str(record.exit_code) if record.exit_code is not None else "—",
        )
    console.print(table)
