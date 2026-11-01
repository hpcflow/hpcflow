"""Incremental terminal action state cache and compact display snapshots."""

from collections import Counter, defaultdict
import json
from pathlib import Path
from typing import TYPE_CHECKING

from ..core.enums import EARStatus
from ..core.skip_reason import SkipReason
from .models import (
    ActionRunState,
    ActionSummary,
    ElementGroup,
    IterationActionState,
    SubmissionRecord,
    TerminalStateCache,
    to_json,
)
from .registry import atomic_json, file_lock, timestamp
from .states import display_state_name

if TYPE_CHECKING:
    from ..core.workflow import Workflow


def cache_path(record: SubmissionRecord) -> Path:
    """Return the submission-scoped tmp/terminal_states.json path without creating it."""
    return (
        Path(record.workflow_path)
        / "artifacts"
        / "submissions"
        / str(record.submission_index)
        / "tmp"
        / "terminal_states.json"
    )


def read_cache(record: SubmissionRecord) -> TerminalStateCache | None:
    """Read valid terminal states for the record's workflow and submission.

    Reject mismatched identities/schema versions and nonterminal entries. Missing,
    unreadable, or malformed cache files return None so monitoring can rebuild them
    from workflow data instead of relying on stale state.
    """
    try:
        data = json.loads(cache_path(record).read_text(encoding="utf-8"))
        if (
            data["schema_version"] != 1
            or data["workflow_id"] != record.workflow_id
            or data["submission_index"] != record.submission_index
            or not isinstance(data["terminal_states"], dict)
        ):
            return None
        states = {}
        valid_reasons = {reason.value for reason in SkipReason}
        for key, entry in data["terminal_states"].items():
            if entry["state"] == "skipped" and (
                type(entry.get("skip_reason")) is not int
                or entry["skip_reason"] not in valid_reasons
            ):
                # Upgrade old caches selectively: successful/error runs stay cached.
                continue
            state = ActionRunState.from_json(entry)
            if state.state not in EARStatus.get_non_running_submitted_states():
                return None
            states[int(key)] = state
        return TerminalStateCache(
            record.workflow_id,
            record.submission_index,
            states,
            data.get("observed_at", ""),
        )
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None


def run_ids(record: SubmissionRecord) -> list[int]:
    """Return the submission part's distinct action-run IDs in first-seen job order."""
    return list(dict.fromkeys(i for job in record.jobs for i in job.run_ids))


def _action_summary(entries: dict[int, ActionRunState], ids: list[int]) -> ActionSummary:
    """Count run states and group elements sharing an identical ordered action pattern."""
    counts: Counter[str] = Counter()
    display_counts: Counter[str] = Counter()
    tasks: defaultdict[int, defaultdict[int, list[ActionRunState]]] = defaultdict(
        lambda: defaultdict(list)
    )
    names = {}
    for run_id in ids:
        entry = entries[run_id]
        counts[entry.state.name] += 1
        display_counts[display_state_name(entry)] += 1
        names[entry.task] = entry.task_name
        tasks[entry.task][entry.element].append(entry)
    groups = []
    for task_index, elements in sorted(tasks.items()):
        patterns: dict[tuple[IterationActionState, ...], list[int]] = {}
        for element_index, actions in sorted(elements.items()):
            pattern = tuple(
                IterationActionState(
                    state.iteration,
                    state.action,
                    state.attempt,
                    display_state_name(state),
                )
                for state in sorted(
                    actions,
                    key=lambda state: (state.iteration, state.action, state.attempt),
                )
            )
            patterns.setdefault(pattern, []).append(element_index)
        for pattern, members in patterns.items():
            groups.append(ElementGroup(names[task_index], pattern, members))
    return ActionSummary(
        counts=dict(counts), display_counts=dict(display_counts), groups=groups
    )


def snapshot(entries: dict[int, ActionRunState], ids: list[int]) -> ActionSummary:
    """Build a compact action summary for the requested run IDs.

    Include observation/start/end times and an alternate view excluding loop skips.
    Only expose a finish time when all requested runs are terminal. Every requested
    ID must be present in ``entries``; the supplied states are not modified.
    """
    result = _action_summary(entries, ids)
    starts = [entries[i].started_at for i in ids if entries[i].started_at]
    ends = [entries[i].finished_at for i in ids if entries[i].finished_at]
    result.observed_at = timestamp()
    result.complete = bool(ids) and all(
        entries[i].state in EARStatus.get_non_running_submitted_states() for i in ids
    )
    result.started_at = min(starts, default="")
    result.finished_at = max(ends, default="") if result.complete else ""
    visible_ids = [
        i
        for i in ids
        if not (
            entries[i].state is EARStatus.skipped
            and entries[i].skip_reason is SkipReason.LOOP_TERMINATION
        )
    ]
    if len(visible_ids) != len(ids):
        result.without_loop_skipped = _action_summary(entries, visible_ids)
    return result


def refresh_actions(
    record: SubmissionRecord, workflow: "Workflow", no_update: bool = False
) -> ActionSummary:
    """Refresh unresolved runs and reuse immutable terminal states from disk.

    Query the workflow store only for IDs absent from the terminal cache. Build the
    submission summary from cached and fresh entries, then merge terminal entries
    under a file lock and write atomically. ``no_update`` performs no cache writes.
    A write failure is reported on the summary without losing collected states;
    store-read failures propagate to the collection layer.
    """
    ids = run_ids(record)
    cache = read_cache(record)
    entries = dict(cache.terminal_states) if cache else {}
    unresolved = [i for i in ids if i not in entries]
    with workflow._store.cache_ctx():
        for run in workflow.get_EARs_from_IDs(unresolved) if unresolved else ():
            start, end = run.start_time, run.end_time
            entries[run.id_] = ActionRunState(
                task=run.task.index,
                task_name=run.task.unique_name,
                element=run.element.index,
                iteration=run.element_iteration.index,
                action=run.element_action.action_idx,
                attempt=run.index,
                state=run.status,
                started_at=start.isoformat() if start else "",
                finished_at=end.isoformat() if end else "",
                skip_reason=run.skip_reason if run.status is EARStatus.skipped else None,
            )
    result = snapshot(entries, ids)
    if not no_update:
        try:
            path = cache_path(record)
            with file_lock(path.with_suffix(".lock")):
                current = read_cache(record)
                terminal_states = dict(current.terminal_states) if current else {}
                terminal_states.update(
                    {
                        key: value
                        for key, value in entries.items()
                        if value.state in EARStatus.get_non_running_submitted_states()
                    }
                )
                atomic_json(
                    path,
                    to_json(
                        TerminalStateCache(
                            record.workflow_id,
                            record.submission_index,
                            terminal_states,
                            result.observed_at,
                        )
                    ),
                )
        except OSError as error:
            result.cache_error = str(error)
    return result
