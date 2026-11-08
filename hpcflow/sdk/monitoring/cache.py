"""Incremental terminal action state cache and compact display snapshots."""

from collections import Counter, defaultdict
from dataclasses import dataclass, replace
import json
from pathlib import Path
from typing import TYPE_CHECKING

import zstandard as zstd

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
from .registry import atomic_bytes, file_lock, timestamp
from .headers import read_bytes, is_archive
from .states import display_state_name
from .run_index import (
    RunIndex,
    RunMetadata,
    parent_elements,
    read_index,
    scheduler_entries,
    write_index,
)
from ..log import TimeIt

if TYPE_CHECKING:
    from ..core.workflow import Workflow


def cache_path(record: SubmissionRecord) -> Path:
    """Return the submission-scoped tmp/terminal_states.json.zst path without creating it."""
    return (
        Path(record.workflow_path)
        / "artifacts"
        / "submissions"
        / str(record.submission_index)
        / "tmp"
        / "terminal_states.json.zst"
    )


@TimeIt.decorator
def read_cache(record: SubmissionRecord) -> TerminalStateCache | None:
    """Read valid terminal states for the record's workflow and submission.

    Prefer zstd-compressed JSON, falling back to legacy plain JSON, including
    embedded ZIP members. Reject mismatched identities/schema versions and
    nonterminal entries. Missing, unreadable, or malformed caches return None so
    monitoring can rebuild them. Reading alone never migrates or writes a cache.
    """
    for suffix in ("json.zst", "json"):
        try:
            payload = read_bytes(
                record.workflow_path,
                f"artifacts/submissions/{record.submission_index}/tmp/terminal_states.{suffix}",
            )
            if suffix.endswith(".zst"):
                payload = zstd.ZstdDecompressor().decompress(payload)
            cache = _decode_cache(record, json.loads(payload))
            if cache is not None:
                return cache
        except (OSError, ValueError, KeyError, TypeError, AttributeError, zstd.ZstdError):
            continue
    return None


@TimeIt.decorator
def _decode_cache(record: SubmissionRecord, data: dict) -> TerminalStateCache | None:
    """Validate a decoded cache independently of its compression or file container."""
    try:
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


@TimeIt.decorator
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


@TimeIt.decorator
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


@dataclass(slots=True)
class StateSelection:
    """Cached/derived observations and the remaining IDs that need store reads."""

    entries: dict[int, ActionRunState]
    unresolved: list[int]
    index: RunIndex


@TimeIt.decorator
def select_states(
    record: SubmissionRecord, cache: TerminalStateCache | None
) -> StateSelection:
    """Combine terminal states and fresh queued-parent evidence before opening a workflow."""
    ids = run_ids(record)
    wanted = set(ids)
    entries = (
        {key: value for key, value in cache.terminal_states.items() if key in wanted}
        if cache
        else {}
    )
    index = read_index(record)
    unresolved = [run_id for run_id in ids if run_id not in entries]
    entries.update(scheduler_entries(record, index, unresolved))
    return StateSelection(
        entries, [run_id for run_id in unresolved if run_id not in entries], index
    )


@TimeIt.decorator
def refresh_actions(
    record: SubmissionRecord,
    workflow: "Workflow",
    no_update: bool = False,
    selection: StateSelection | None = None,
) -> ActionSummary:
    """Refresh unresolved runs and reuse immutable terminal states from disk.

    Query only runs absent from the terminal cache and not covered by fresh queued
    parent observations or an execution cursor proving they are still in the future.
    Bootstrap the parent index once using
    jobscript block arrays and the runs read here. Merge newly terminal states under
    a lock and write atomically. ``no_update`` performs no cache or index writes.
    A write failure is reported on the summary without losing collected states;
    store-read failures propagate to the collection layer.
    Fresh scheduler evidence for an inactive parent marks recorded running runs as
    interrupted, even while other jobs keep the submission active. This display
    inference does not alter engine states or enter the terminal cache.
    """
    ids = run_ids(record)
    selection = selection or select_states(record, read_cache(record))
    entries = selection.entries
    unresolved = selection.unresolved
    new_metadata = {}
    jobs = {job.jobscript_index: job for job in record.jobs}
    with workflow._store.cache_ctx():
        expected_jobs = {
            run_id: job.jobscript_index for job in record.jobs for run_id in job.run_ids
        }
        missing_metadata = [
            run_id
            for run_id in ids
            if run_id not in selection.index.runs
            or selection.index.runs[run_id].jobscript_index != expected_jobs[run_id]
        ]
        parents = parent_elements(record, workflow) if missing_metadata else {}
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
        for run_id, state in entries.items():
            parent = parents.get(run_id)
            if parent is None and run_id in selection.index.runs:
                existing = selection.index.runs[run_id]
                if existing.jobscript_index == expected_jobs[run_id]:
                    parent = existing.jobscript_index, existing.jobscript_element
            if parent is not None and state.display_state is None:
                metadata = RunMetadata.from_state(state, *parent)
                if selection.index.runs.get(run_id) != metadata:
                    new_metadata[run_id] = metadata
            if parent is not None and state.state is EARStatus.running:
                job = jobs[parent[0]]
                observation = job.observation
                if observation is not None:
                    parent_state = observation.state or observation.elements.get(
                        parent[1]
                    )
                    if parent_state in (None, "finished", "cancelled", "errored"):
                        entries[run_id] = replace(state, display_state="interrupted")
    result = snapshot(entries, ids)
    if not no_update and not is_archive(record.workflow_path):
        try:
            write_index(record, new_metadata)
        except (OSError, zstd.ZstdError) as error:
            result.cache_error = str(error)
        terminal = {
            key: value
            for key, value in entries.items()
            if value.state in EARStatus.get_non_running_submitted_states()
        }
        try:
            # Do not rewrite a large terminal cache just because active runs were
            # inspected again. Still migrate a legacy plain cache when necessary.
            if any(run_id in terminal for run_id in unresolved) or (
                terminal and not cache_path(record).exists()
            ):
                write_cache(
                    record,
                    TerminalStateCache(
                        record.workflow_id,
                        record.submission_index,
                        terminal,
                        result.observed_at,
                    ),
                )
        except (OSError, zstd.ZstdError) as error:
            result.cache_error = str(error)
    return result


@TimeIt.decorator
def write_cache(record: SubmissionRecord, cache: TerminalStateCache) -> None:
    """Merge immutable states and atomically write zstd-compressed JSON.

    Keep the historical lock name, merge concurrent observations under that lock,
    and remove the redundant legacy JSON only after a successful compressed write.
    ZIP workflows remain read-only and can reuse either embedded cache format.
    """
    if is_archive(record.workflow_path):
        return
    path = cache_path(record)
    with file_lock(path.with_name("terminal_states.lock")):
        current = read_cache(record)
        states = dict(current.terminal_states) if current else {}
        states.update(cache.terminal_states)
        merged = TerminalStateCache(
            cache.workflow_id, cache.submission_index, states, cache.observed_at
        )
        payload = json.dumps(
            to_json(merged), ensure_ascii=False, separators=(",", ":")
        ).encode("utf-8")
        try:
            compressed = zstd.ZstdCompressor().compress(payload)
        except zstd.ZstdError as error:
            raise OSError("Could not compress terminal-state cache") from error
        atomic_bytes(path, compressed)
        path.with_suffix("").unlink(missing_ok=True)
