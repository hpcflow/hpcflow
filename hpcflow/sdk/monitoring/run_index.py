"""Persistent run coordinates and parent jobscript elements for scheduler-only reads."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import TYPE_CHECKING

import zstandard as zstd

from ..core.enums import EARStatus
from .headers import is_archive, read_bytes
from .models import ActionRunState, SubmissionRecord, to_json
from .registry import atomic_bytes, file_lock

if TYPE_CHECKING:
    from pathlib import Path
    from ..core.workflow import Workflow


@dataclass(frozen=True, slots=True)
class RunMetadata:
    """Stable display coordinates and the zero-based parent scheduler array element."""

    task: int
    task_name: str
    element: int
    iteration: int
    action: int
    attempt: int
    jobscript_index: int
    jobscript_element: int
    started_at: str = ""

    @classmethod
    def from_state(
        cls, state: ActionRunState, jobscript: int, element: int
    ) -> RunMetadata:
        """Capture coordinates once; state and exit results are never authoritative here."""
        return cls(
            state.task,
            state.task_name,
            state.element,
            state.iteration,
            state.action,
            state.attempt,
            jobscript,
            element,
            state.started_at,
        )

    @classmethod
    def from_json(cls, value: dict) -> RunMetadata:
        """Validate indexed positions before they can suppress a workflow-store read."""
        keys = (
            "task",
            "element",
            "iteration",
            "action",
            "attempt",
            "jobscript_index",
            "jobscript_element",
        )
        if any(type(value.get(key)) is not int or value[key] < 0 for key in keys):
            raise ValueError("Invalid run coordinates")
        if not isinstance(value.get("task_name"), str) or not isinstance(
            value.get("started_at", ""), str
        ):
            raise ValueError("Invalid run metadata")
        return cls(
            **{key: value[key] for key in (*keys, "task_name")},
            started_at=value.get("started_at", ""),
        )

    def scheduler_state(self, name: str) -> ActionRunState:
        """Construct a nonterminal, explicitly scheduler-derived display observation.

        Submitted is the only engine-level assertion: the parent job is scheduled.
        A queued parent does not supply an individual run's stored start/exit state.
        """
        return ActionRunState(
            self.task,
            self.task_name,
            self.element,
            self.iteration,
            self.action,
            self.attempt,
            EARStatus.submitted,
            started_at=self.started_at,
            display_state=name,
        )


@dataclass(slots=True)
class RunIndex:
    """Submission-scoped metadata, shared safely by different submission parts."""

    workflow_id: str
    submission_index: int
    runs: dict[int, RunMetadata] = field(default_factory=dict)
    schema_version: int = 1


def index_path(record: SubmissionRecord) -> Path:
    """Return the sibling of terminal_states.json.zst without creating directories."""
    from .cache import cache_path

    return cache_path(record).with_name("run_index.json.zst")


def read_index(record: SubmissionRecord) -> RunIndex:
    """Read an index from disk or ZIP; invalid or foreign data forces normal reads."""
    try:
        payload = read_bytes(
            record.workflow_path,
            f"artifacts/submissions/{record.submission_index}/tmp/run_index.json.zst",
        )
        data = json.loads(zstd.ZstdDecompressor().decompress(payload))
        if (
            data["schema_version"] != 1
            or data["workflow_id"] != record.workflow_id
            or data["submission_index"] != record.submission_index
            or not isinstance(data["runs"], dict)
        ):
            raise ValueError("Incompatible run index")
        runs = {}
        for key, value in data["runs"].items():
            run_id = int(key)
            if run_id < 0 or str(run_id) != key:
                raise ValueError("Invalid run ID")
            runs[run_id] = RunMetadata.from_json(value)
        return RunIndex(record.workflow_id, record.submission_index, runs)
    except (OSError, ValueError, KeyError, TypeError, AttributeError, zstd.ZstdError):
        return RunIndex(record.workflow_id, record.submission_index)


def write_index(record: SubmissionRecord, runs: dict[int, RunMetadata]) -> None:
    """Merge new metadata under a lock and atomically write compressed JSON."""
    if not runs or is_archive(record.workflow_path):
        return
    path = index_path(record)
    with file_lock(path.with_name("run_index.lock")):
        index = read_index(record)
        index.runs.update(runs)
        payload = json.dumps(
            to_json(index), separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        atomic_bytes(path, zstd.ZstdCompressor().compress(payload))


def parent_elements(
    record: SubmissionRecord, workflow: Workflow
) -> dict[int, tuple[int, int]]:
    """Read block ID arrays once, without constructing any EAR objects.

    Every block shares the jobscript's element axis (axis 1). Missing IDs are -1;
    do not confuse a task element with its scheduler array-element index. Unusual
    or legacy layouts use the existing store-read path instead of a guessed map.
    """
    result: dict[int, tuple[int, int]] = {}
    ambiguous = set()
    try:
        submission = workflow.submissions[record.submission_index]
        for job in record.jobs:
            wanted = set(job.run_ids)
            for block in submission.jobscripts[job.jobscript_index].blocks:
                array = block.EAR_ID
                if array.ndim != 2:
                    raise ValueError("Invalid jobscript block shape")
                for column in range(array.shape[1]):
                    for value in array[:, column]:
                        run_id = int(value)
                        if run_id < 0 or run_id not in wanted:
                            continue
                        parent = job.jobscript_index, column
                        if run_id in result and result[run_id] != parent:
                            ambiguous.add(run_id)
                        result[run_id] = parent
    except (AttributeError, KeyError, IndexError, TypeError, ValueError):
        return {}
    return {key: value for key, value in result.items() if key not in ambiguous}


def scheduler_entries(
    record: SubmissionRecord, index: RunIndex, ids: list[int]
) -> dict[int, ActionRunState]:
    """Skip reads only for fresh queued/waiting parents; inspect running elements."""
    wanted = set(ids)
    result = {}
    for job in record.jobs:
        if job.observation is None:
            continue
        for run_id in job.run_ids:
            metadata = index.runs.get(run_id)
            if (
                run_id not in wanted
                or metadata is None
                or metadata.jobscript_index != job.jobscript_index
            ):
                continue
            state = job.observation.state or job.observation.elements.get(
                metadata.jobscript_element
            )
            if state in ("pending", "waiting"):
                result[run_id] = metadata.scheduler_state("scheduler_pending")
    return result
