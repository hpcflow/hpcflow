"""Typed monitoring data and conversion of the existing JSON file formats.

Dataclasses are used throughout monitoring. Dictionaries remain appropriate for
dynamic scheduler arguments, counters, and indexes keyed by run ID.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from enum import Enum
from typing import Any, Literal, cast

from ..core.enums import EARStatus
from ..core.skip_reason import SkipReason
from .naming import workflow_name_from_path

Activity = Literal["active", "inactive", "unknown"]
Accessibility = Literal["readable", "missing", "incompatible", "unreadable"]


def _integer(data: dict, key: str) -> int:
    value = data[key]
    if type(value) is not int:
        raise ValueError(f"Expected an integer for {key}")
    return value


def _string(data: dict, key: str) -> str:
    value = data[key]
    if not isinstance(value, str):
        raise ValueError(f"Expected a string for {key}")
    return value


def _scalars(cls, data: dict, exclude: set[str]) -> dict[str, Any]:
    return {
        f.name: data[f.name]
        for f in fields(cls)
        if f.name in data and f.name not in exclude
    }


@dataclass(frozen=True, slots=True)
class ProcessReference:
    """A direct job's process ID and original command, without a workflow object."""

    pid: int | None
    command: tuple[str, ...] | None

    @classmethod
    def from_json(cls, data) -> ProcessReference:
        """Validate and decode the historical two-item PID/command representation."""
        if not isinstance(data, (list, tuple)) or len(data) != 2:
            raise ValueError("Invalid process reference")
        pid, command = data
        if pid is not None and type(pid) is not int:
            raise ValueError("Invalid process ID")
        if command is not None and (
            not isinstance(command, (list, tuple))
            or not all(isinstance(arg, str) for arg in command)
        ):
            raise ValueError("Invalid process command")
        return cls(pid, tuple(command) if command is not None else None)


@dataclass(frozen=True, slots=True)
class ProcessIdentity:
    """PID and creation time used to distinguish a submitter from a later reused PID."""

    pid: int
    created_at: float

    @classmethod
    def from_json(cls, data) -> ProcessIdentity:
        """Validate a saved PID and process creation timestamp."""
        if (
            len(data) != 2
            or type(data[0]) is not int
            or not isinstance(data[1], (int, float))
        ):
            raise ValueError("Invalid submitting process identity")
        return cls(data[0], data[1])


@dataclass(frozen=True, slots=True)
class WorkflowHeaderToken:
    """File identity and stat values used to avoid reparsing an unchanged store header."""

    filename: str
    device: int
    inode: int
    size: int
    modified_ns: int
    changed_ns: int

    @classmethod
    def from_json(cls, data) -> WorkflowHeaderToken:
        """Validate and decode the six-item header file identity/stat sequence."""
        if (
            len(data) != 6
            or not isinstance(data[0], str)
            or not all(type(i) is int for i in data[1:])
        ):
            raise ValueError("Invalid workflow header token")
        return cls(*data)


@dataclass(frozen=True, slots=True)
class WorkflowKey:
    """Workflow path and persistent ID used to share loads and reject replaced stores."""

    path: str
    workflow_id: str


@dataclass(slots=True)
class JobRecord:
    """Saved scheduler context and reference for one successfully submitted jobscript.

    Contain everything required to query or cancel without opening a workflow store.
    ``run_ids`` associate jobs with cached action states; observations and direct
    process creation time support later status checks and PID-reuse detection.
    """

    jobscript_index: int
    scheduler_type: str = ""
    os_name: str = ""
    scheduler_args: dict[str, Any] = field(default_factory=dict)
    hostname: str = ""
    machine: str | None = None
    scheduler_reference: str | ProcessReference | None = None
    is_array: bool = False
    run_ids: list[int] = field(default_factory=list)
    process_created_at: float | None = None
    last_checked_at: str = ""
    last_observed_state: list[str] = field(default_factory=list)

    @classmethod
    def from_json(cls, data: dict) -> JobRecord:
        """Decode a saved job, converting legacy process-reference sequences to a model."""
        values = _scalars(cls, data, {"scheduler_reference", "jobscript_index"})
        reference = data.get("scheduler_reference")
        if isinstance(reference, (list, tuple)):
            reference = ProcessReference.from_json(reference)
        elif reference is not None and not isinstance(reference, str):
            raise ValueError("Invalid scheduler reference")
        if not isinstance(data.get("scheduler_args", {}), dict):
            raise ValueError("Invalid scheduler arguments")
        if not isinstance(data.get("run_ids", []), list) or not all(
            type(i) is int for i in data.get("run_ids", [])
        ):
            raise ValueError("Invalid action run IDs")
        return cls(
            _integer(data, "jobscript_index"), scheduler_reference=reference, **values
        )


@dataclass(slots=True)
class ActionRunState:
    """A run's engine state and its position in the task/element/iteration layout."""

    task: int
    task_name: str
    element: int
    iteration: int
    action: int
    attempt: int
    state: EARStatus
    started_at: str = ""
    finished_at: str = ""
    skip_reason: SkipReason | None = None

    @classmethod
    def from_json(cls, data: dict) -> ActionRunState:
        """Decode cached run coordinates, engine status, timestamps, and optional skip reason."""
        return cls(
            **{
                key: _integer(data, key)
                for key in ("task", "element", "iteration", "action", "attempt")
            },
            task_name=_string(data, "task_name"),
            state=EARStatus[data["state"]],
            started_at=data.get("started_at", ""),
            finished_at=data.get("finished_at", ""),
            skip_reason=(
                SkipReason(data["skip_reason"])
                if data.get("skip_reason") is not None
                else None
            ),
        )


@dataclass(frozen=True, slots=True)
class ActionStatus:
    """An action attempt's display state within an iteration."""

    action: int
    attempt: int
    state: str


@dataclass(frozen=True, slots=True)
class IterationActionState:
    """A named, immutable step in a grouped element's iteration pattern."""

    iteration: int
    action: int
    attempt: int
    state: str

    @property
    def status(self) -> ActionStatus:
        """Return the iteration-independent action/attempt state used to compare patterns."""
        return ActionStatus(self.action, self.attempt, self.state)

    @classmethod
    def from_json(cls, data) -> IterationActionState:
        """Validate the historical iteration/action/attempt/state sequence."""
        if (
            len(data) != 4
            or not all(type(i) is int for i in data[:3])
            or not isinstance(data[3], str)
        ):
            raise ValueError("Invalid iteration action pattern")
        return cls(*data)


@dataclass(slots=True)
class ElementGroup:
    """Elements of one task sharing the same ordered iteration/action/attempt states."""

    task: str
    pattern: tuple[IterationActionState, ...]
    elements: list[int]

    @classmethod
    def from_json(cls, data: dict) -> ElementGroup:
        """Decode one task's element indices and shared iteration/action pattern."""
        return cls(
            _string(data, "task"),
            tuple(IterationActionState.from_json(i) for i in data["pattern"]),
            list(data["elements"]),
        )


@dataclass(slots=True)
class ActionSummary:
    """Compact action counts, grouped patterns, and timing observations for one part.

    ``counts`` preserve engine states; optional ``display_counts`` distinguish skip
    reasons. ``without_loop_skipped`` provides a prepared visibility view rather than
    requiring renderers to reread runs. ``cache_error`` reports persistence failures
    without discarding the observation.
    """

    counts: dict[str, int] = field(default_factory=dict)
    groups: list[ElementGroup] = field(default_factory=list)
    display_counts: dict[str, int] | None = None
    observed_at: str = ""
    complete: bool = False
    started_at: str = ""
    finished_at: str = ""
    without_loop_skipped: ActionSummary | None = None
    cache_error: str | None = None

    @classmethod
    def from_json(cls, data: dict) -> ActionSummary:
        """Decode grouped counts and the optional nested summary excluding loop-skipped runs."""
        values = _scalars(cls, data, {"groups", "without_loop_skipped"})
        return cls(
            groups=[ElementGroup.from_json(i) for i in data.get("groups", [])],
            without_loop_skipped=(
                cls.from_json(data["without_loop_skipped"])
                if data.get("without_loop_skipped") is not None
                else None
            ),
            **values,
        )


@dataclass(slots=True)
class SubmissionRecord:
    """One registered submission part, its job references, and latest observations.

    Activity and workflow accessibility are independent: an unreadable workflow may
    still have active jobs that can be cancelled from saved references. ``revision``
    protects concurrent registry updates. ``template_name`` may be None on older
    records until recovered from metadata. Timestamp strings retain stored UTC
    semantics; renderers alone convert them to a display timezone.
    """

    workflow_path: str
    workflow_id: str
    workflow_name: str
    submission_index: int
    submission_part_id: str
    submitted_at: str
    local_id: int = -1
    workflow_name_from_metadata: bool = False
    started_at: str = ""
    finished_at: str = ""
    activity: Activity = "unknown"
    accessibility: Accessibility = "unreadable"
    jobs: list[JobRecord] = field(default_factory=list)
    revision: int = 0
    submitting: bool = False
    submitting_owner: ProcessIdentity | None = None
    app_info: dict[str, Any] | None = None
    legacy: bool = False
    workflow_header_token: WorkflowHeaderToken | None = None
    last_checked_at: str = ""
    scheduler_states: list[str] = field(default_factory=list)
    scheduler_error: str | None = None
    detail_error: str | None = None
    inactive_at: str = ""
    cancel_requested_at: str = ""
    last_action_summary: ActionSummary | None = None
    template_name: str | None = None

    @classmethod
    def from_json(cls, data: dict) -> SubmissionRecord:
        """Decode a registry entry, preserving defaults for fields absent in older records.

        Validate required identities and activity/accessibility states, then reconstruct
        nested job references, header tokens, and cached summaries.
        """
        required = {
            key: _string(data, key)
            for key in ("workflow_path", "workflow_id", "submission_part_id")
        }
        activity = data.get("activity", "unknown")
        accessibility = data.get("accessibility", "unreadable")
        if activity not in ("active", "inactive", "unknown") or accessibility not in (
            "readable",
            "missing",
            "incompatible",
            "unreadable",
        ):
            raise ValueError("Invalid submission activity or accessibility")
        values = _scalars(
            cls,
            data,
            set(required)
            | {
                "local_id",
                "submission_index",
                "workflow_name",
                "submitted_at",
                "jobs",
                "submitting_owner",
                "workflow_header_token",
                "last_action_summary",
                "activity",
                "accessibility",
            },
        )
        return cls(
            workflow_path=required["workflow_path"],
            workflow_id=required["workflow_id"],
            submission_part_id=required["submission_part_id"],
            local_id=_integer(data, "local_id"),
            submission_index=_integer(data, "submission_index"),
            workflow_name=data.get("workflow_name")
            or workflow_name_from_path(required["workflow_path"]),
            submitted_at=data.get("submitted_at", required["submission_part_id"]),
            activity=cast(Activity, activity),
            accessibility=cast(Accessibility, accessibility),
            jobs=[JobRecord.from_json(i) for i in data.get("jobs", [])],
            submitting_owner=(
                ProcessIdentity.from_json(data["submitting_owner"])
                if data.get("submitting_owner") is not None
                else None
            ),
            workflow_header_token=(
                WorkflowHeaderToken.from_json(data["workflow_header_token"])
                if data.get("workflow_header_token") is not None
                else None
            ),
            last_action_summary=(
                ActionSummary.from_json(data["last_action_summary"])
                if data.get("last_action_summary") is not None
                else None
            ),
            **values,
        )


@dataclass(slots=True)
class RegistryData:
    """The versioned registry document and its monotonic local ID counter."""

    records: list[SubmissionRecord] = field(default_factory=list)
    next_local_id: int = 0
    schema_version: int = 1

    @classmethod
    def from_json(cls, data: dict) -> RegistryData:
        """Validate the schema and unique local IDs, then decode typed submission records.

        Keep the next-ID counter above every retained ID so pruning cannot reuse IDs.
        """
        if data.get("schema_version") != 1 or not isinstance(data.get("records"), list):
            raise ValueError("Unsupported or malformed submission registry")
        records = [SubmissionRecord.from_json(i) for i in data["records"]]
        ids = {record.local_id for record in records}
        if len(ids) != len(records):
            raise ValueError("Duplicate local submission IDs")
        return cls(
            records, max(_integer(data, "next_local_id"), max(ids, default=-1) + 1)
        )


@dataclass(slots=True)
class TerminalStateCache:
    """Terminal action states shared by parts of one workflow submission.

    Run IDs map to states that no longer need store lookups. Workflow/submission
    identity and schema version prevent reuse for another workflow at the same path.
    Active states are deliberately excluded from persistent terminal caches.
    """

    workflow_id: str
    submission_index: int
    terminal_states: dict[int, ActionRunState] = field(default_factory=dict)
    observed_at: str = ""
    schema_version: int = 1


def to_json(value: Any) -> Any:
    """Convert models to the existing JSON schema, including historical sequences."""
    if value is None or type(value) in (str, int, float, bool):
        return value
    if isinstance(value, EARStatus):
        return value.name
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, ProcessReference):
        return [value.pid, to_json(value.command)]
    if isinstance(value, ProcessIdentity):
        return [value.pid, value.created_at]
    if isinstance(value, WorkflowHeaderToken):
        return [getattr(value, f.name) for f in fields(value)]
    if isinstance(value, IterationActionState):
        return [value.iteration, value.action, value.attempt, value.state]
    if is_dataclass(value) and not isinstance(value, type):
        return {
            f.name: to_json(getattr(value, f.name))
            for f in fields(value)
            if getattr(value, f.name) is not None
        }
    if isinstance(value, dict):
        return {str(key): to_json(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json(item) for item in value]
    return value
