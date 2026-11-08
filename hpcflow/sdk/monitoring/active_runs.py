"""Immutable execution plans and constant-size active-run cursors, outside the store."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import tempfile
from typing import TYPE_CHECKING

from .headers import is_archive
from .registry import file_lock, atomic_json

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence
    from ..core.workflow import Workflow
    from ..submission.jobscript import Jobscript


_current: ContextVar[ActiveRunTracker | None] = ContextVar(
    "monitoring_active_runs", default=None
)
ITEMS_PER_SHARD = 500


@dataclass(frozen=True, slots=True)
class RunActivity:
    """An item's ordered plan and last-started position, retained between actions."""

    run_ids: tuple[int, ...]
    positions: dict[int, int]
    cursor: int
    active: int | None
    plan_id: str

    def is_future(self, run_id: int) -> bool:
        """Only an ID explicitly after the cursor has not yet been reached."""
        return run_id in self.positions and self.positions[run_id] > self.cursor


def activity_directory(workflow_path: str, submission: int) -> Path:
    """Return the submission-local active directory without creating it."""
    return (
        Path(workflow_path)
        / "artifacts"
        / "submissions"
        / str(submission)
        / "tmp"
        / "active_runs"
    )


def tracking_enabled(workflow_path: str, workflow_id: str, submission: int) -> bool:
    """Require the cursor protocol to have been enabled before execution began."""
    if is_archive(workflow_path):
        return False
    try:
        return json.loads(
            (activity_directory(workflow_path, submission) / "enabled.json").read_bytes()
        ) == dict(schema_version=2, workflow_id=workflow_id, submission_index=submission)
    except (OSError, ValueError, TypeError):
        return False


def _identity(workflow_id: str, submission: int, jobscript: int, item: int) -> dict:
    return dict(
        schema_version=2,
        workflow_id=workflow_id,
        submission_index=submission,
        jobscript_index=jobscript,
        array_item=item,
    )


def _paths(directory: Path, jobscript: int, item: int) -> tuple[Path, Path]:
    """Partition items and separate markers, plans, locks, and temporary files.

    Each leaf holds at most 500 items of one file type. Radix-256 branch directories
    also bound parent fanout, even with many jobs or more than 250,000 array items.
    """
    name = f"js_{jobscript}_item_{item}"
    job = _shard_node(directory / "jobs", jobscript)
    shard = _shard_node(job / "items", item // ITEMS_PER_SHARD)
    return shard / "markers" / f"{name}.json", shard / "plans" / f"{name}.json"


def _shard_node(root: Path, index: int) -> Path:
    """Encode a nonnegative index without mixing branch entries and leaf files."""
    if index < 0:
        raise ValueError("Negative shard index")
    digits = f"{index:x}"
    digits = digits.zfill(len(digits) + len(digits) % 2)
    for offset in range(0, len(digits), 2):
        root = root / "children" / digits[offset : offset + 2]
    return root


def _sharded_file(path: Path) -> bool:
    return path.parent.parent.parent.name == "children"


def _ids(values) -> tuple[int, ...]:
    if not isinstance(values, list) or any(
        type(value) is not int or value < 0 for value in values
    ):
        raise ValueError("Invalid run IDs")
    return tuple(values)


def _plan_id(ids: tuple[int, ...]) -> str:
    return sha256(json.dumps(ids, separators=(",", ":")).encode()).hexdigest()


def _atomic_file(path: Path, data: dict) -> None:
    """Atomically publish telemetry without a durable store write per action."""
    path.parent.mkdir(parents=True, exist_ok=True)
    sharded = _sharded_file(path)
    temporary_directory = (
        path.parent.parent / "temporary" / path.parent.name if sharded else path.parent
    )
    temporary_directory.mkdir(parents=True, exist_ok=True)
    if sharded:
        # One staging name per item/type also bounds leftovers from killed writers.
        # Runtime callers hold the item's lock; initialization precedes execution.
        temporary = str(temporary_directory / f"{path.name}.tmp")
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        try:
            fd = os.open(temporary, flags, 0o600)
        except FileExistsError:
            os.unlink(temporary)
            fd = os.open(temporary, flags, 0o600)
    else:
        fd, temporary = tempfile.mkstemp(prefix=".active-", dir=temporary_directory)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(json.dumps(data, separators=(",", ":")).encode())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def execution_plans(job: Jobscript) -> dict[int, tuple[int, ...]]:
    """Follow generated loop order rather than numeric run-ID order.

    Array items execute one column in action order. Serial shell jobs execute
    block/element/action; combined Python jobs execute block/action/element.
    Missing -1 slots are omitted; ambiguous repeated IDs disable this optimization.
    """
    arrays = [block.EAR_ID for block in job.blocks]
    if any(array.ndim != 2 for array in arrays):
        raise ValueError("Unsupported execution plan")
    if job.is_array:
        if len(arrays) != 1:
            raise ValueError("Unsupported multi-block array")
        plans = {
            column: tuple(int(value) for value in arrays[0][:, column] if value >= 0)
            for column in range(arrays[0].shape[1])
        }
    else:
        combined = getattr(getattr(job, "resources", None), "combine_scripts", False)
        plans = {
            0: tuple(
                int(value)
                for array in arrays
                for value in (array if combined else array.T).flat
                if value >= 0
            )
        }
    if any(len(set(ids)) != len(ids) for ids in plans.values()):
        raise ValueError("Repeated IDs in execution plan")
    return plans


def enable_tracking(
    workflow: Workflow, submission: int, jobscripts: Sequence[Jobscript] | None = None
) -> None:
    """Write immutable plans and initial cursors before a new submission can run."""
    if tracking_enabled(str(workflow.path), workflow.id_, submission):
        return  # Never reset live cursors on a retry of submission setup.
    try:
        jobscripts = (
            jobscripts
            if jobscripts is not None
            else workflow.submissions[submission].jobscripts
        )
        directory = activity_directory(str(workflow.path), submission)
        for jobscript, job in enumerate(jobscripts):
            for item, ids in execution_plans(job).items():
                identity = _identity(workflow.id_, submission, jobscript, item)
                marker, plan = _paths(directory, jobscript, item)
                digest = _plan_id(ids)
                _atomic_file(plan, dict(identity, plan_id=digest, run_ids=ids))
                _atomic_file(
                    marker, dict(identity, plan_id=digest, cursor=-1, active_run_id=None)
                )
        atomic_json(
            directory / "enabled.json",
            dict(schema_version=2, workflow_id=workflow.id_, submission_index=submission),
        )
    except (OSError, ValueError, TypeError, AttributeError) as error:
        workflow._app.submission_logger.warning(
            "Could not enable active-run tracking: %s", error
        )


def _read_plan(path: Path, identity: dict) -> tuple[tuple[int, ...], str]:
    data = json.loads(path.read_bytes())
    if any(data.get(key) != value for key, value in identity.items()):
        raise ValueError("Incompatible execution plan")
    ids = _ids(data["run_ids"])
    digest = _plan_id(ids)
    if len(set(ids)) != len(ids) or data["plan_id"] != digest:
        raise ValueError("Invalid execution plan")
    return ids, digest


def _read_existing_plan(
    directory: Path, jobscript: int, item: int, identity: dict
) -> tuple[Path, Path, tuple[int, ...], str]:
    """Read sharded plans without extra stat calls, preserving older flat layouts.

    Only a missing plan triggers legacy lookup; corrupt sharded data must retain
    the store fallback rather than silently reviving an older cursor.
    """
    marker, plan = _paths(directory, jobscript, item)
    try:
        ids, digest = _read_plan(plan, identity)
    except FileNotFoundError:
        name = f"js_{jobscript}_item_{item}.json"
        marker, plan = directory / name, directory / "plans" / name
        ids, digest = _read_plan(plan, identity)
    return marker, plan, ids, digest


def _read_cursor(
    path: Path, identity: dict, ids: tuple[int, ...], digest: str
) -> tuple[int, int | None]:
    data = json.loads(path.read_bytes())
    if (
        any(data.get(key) != value for key, value in identity.items())
        or data["plan_id"] != digest
    ):
        raise ValueError("Incompatible active-run cursor")
    cursor, active = data["cursor"], data["active_run_id"]
    if type(cursor) is not int or not -1 <= cursor < len(ids):
        raise ValueError("Invalid cursor position")
    if active is not None and (
        type(active) is not int or cursor < 0 or active != ids[cursor]
    ):
        raise ValueError("Active ID does not match cursor")
    return cursor, active


def read_activity(
    workflow_path: str, workflow_id: str, submission: int, jobscript: int, item: int
) -> RunActivity | None:
    """Read a matching immutable plan and atomic cursor, or use the store fallback."""
    if is_archive(workflow_path):
        return None
    identity = _identity(workflow_id, submission, jobscript, item)
    try:
        marker, _, ids, digest = _read_existing_plan(
            activity_directory(workflow_path, submission), jobscript, item, identity
        )
        cursor, active = _read_cursor(marker, identity, ids, digest)
        return RunActivity(
            ids,
            {run_id: index for index, run_id in enumerate(ids)},
            cursor,
            active,
            digest,
        )
    except (OSError, ValueError, KeyError, TypeError, AttributeError):
        return None


class ActiveRunTracker:
    """Advance a constant-size cursor through a fixed sequential execution plan."""

    def __init__(self, workflow: Workflow, submission: int, jobscript: int, item: int):
        self.workflow = workflow
        self.submission = submission
        self.directory = activity_directory(str(workflow.path), submission)
        self.identity = _identity(workflow.id_, submission, jobscript, item)
        self.marker, self.plan, self.run_ids, self.plan_id = _read_existing_plan(
            self.directory, jobscript, item, self.identity
        )
        self.lock = (
            self.marker.parent.parent / "locks" / self.marker.with_suffix(".lock").name
            if _sharded_file(self.marker)
            else self.marker.with_suffix(".lock")
        )
        self.positions = {run_id: index for index, run_id in enumerate(self.run_ids)}
        self._active: set[int] = set()

    @classmethod
    def for_workflow(
        cls, workflow: Workflow, submission: int, jobscript: int
    ) -> ActiveRunTracker | None:
        """Resolve a scheduler array item, or item zero for a direct/serial job."""
        if not tracking_enabled(str(workflow.path), workflow.id_, submission):
            return None
        try:
            job = workflow.submissions[submission].jobscripts[jobscript]
            value = os.environ.get(f"{workflow._app.package_name.upper()}_JS_ELEM_IDX")
            if job.is_array:
                if value is None:
                    return None
                item = int(value)
            else:
                item = 0
            return cls(workflow, submission, jobscript, item) if item >= 0 else None
        except (OSError, ValueError, KeyError, IndexError, TypeError, AttributeError):
            return None

    def _update(
        self, starts: list[int] | None = None, finishes: list[int] | None = None
    ) -> None:
        try:
            with file_lock(self.lock):
                cursor, active = _read_cursor(
                    self.marker, self.identity, self.run_ids, self.plan_id
                )
                if starts is not None:
                    if len(starts) != 1:
                        raise ValueError("An array item executes one run at a time")
                    position = self.positions[starts[0]]
                    if position not in (cursor, cursor + 1):
                        raise ValueError("Run does not follow the execution plan")
                    cursor, active = position, starts[0]
                if finishes is not None and active in finishes:
                    active = None  # Retain position through the gap before the next run.
                _atomic_file(
                    self.marker,
                    dict(
                        self.identity,
                        plan_id=self.plan_id,
                        cursor=cursor,
                        active_run_id=active,
                    ),
                )
        except (OSError, ValueError, KeyError, TypeError, AttributeError) as error:
            try:
                self.marker.unlink(missing_ok=True)
            except OSError:
                pass
            self.workflow._app.submission_logger.warning(
                "Could not update active-run marker: %s", error
            )

    def start(self, run_ids: Iterable[int]) -> None:
        """Publish the next sequential run before execution/preparation."""
        self._active = {int(value) for value in run_ids if value >= 0}
        self._update(starts=list(self._active))

    def finish(self, run_ids: Iterable[int]) -> None:
        """Clear the active ID while retaining the last-started cursor."""
        ids = [int(value) for value in run_ids if value >= 0]
        self._active.difference_update(ids)
        self._update(finishes=ids)

    def clear(self) -> None:
        """Clear the current ID on exit without discarding progress."""
        self.finish(list(self._active))

    @contextmanager
    def context(self) -> Iterator[None]:
        """Route cross-item skip notifications to this submission during execution."""
        token = _current.set(self)
        try:
            yield
        finally:
            _current.reset(token)


def read_changed_runs(
    workflow_path: str, workflow_id: str, submission: int
) -> set[int] | None:
    """Read rare cross-item skip notifications; malformed data requires ordinary reads."""
    path = activity_directory(workflow_path, submission) / "changed.jsonl"
    try:
        payload = path.read_bytes()
    except FileNotFoundError:
        return set()
    except OSError:
        return None
    try:
        result: set[int] = set()
        for line in payload.splitlines():
            data = json.loads(line)
            if data["workflow_id"] != workflow_id:
                raise ValueError("Foreign activity notification")
            result.update(_ids(data["run_ids"]))
        return result
    except (ValueError, KeyError, TypeError, AttributeError):
        return None


@contextmanager
def track_run(
    workflow: Workflow, submission: int, jobscript: int, run_id: int
) -> Iterator[None]:
    """Track ordinary execution, retaining the cursor even when execution raises."""
    try:
        tracker = ActiveRunTracker.for_workflow(workflow, submission, jobscript)
    except (AttributeError, KeyError, IndexError, TypeError, ValueError):
        tracker = None
    if tracker is None:
        yield
        return
    with tracker.context():
        tracker.start([run_id])
        try:
            yield
        finally:
            tracker.finish([run_id])


def note_changed_runs(workflow: Workflow, run_ids: Iterable[int]) -> None:
    """Notify show about skips that affect unvisited actions in other array items."""
    tracker = _current.get()
    if tracker is None or tracker.workflow.id_ != workflow.id_:
        return
    # The current run is already covered by this item's cursor. Only
    # cross-item or future updates need the shared (rarely written) notification.
    ids = [int(value) for value in run_ids if int(value) not in tracker._active]
    if not ids:
        return
    path = tracker.directory / "changed.jsonl"
    try:
        with file_lock(path.with_suffix(".lock")):
            with path.open("ab") as stream:
                stream.write(
                    json.dumps(
                        dict(workflow_id=workflow.id_, run_ids=ids), separators=(",", ":")
                    ).encode()
                    + b"\n"
                )
    except OSError as error:
        workflow._app.submission_logger.warning(
            "Could not record changed run IDs: %s", error
        )
