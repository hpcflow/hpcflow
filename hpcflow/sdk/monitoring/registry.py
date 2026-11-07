"""Independent, versioned submission history used by show and cancellation."""

from __future__ import annotations

from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
from typing import Any, TYPE_CHECKING

from .settings import get_limit
from .models import (
    JobRecord,
    ProcessIdentity,
    ProcessReference,
    RegistryData,
    SubmissionRecord,
    to_json,
)
from .naming import registered_workflow_name, workflow_name_from_path

if TYPE_CHECKING:
    from ..app import BaseApp
    from ..submission.jobscript import Jobscript
    from ..submission.submission import Submission


def timestamp() -> str:
    """Return the current aware UTC timestamp in the registry's ISO string format."""
    return datetime.now(timezone.utc).isoformat()


@contextmanager
def file_lock(path: Path):
    """An OS-managed lock, released even if its owner exits unexpectedly."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+b") as stream:
        stream.seek(0, os.SEEK_END)
        if not stream.tell():
            stream.write(b"\0")
            stream.flush()
        deadline = time.monotonic() + 10
        while True:
            try:
                stream.seek(0)
                if sys.platform == "win32":
                    import msvcrt

                    msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl

                    fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"Timed out locking {path}")
                time.sleep(0.05)
        try:
            yield
        finally:
            stream.seek(0)
            if sys.platform == "win32":
                msvcrt.locking(stream.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def atomic_json(path: Path, value: Any) -> None:
    """Write JSON through a temporary sibling and atomically replace the destination.

    Create the parent directory if needed, flush file data before replacement,
    and remove the temporary file on failure. Callers provide any required locking.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomic_bytes(path: Path, data: bytes) -> None:
    """Atomically replace a binary cache using a flushed temporary sibling.

    Callers own locking. A failed write leaves the previous cache intact and
    removes its temporary file, using the same guarantees as registry JSON writes.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix=path.name, suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def new_record(
    path: str,
    workflow_id: str,
    sub_idx: int,
    part: str,
    workflow_name: str | None = None,
    template_name: str | None = None,
) -> SubmissionRecord:
    """Build an unregistered submission-part record with optional saved names.

    Use the part identifier as its submission timestamp. Recover a display name
    from the path when none is supplied; ID assignment and persistence are left
    to the registry transaction.
    """
    return SubmissionRecord(
        workflow_path=str(path),
        workflow_id=workflow_id,
        workflow_name=(
            workflow_name if workflow_name is not None else workflow_name_from_path(path)
        ),
        workflow_name_from_metadata=workflow_name is not None,
        template_name=template_name,
        submission_index=sub_idx,
        submission_part_id=part,
        submitted_at=part,
    )


def job_record(job: Jobscript, inspect_scheduler: bool = True) -> JobRecord:
    """Snapshot scheduler settings and references without retaining a live jobscript.

    Include query/cancel command overrides, action-run IDs, and the submission host.
    For local direct jobs, capture process creation time to detect PID reuse.
    Foreign jobs and calls with ``inspect_scheduler=False`` read saved reference
    fields directly, without constructing an adapter or inspecting local PIDs.
    """
    inspect_scheduler = inspect_scheduler and (
        not job.submit_hostname or job.submit_hostname == socket.gethostname()
    )
    scheduler = job.scheduler if inspect_scheduler else None
    args = deepcopy(job._get_submission_scheduler_args())
    for name in ("show_cmd", "del_cmd"):
        if hasattr(scheduler, name):
            args[name] = getattr(scheduler, name)
    reference = (
        job.scheduler_js_ref
        if inspect_scheduler
        else (
            job.scheduler_job_ID
            if job.scheduler_job_ID is not None
            else (job.process_ID, job.submit_cmdline)
        )
    )
    result = JobRecord(
        jobscript_index=job.index,
        scheduler_type=job.scheduler_name,
        os_name=job.os_name,
        scheduler_args=args,
        hostname=job.submit_hostname or socket.gethostname(),
        machine=job.submit_machine,
        scheduler_reference=(
            ProcessReference.from_json(reference)
            if isinstance(reference, tuple)
            else reference
        ),
        is_array=job.is_array,
        run_ids=[int(i) for i in job.all_EAR_IDs],
    )
    if (
        inspect_scheduler
        and result.hostname == socket.gethostname()
        and isinstance(result.scheduler_reference, ProcessReference)
        and result.scheduler_reference.pid is not None
    ):
        import psutil

        try:
            result.process_created_at = psutil.Process(
                result.scheduler_reference.pid
            ).create_time()
        except psutil.Error:
            pass
    return result


class SubmissionRegistry:
    """Persist submission history independently of workflow-store readability.

    The host-scoped JSON registry retains job references needed for monitoring and
    cancellation. Read legacy history without changing its source file. Mutations
    use an OS lock and atomic replacement. Inactive and unknown records have
    separate retention limits; active records and allocated IDs are protected.
    """

    SCHEMA_VERSION = 1

    def __init__(self, app: BaseApp):
        self.app = app
        self.path = app.user_data_hostname_dir / "known_submissions_v2.json"

    def read(self) -> RegistryData:
        """Read typed records, or import legacy text history in memory if JSON is absent.

        Repair legacy display names without loading workflows or writing files.
        Malformed or unsupported JSON raises ValueError rather than silently discarding
        registered job references.
        """
        try:
            with self.path.open(encoding="utf-8") as stream:
                raw = json.load(stream)
        except FileNotFoundError:
            records = []
            try:
                lines = self.app.known_subs_file_path.read_text().splitlines()
            except FileNotFoundError:
                lines = []
            for line in lines:
                if not line.strip():
                    continue
                try:
                    old = self.app._parse_known_submissions_line(line)
                    record = new_record(
                        old["path"],
                        old["workflow_id"],
                        old["sub_idx"],
                        old["submit_time"],
                    )
                    record.local_id = old["local_id"]
                    record.activity = "unknown" if old["is_active"] else "inactive"
                    record.started_at = old["start_time"]
                    record.finished_at = old["end_time"]
                    record.legacy = True
                    records.append(record)
                except (ValueError, KeyError, TypeError):
                    self.app.submission_logger.warning("Ignoring malformed legacy record")
            return RegistryData(
                records, max((record.local_id for record in records), default=-1) + 1
            )
        try:
            data = RegistryData.from_json(raw)
        except (ValueError, KeyError, TypeError, AttributeError) as error:
            raise ValueError(
                f"Unsupported or malformed submission registry {self.path}: {error}"
            ) from error
        for record in data.records:
            record.workflow_name = registered_workflow_name(record)
        return data

    @contextmanager
    def transaction(self):
        """Yield mutable registry data under an exclusive lock and commit on success.

        After the caller finishes, apply separate inactive and unknown retention
        limits, retain every active record, and atomically replace the JSON document.
        Unknown records are ordered by submission time, then local ID; rechecking
        a stale record does not make it recent. Exceptions skip the commit.
        """
        with file_lock(self.path.with_suffix(".lock")):
            data = self.read()
            yield data
            limit = get_limit(self.app.config, "known_submissions_max_inactive", 100)
            inactive = sorted(
                (record for record in data.records if record.activity == "inactive"),
                key=recent_key,
                reverse=True,
            )
            remove = {record.local_id for record in inactive[limit:]}
            unknown_limit = get_limit(
                self.app.config, "known_submissions_max_unknown", 10
            )
            unknown = sorted(
                (record for record in data.records if record.activity == "unknown"),
                key=lambda record: (sort_timestamp(record.submitted_at), record.local_id),
                reverse=True,
            )
            remove.update(record.local_id for record in unknown[unknown_limit:])
            data.records = [
                record for record in data.records if record.local_id not in remove
            ]
            atomic_json(self.path, to_json(data))

    def begin(self, submission: Submission, part: str) -> int:
        """Register a submission part before jobs are launched and return its local ID.

        Save workflow/template names and submitting-process identity immediately so a
        partial or interrupted submission can still be monitored.
        """
        import psutil

        with self.transaction() as data:
            record = new_record(
                str(submission.workflow.path),
                submission.workflow.id_,
                submission.index,
                part,
                workflow_name=submission.workflow.name,
                template_name=getattr(
                    getattr(submission.workflow, "template", None), "name", None
                ),
            )
            record.local_id = data.next_local_id
            record.activity = "active"
            record.accessibility = "readable"
            record.submitting = True
            record.submitting_owner = ProcessIdentity(
                os.getpid(), psutil.Process().create_time()
            )
            record.app_info = self.app.get_info()
            data.next_local_id += 1
            data.records.append(record)
        return record.local_id

    def add_job(self, local_id: int, job: JobRecord) -> None:
        """Append one successful job reference immediately and advance the record revision."""
        with self.transaction() as data:
            record = next(
                record for record in data.records if record.local_id == local_id
            )
            record.jobs.append(job)
            record.revision += 1

    def finish(self, local_id: int) -> None:
        """Clear submission-in-progress tracking and advance the record revision.

        Remove a record with no successfully registered jobs, including submissions that
        failed before launching anything. Existing local IDs remain allocated.
        """
        with self.transaction() as data:
            record = next(
                record for record in data.records if record.local_id == local_id
            )
            record.submitting = False
            record.revision += 1
            if not record.jobs:
                data.records.remove(record)

    def update(self, records: list[SubmissionRecord]) -> set[int]:
        """Merge observations only when their record revisions still match the registry.

        Preserve concurrent submission/job updates rather than overwriting them with
        older observations. Apply normal retention and return the IDs still retained.
        """
        updates = {record.local_id: record for record in records}
        with self.transaction() as data:
            for index, record in enumerate(data.records):
                observation = updates.get(record.local_id)
                if observation and observation.revision == record.revision:
                    data.records[index] = replace(
                        observation, revision=record.revision + 1
                    )
        return {record.local_id for record in data.records}


def sort_timestamp(value: str) -> float:
    """Convert ISO or legacy naive UTC timestamps to sortable seconds, using zero on error."""
    try:
        # Python 3.10 does not accept the ISO UTC Z suffix directly.
        if isinstance(value, str) and value.endswith("Z"):
            value = value[:-1] + "+00:00"
        parsed = datetime.fromisoformat(value)
        return parsed.replace(tzinfo=parsed.tzinfo or timezone.utc).timestamp()
    except (ValueError, TypeError, OverflowError):
        return 0.0


def recent_key(record: SubmissionRecord) -> float:
    """Prefer finish, inactivity, start, then submit time when ordering inactive history."""
    return sort_timestamp(
        record.finished_at
        or record.inactive_at
        or record.started_at
        or record.submitted_at
    )
