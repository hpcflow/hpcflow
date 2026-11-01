"""Collect submission observations without coupling activity to store access."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import TYPE_CHECKING
from collections.abc import Mapping, Sequence
import json
from pathlib import Path
import socket

from .cache import read_cache, refresh_actions, run_ids, snapshot
from .references import resolve_records
from .filtering import filter_templates, metadata_template_name
from .settings import get_limit
from .models import (
    JobRecord,
    ProcessReference,
    SubmissionRecord,
    WorkflowHeaderToken,
    WorkflowKey,
    to_json,
)

from .registry import (
    SubmissionRegistry,
    job_record,
    new_record,
    recent_key,
    sort_timestamp,
    timestamp,
)

if TYPE_CHECKING:
    from ..app import BaseApp
    from ..core.workflow import Workflow


@dataclass(slots=True)
class RegisteredJob:
    """Associate a saved jobscript reference with its owning submission record."""

    submission: SubmissionRecord
    job: JobRecord


@dataclass(slots=True)
class ResolvedJob:
    """Pair a registered job with the scheduler adapter's validated reference."""

    registration: RegisteredJob
    reference: str | tuple[int, list[str]] | None


def check_identity(record: SubmissionRecord) -> None:
    """Verify the workflow header before trusting cached action-run identities.

    Reject missing stores, unsupported headers, and paths now belonging to another
    workflow. Reuse an unchanged header token where possible; otherwise refresh the
    saved workflow/template names and token on the supplied record. This check does
    not construct a workflow object.
    """
    path = Path(record.workflow_path)
    if not path.exists():
        raise FileNotFoundError(path)
    for filename in (".zattrs", "metadata.json"):
        header = path / filename
        if header.exists():
            stat = header.stat()
            token = WorkflowHeaderToken(
                filename,
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime_ns,
                stat.st_ctime_ns,
            )
            if (
                record.workflow_header_token == token
                and record.workflow_name_from_metadata
            ):
                return
            metadata = json.loads(header.read_text(encoding="utf-8"))
            if metadata["creation_info"]["id"] != record.workflow_id:
                raise ValueError("Workflow at this path has been replaced")
            if isinstance(metadata.get("name"), str) and metadata["name"]:
                record.workflow_name = metadata["name"]
                record.workflow_name_from_metadata = True
            record.template_name = metadata_template_name(metadata)
            record.workflow_header_token = token
            return
    raise ValueError("Unsupported workflow format or missing workflow metadata")


def load_workflow(
    app: BaseApp, record: SubmissionRecord, loaded: dict[WorkflowKey, Workflow]
) -> Workflow:
    """Load and verify a workflow once per path/ID pair within one collection.

    Reuse ``loaded`` across submission parts, recheck the workflow ID after loading,
    and refresh the record's workflow and template names. Loading errors propagate
    to the caller, which can mark the record inaccessible while retaining job references.
    """
    key = WorkflowKey(record.workflow_path, record.workflow_id)
    if key not in loaded:
        check_identity(record)
        workflow = app.Workflow(record.workflow_path)
        if workflow.id_ != record.workflow_id:
            raise ValueError("Workflow identity does not match submission record")
        loaded[key] = workflow
    workflow = loaded[key]
    record.template_name = workflow.template.name
    record.workflow_name = workflow.name
    record.workflow_name_from_metadata = True
    return workflow


def hydrate(
    app: BaseApp, record: SubmissionRecord, loaded: dict[WorkflowKey, Workflow]
) -> None:
    """Backfill saved job references for one legacy submission part.

    Load the workflow through the per-invocation cache, snapshot its jobscripts,
    and clear the legacy marker only after successful recovery. Errors propagate
    without inventing scheduler references for unreadable workflows.
    """
    workflow = load_workflow(app, record, loaded)
    sub = workflow.submissions[record.submission_index]
    indices = sub._submission_parts[record.submission_part_id]
    record.jobs = [job_record(sub.jobscripts[i]) for i in indices]
    record.legacy = False
    record.accessibility = "readable"


def note_failure(app: BaseApp, record: SubmissionRecord, error: Exception) -> None:
    """Record an inspection failure and log it without changing scheduler activity."""
    if isinstance(error, FileNotFoundError):
        record.accessibility = "missing"
    elif isinstance(error, (ValueError, KeyError, IndexError, TypeError, AttributeError)):
        record.accessibility = "incompatible"
    else:
        record.accessibility = "unreadable"
    record.detail_error = str(error)
    app.submission_logger.info("Cannot inspect %s: %s", record.workflow_path, error)


def context_key(job: JobRecord) -> str:
    """Return a stable key for jobs that can share one scheduler status request.

    Include scheduler type, OS, all saved scheduler arguments, and submission host.
    Different scheduler configurations must not share an adapter invocation.
    """
    return json.dumps(
        {
            "scheduler_type": job.scheduler_type,
            "os_name": job.os_name,
            "scheduler_args": job.scheduler_args,
            "hostname": job.hostname,
        },
        sort_keys=True,
    )


def scheduler_for(app: BaseApp, job: JobRecord):
    """Construct an adapter from saved scheduler settings after checking its host.

    Cross-host queued jobs are accepted only when their saved machine matches the
    configured machine. Other foreign-host references raise ValueError.
    """
    if job.hostname != socket.gethostname():
        queued_on_same_machine = (
            isinstance(job.scheduler_reference, str)
            and job.machine
            and job.machine == app.config.get("machine")
        )
        if not queued_on_same_machine:
            raise ValueError(f"Scheduler reference belongs to host {job.hostname}")
    return app.get_scheduler(job.scheduler_type, job.os_name, job.scheduler_args)


def scheduler_ref(job: JobRecord) -> str | tuple[int, list[str]] | None:
    """Convert a saved reference to the adapter API and guard against reused PIDs.

    Return None when a direct process has exited or its PID belongs to a different
    process. Incomplete references raise ValueError rather than querying or
    cancelling an unrelated process.
    """
    reference = job.scheduler_reference
    if isinstance(reference, ProcessReference):
        if reference.pid is None or reference.command is None:
            raise ValueError("Incomplete direct process reference")
        if job.process_created_at is not None:
            import psutil

            try:
                if psutil.Process(reference.pid).create_time() != job.process_created_at:
                    return None
            except psutil.NoSuchProcess:
                return None
        # Scheduler adapters have an established tuple-based API. Conversion is
        # confined to this boundary; monitoring stores a ProcessReference object.
        return reference.pid, list(reference.command)
    if not isinstance(reference, str) or not reference:
        raise ValueError("Incomplete scheduler reference")
    return reference


def observe_activity(app: BaseApp, records: list[SubmissionRecord]) -> None:
    """Update records in place using at most one status call per scheduler context.

    Batch saved references across submission parts; known inactive records do not
    need queries. A live submitting process also keeps a record active. Scheduler
    failures produce unknown activity unless activity is independently known to be
    active, and are recorded without aborting observations of other contexts.
    This function updates observations in memory; its caller controls persistence.
    """
    contexts: defaultdict[str, list[RegisteredJob]] = defaultdict(list)
    failed = set()
    for record in records:
        if record.activity == "inactive":
            continue
        if record.submitting and record.submitting_owner:
            import psutil

            owner = record.submitting_owner
            try:
                record.submitting = (
                    psutil.Process(owner.pid).create_time() == owner.created_at
                )
            except psutil.NoSuchProcess:
                record.submitting = False
        record.scheduler_states = []
        record.scheduler_error = None
        if not record.jobs:
            record.activity = (
                "active"
                if record.submitting
                else "unknown" if record.legacy else "inactive"
            )
            continue
        record.activity = "active" if record.submitting else "inactive"
        for job in record.jobs:
            try:
                contexts[context_key(job)].append(RegisteredJob(record, job))
            except (KeyError, TypeError, ValueError) as error:
                failed.add(record.local_id)
                record.scheduler_error = str(error)
    for items in contexts.values():
        try:
            scheduler = scheduler_for(app, items[0].job)
            refs = [ResolvedJob(item, scheduler_ref(item.job)) for item in items]
            valid = [item.reference for item in refs if item.reference is not None]
            states = scheduler.get_job_state_info(js_refs=valid) if valid else {}
            for resolved in refs:
                record, job = resolved.registration.submission, resolved.registration.job
                ref = resolved.reference
                key = str(ref[0]) if isinstance(ref, tuple) else ref
                observed = states.get(key)
                job.last_checked_at = timestamp()
                job.last_observed_state = (
                    [state.name for state in observed.values()]
                    if isinstance(observed, Mapping)
                    else [observed.name] if observed else []
                )
                if observed:
                    record.activity = "active"
                    record.scheduler_states.extend(job.last_observed_state)
        except Exception as error:
            for item in items:
                record = item.submission
                failed.add(record.local_id)
                record.scheduler_error = str(error)
            app.submission_logger.info("Scheduler monitoring failed: %s", error)
    for record in records:
        if record.local_id in failed and record.activity != "active":
            record.activity = "unknown"
        record.last_checked_at = timestamp()
        if record.activity == "inactive" and not record.inactive_at:
            record.inactive_at = record.last_checked_at


def collect(
    app: BaseApp,
    workflow_ref=None,
    ref_type=None,
    max_recent=None,
    no_update=False,
    include_summary: bool = True,
    template_names: Sequence[str] | str | None = None,
) -> list[SubmissionRecord]:
    """Collect active submissions and a limited recent inactive history.

    Resolve an optional workflow ID, registered name, or path, then apply exact
    template-name filtering before scheduler queries and the inactive-history limit.
    An explicit unregistered path can recover submission parts from its workflow.
    Active and unknown records precede inactive records and do not consume
    ``max_recent`` slots; None uses the ``show_max_recent`` configuration.

    Refresh action summaries only for selected records when ``include_summary`` is
    True, reusing terminal caches and loaded workflows. Inspection failures remain
    visible as accessibility information rather than hiding the submission.
    ``no_update`` prevents registry/cache writes but still observes live states.
    Otherwise merge observations with concurrent registry changes and retention.
    Return invocation-owned snapshots, without live workflow or scheduler objects.
    """
    if max_recent is None:
        max_recent = get_limit(app.config, "show_max_recent", 3)
    if type(max_recent) is not int or max_recent < 0:
        raise ValueError("max_recent must be a non-negative integer")
    registry = SubmissionRegistry(app)
    records = resolve_records(registry.read().records, workflow_ref, ref_type)
    loaded: dict[WorkflowKey, Workflow] = {}
    if workflow_ref is not None and not records:
        # An explicit path may refer to history that has already been pruned.
        workflow = app.Workflow(Path(workflow_ref).resolve())
        for sub in workflow.submissions:
            for part in sub._submission_parts:
                record = new_record(
                    str(workflow.path),
                    workflow.id_,
                    sub.index,
                    part,
                    workflow_name=workflow.name,
                    template_name=workflow.template.name,
                )
                record.local_id = -(len(records) + 1)
                record.legacy = True
                records.append(record)
        loaded[WorkflowKey(str(workflow.path), workflow.id_)] = workflow
    records = filter_templates(records, template_names)
    for record in records:
        if record.legacy and record.activity != "inactive":
            try:
                hydrate(app, record, loaded)
            except Exception as error:
                note_failure(app, record, error)
    observe_activity(app, records)
    active = sorted(
        (r for r in records if r.activity != "inactive"),
        key=lambda r: (r.activity == "active", sort_timestamp(r.submitted_at)),
        reverse=True,
    )
    inactive = sorted(
        (r for r in records if r.activity == "inactive"), key=recent_key, reverse=True
    )[:max_recent]
    selected = active + inactive
    for record in selected:
        try:
            check_identity(record)
            record.accessibility = "readable"
            record.detail_error = None
            if record.legacy:
                hydrate(app, record, loaded)
            if not include_summary:
                continue
            ids = run_ids(record)
            cache = read_cache(record)
            if ids and cache and all(i in cache.terminal_states for i in ids):
                summary = snapshot(cache.terminal_states, ids)
                summary.complete = True
                summary.observed_at = cache.observed_at or "unknown"
            else:
                workflow = load_workflow(app, record, loaded)
                with workflow._store.cache_ctx():
                    summary = refresh_actions(record, workflow, no_update)
            record.last_action_summary = summary
            for field in ("started_at", "finished_at"):
                if getattr(summary, field):
                    setattr(record, field, getattr(summary, field))
        except Exception as error:
            note_failure(app, record, error)
    if not no_update:
        retained = registry.update([r for r in records if r.local_id >= 0])
        selected = [r for r in selected if r.local_id < 0 or r.local_id in retained]
    # These models are owned by this invocation; no live store/scheduler objects escape.
    return selected


def cancel(app: BaseApp, workflow_ref, ref_type=None, quiet=False) -> bool:
    """Cancel saved jobs for all retained parts matching a workflow reference.

    Use registry references even if workflow data cannot be loaded. Legacy records
    may first require hydration. Batch requests by scheduler context and deduplicate
    references within each context. Persist cancellation requests as unknown activity
    pending a subsequent observation.

    Return False only when no registry record matches, allowing a caller fallback.
    Return True for handled records, including those with no active jobs. If some
    requests fail, complete the remaining contexts and then raise RuntimeError with
    the collected errors.
    """
    registry = SubmissionRegistry(app)
    records = resolve_records(registry.read().records, workflow_ref, ref_type)
    if not records:
        return False
    contexts: defaultdict[str, list[RegisteredJob]] = defaultdict(list)
    errors = []
    for record in records:
        if record.activity == "inactive":
            continue
        if record.legacy:
            try:
                hydrate(app, record, {})
            except Exception as error:
                errors.append(f"ID {record.local_id}: no saved references; {error}")
                continue
        for job in record.jobs:
            try:
                contexts[context_key(job)].append(RegisteredJob(record, job))
            except (KeyError, TypeError, ValueError) as error:
                errors.append(str(error))
    requested = False
    for items in contexts.values():
        try:
            scheduler = scheduler_for(app, items[0].job)
            refs = [scheduler_ref(item.job) for item in items]
            refs = [ref for ref in refs if ref is not None]
            # Avoid duplicate requests for jobs shared by records.
            refs = list({json.dumps(to_json(ref)): ref for ref in refs}.values())
            if refs:
                scheduler.cancel_jobs(js_refs=refs, quiet=quiet)
                requested = True
                for item in items:
                    record = item.submission
                    record.cancel_requested_at = timestamp()
                    record.activity = "unknown"
        except Exception as error:
            errors.append(str(error))
    registry.update(records)
    if not quiet:
        print(
            "Cancellation requested." if requested else "No active jobscripts to cancel."
        )
    if errors:
        raise RuntimeError("Some jobs could not be cancelled: " + "; ".join(errors))
    return True
