"""Collect submission observations without coupling activity to store access."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, fields, replace
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from queue import Queue
from typing import TYPE_CHECKING
from collections.abc import Callable, Mapping, Sequence
import json
from pathlib import Path
import socket

from .cache import (
    cache_path,
    read_cache,
    refresh_actions,
    run_ids,
    select_states,
    snapshot,
    write_cache,
)
from .availability import existing_records
from .references import resolve_records
from .filtering import filter_templates, metadata_template_name
from .settings import get_limit
from .headers import read_header, is_archive
from .models import (
    JobObservation,
    JobRecord,
    ProcessReference,
    SubmissionRecord,
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
from ..log import TimeIt
from .scheduler_cache import query_snapshot, reference_key

if TYPE_CHECKING:
    from ..app import BaseApp
    from ..core.workflow import Workflow
    from ..submission.schedulers import Scheduler


@dataclass(slots=True)
class CollectionProgress:
    """Current rows and unfinished observations for a progressive table.

    Records belong to the collection invocation; consume updates immediately.
    Pending entries use object identities because unregistered parts can lack IDs.
    """

    records: list[SubmissionRecord]
    pending: frozenset[int]
    checking_activity: bool = False


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


@dataclass(slots=True)
class SchedulerQuery:
    """One scheduler query and every submission sharing its observation."""

    scheduler: Scheduler
    jobs: list[RegisteredJob]


@TimeIt.decorator
def check_identity(record: SubmissionRecord) -> None:
    """Verify the workflow header before trusting cached action-run identities.

    Reject missing stores, unsupported headers, and paths now belonging to another
    workflow. Reuse an unchanged header token where possible; otherwise refresh the
    saved workflow/template names and token on the supplied record. This check does
    not construct a workflow object.
    """
    metadata, token = read_header(
        record.workflow_path,
        record.workflow_header_token if record.workflow_name_from_metadata else None,
    )
    if metadata is not None:
        if metadata["creation_info"]["id"] != record.workflow_id:
            raise ValueError("Workflow at this path has been replaced")
        if isinstance(metadata.get("name"), str) and metadata["name"]:
            record.workflow_name = metadata["name"]
            record.workflow_name_from_metadata = True
        record.template_name = metadata_template_name(metadata)
    record.workflow_header_token = token


@TimeIt.decorator
def load_workflow(
    app: BaseApp,
    record: SubmissionRecord,
    loaded: dict[WorkflowKey, Workflow],
    progress: Callable[[str], None] | None = None,
) -> Workflow:
    """Load and verify a workflow once per path/ID pair within one collection.

    Reuse ``loaded`` across submission parts, recheck the workflow ID after loading,
    and refresh the record's workflow and template names. Loading errors propagate
    to the caller, which can mark the record inaccessible while retaining job references.
    """
    key = WorkflowKey(record.workflow_path, record.workflow_id)
    if key not in loaded:
        check_identity(record)
        if progress:
            progress(f"Loading workflow: {record.workflow_name}")
        workflow = app.Workflow(record.workflow_path)
        if workflow.id_ != record.workflow_id:
            raise ValueError("Workflow identity does not match submission record")
        loaded[key] = workflow
    workflow = loaded[key]
    record.template_name = workflow.template.name
    record.workflow_name = workflow.name
    record.workflow_name_from_metadata = True
    return workflow


@TimeIt.decorator
def hydrate(
    app: BaseApp,
    record: SubmissionRecord,
    loaded: dict[WorkflowKey, Workflow],
    progress: Callable[[str], None] | None = None,
) -> None:
    """Backfill saved job references for one legacy submission part.

    Load the workflow through the per-invocation cache, snapshot its jobscripts,
    and clear the legacy marker only after successful recovery. Errors propagate
    without inventing scheduler references for unreadable workflows.
    """
    workflow = load_workflow(app, record, loaded, progress)
    sub = workflow.submissions[record.submission_index]
    indices = sub._submission_parts[record.submission_part_id]
    record.jobs = [
        job_record(
            sub.jobscripts[i], inspect_scheduler=not is_archive(record.workflow_path)
        )
        for i in indices
    ]
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
    app.submission_logger.info(
        "Cannot inspect %s: %s", record.workflow_path, error, exc_info=True
    )


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


def query_context_key(app: BaseApp, job: JobRecord, scheduler: Scheduler) -> str:
    """Identify compatible status queries independently of submission settings.

    Directives, submission/cancel commands, and script-format options do not affect
    status queries. Retain other arguments conservatively, and normalize a queued
    scheduler's effective show command so omitted and explicit defaults can share
    a snapshot. Login hosts on the same configured machine share a query target.
    Cancellation continues to use the complete saved scheduler context.
    """
    submission_only = {
        "directives",
        "options",
        "submit_cmd",
        "del_cmd",
        "js_cmd",
        "login_nodes_cmd",
        "array_switch",
        "array_item_var",
        "cwd_switch",
        "shebang_executable",
    }
    args = {
        key: value
        for key, value in job.scheduler_args.items()
        if key not in submission_only
    }
    show_cmd = getattr(scheduler, "show_cmd", None)
    if isinstance(show_cmd, (list, tuple)):
        args["show_cmd"] = list(show_cmd)
    return json.dumps(
        {
            "scheduler_type": job.scheduler_type.lower(),
            "os_name": job.os_name.lower(),
            "machine": job.machine or app.config.get("machine") or socket.gethostname(),
            "query_args": args,
        },
        sort_keys=True,
    )


def scheduler_is_local(app: BaseApp, job: JobRecord) -> bool:
    """Allow local jobs or queued jobs on an explicitly shared configured machine.

    A foreign direct PID never denotes a local process, even if its numeric value
    happens to exist here. This predicate does not construct a scheduler adapter.
    """
    return job.hostname == socket.gethostname() or bool(
        isinstance(job.scheduler_reference, str)
        and job.machine
        and job.machine == app.config.get("machine")
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


@TimeIt.decorator
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


@TimeIt.decorator
def observe_activity(
    app: BaseApp,
    records: list[SubmissionRecord],
    progress: Callable[[str], None] | None = None,
    *,
    min_interval: int = 0,
    ignore_scheduler_interval: bool = False,
    no_update: bool = False,
) -> None:
    """Update records using one snapshot per compatible scheduler query target.

    Submission directives and submit/cancel commands do not split status queries.
    Deduplicate references across submissions and reuse each query's observation
    for all matching jobs. Different effective show commands or targets remain
    isolated. Malformed references fail individually instead of blocking the batch.
    Batch saved references across submission parts; known inactive records do not
    need queries. A live submitting process also keeps a record active. Scheduler
    failures produce unknown activity unless activity is independently known to be
    active, and are recorded without aborting observations of other contexts.
    Foreign jobs without a shared configured machine remain remotely unavailable;
    archives use saved observations only. Neither case inspects local PIDs or
    makes the workflow unreadable. ``progress`` receives transient query messages.
    Update submission records in memory; the caller controls registry persistence.
    Rate-limit snapshots are saved separately unless ``no_update`` is True.
    ``min_interval`` enables persistent rate limiting per query target. Cached
    snapshots retain their observation time and do not supply fresh queued/terminal
    parent evidence. New references absent from a cached snapshot remain unknown.
    """
    contexts: defaultdict[str, list[RegisteredJob]] = defaultdict(list)
    failed = set()
    observed_records = []
    for record in records:
        record.scheduler_cached = False
        for job in record.jobs:
            job.observation = None
    for record in records:
        if is_archive(record.workflow_path):
            observed_records.append(record)
            record.submitting = False
            record.activity = "unknown"
            record.scheduler_states = []
            record.scheduler_error = None
            record.scheduler_availability = "archived"
            continue
        if record.activity == "inactive":
            if any(not scheduler_is_local(app, job) for job in record.jobs):
                record.scheduler_availability = "remote"
            continue
        observed_records.append(record)
        local_jobs = not record.jobs or any(
            scheduler_is_local(app, job) for job in record.jobs
        )
        if not local_jobs:
            record.submitting = False
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
        record.scheduler_availability = "available"
        if not record.jobs:
            record.activity = (
                "active"
                if record.submitting
                else "unknown" if record.legacy else "inactive"
            )
            continue
        record.activity = "active" if record.submitting else "inactive"
        for job in record.jobs:
            if not scheduler_is_local(app, job):
                failed.add(record.local_id)
                record.scheduler_availability = "remote"
                continue
            try:
                contexts[context_key(job)].append(RegisteredJob(record, job))
            except (KeyError, TypeError, ValueError) as error:
                failed.add(record.local_id)
                record.scheduler_error = str(error)
    queries: dict[str, SchedulerQuery] = {}
    for items in contexts.values():
        try:
            scheduler = scheduler_for(app, items[0].job)
            query_key = query_context_key(app, items[0].job, scheduler)
            if query_key not in queries:
                queries[query_key] = SchedulerQuery(scheduler, [])
            queries[query_key].jobs.extend(items)
        except Exception as error:
            for item in items:
                record = item.submission
                failed.add(record.local_id)
                record.scheduler_error = str(error)
                record.scheduler_availability = "unavailable"
            app.submission_logger.info("Scheduler monitoring failed: %s", error)
    for query_key, query in queries.items():
        items = query.jobs
        try:
            refs = []
            for item in items:
                try:
                    refs.append(ResolvedJob(item, scheduler_ref(item.job)))
                except Exception as error:
                    record = item.submission
                    failed.add(record.local_id)
                    record.scheduler_error = str(error)
                    record.scheduler_availability = "unavailable"
            valid = list(
                {
                    json.dumps(to_json(item.reference)): item.reference
                    for item in refs
                    if item.reference is not None
                }.values()
            )
            result = (
                query_snapshot(
                    app,
                    query_key,
                    valid,
                    lambda: query.scheduler.get_job_state_info(js_refs=valid),
                    min_interval,
                    ignore_scheduler_interval,
                    progress,
                    items[0].job.scheduler_type,
                    no_update=no_update,
                )
                if valid
                else None
            )
            for resolved in refs:
                record, job = resolved.registration.submission, resolved.registration.job
                ref = resolved.reference
                key = str(ref[0]) if isinstance(ref, tuple) else ref
                cached = result is not None and result.cached and ref is not None
                record.scheduler_cached |= cached
                job.last_checked_at = result.observed_at if result else timestamp()
                if (
                    result
                    and ref is not None
                    and reference_key(ref) not in result.references
                ):
                    failed.add(record.local_id)
                    job.observation = None
                    job.last_observed_state = []
                    continue
                observed = (
                    result.states.get(key, JobObservation())
                    if result and key is not None
                    else JobObservation()
                )
                job.observation = replace(observed, fresh=not cached)
                job.last_observed_state = (
                    list(observed.elements.values())
                    if observed.elements
                    else [observed.state] if observed.state else []
                )
                if job.last_observed_state:
                    record.activity = "active"
                    record.scheduler_states.extend(job.last_observed_state)
                elif cached:
                    # An older absence cannot establish current inactivity.
                    failed.add(record.local_id)
        except Exception as error:
            for item in items:
                record = item.submission
                failed.add(record.local_id)
                record.scheduler_error = str(error)
                record.scheduler_availability = "unavailable"
                item.job.observation = None
            app.submission_logger.info("Scheduler monitoring failed: %s", error)
    # Already-inactive records were not queried. Preserve their timestamps rather
    # than making old history appear newly inactive on each show invocation.
    for record in observed_records:
        if record.local_id in failed and record.activity != "active":
            record.activity = "unknown"
        record.last_checked_at = timestamp()
        if record.activity == "inactive" and not record.inactive_at:
            record.inactive_at = record.last_checked_at


def _selected_records(
    records: list[SubmissionRecord], max_recent: int | None
) -> list[SubmissionRecord]:
    """Order active/unknown entries first and limit inactive history consistently.

    Use saved activity for initial placeholders, then fresh activity for observations
    and the final result. Scheduler collection still inspects all matching records.
    """
    return (
        sorted(
            (record for record in records if record.activity != "inactive"),
            key=lambda record: (
                record.activity == "active",
                sort_timestamp(record.submitted_at),
            ),
            reverse=True,
        )
        + sorted(
            (record for record in records if record.activity == "inactive"),
            key=recent_key,
            reverse=True,
        )[:max_recent]
    )


@TimeIt.decorator
def _collect_record(
    app: BaseApp,
    record: SubmissionRecord,
    loaded: dict[WorkflowKey, Workflow],
    include_summary: bool,
    no_update: bool,
    progress: Callable[[str], None] | None,
) -> None:
    """Inspect one row using a workflow cache owned exclusively by its worker."""
    try:
        check_identity(record)
        record.accessibility = "readable"
        record.detail_error = None
        if record.legacy:
            hydrate(app, record, loaded, progress)
        if not include_summary:
            return
        ids = run_ids(record)
        cache = read_cache(record)
        if ids and cache and all(i in cache.terminal_states for i in ids):
            summary = snapshot(cache.terminal_states, ids)
            summary.complete = True
            summary.observed_at = cache.observed_at or "unknown"
            if (
                not no_update
                and not is_archive(record.workflow_path)
                and not cache_path(record).exists()
            ):
                try:
                    write_cache(record, cache)
                except OSError as error:
                    summary.cache_error = str(error)
        else:
            selection = select_states(record, cache)
            if not selection.unresolved:
                summary = snapshot(selection.entries, ids)
            else:
                workflow = load_workflow(app, record, loaded, progress)
                if progress:
                    progress(f"Reading action states: {record.workflow_name}")
                summary = refresh_actions(
                    record,
                    workflow,
                    no_update or is_archive(record.workflow_path),
                    selection=selection,
                )
        record.last_action_summary = summary
        if (
            record.scheduler_availability in ("remote", "archived")
            and summary.complete
            and record.activity != "active"
        ):
            record.activity = "inactive"
            record.inactive_at = record.inactive_at or timestamp()
        for field in ("started_at", "finished_at"):
            if getattr(summary, field):
                setattr(record, field, getattr(summary, field))
    except Exception as error:
        note_failure(app, record, error)


@dataclass(slots=True)
class ReadyRecord:
    """Worker-owned observation ready to publish on the collecting thread."""

    original: SubmissionRecord
    observation: SubmissionRecord


@TimeIt.decorator
def _collect_selected(
    app: BaseApp,
    selected: list[SubmissionRecord],
    loaded: dict[WorkflowKey, Workflow],
    include_summary: bool,
    no_update: bool,
    progress: Callable[[str], None] | None,
    on_update: Callable[[CollectionProgress], None] | None,
) -> None:
    """Read up to four workflows concurrently, publishing rows as they become ready.

    Each workflow path belongs to one worker, which handles its submissions
    sequentially and owns its workflow/store cache. Workers mutate private record
    copies. Only the collecting thread publishes observations, invokes progress or
    rendering callbacks, and later updates the registry. Scheduler queries have
    already completed before this function is called.
    """
    groups: defaultdict[Path, list[SubmissionRecord]] = defaultdict(list)
    for record in selected:
        groups[Path(record.workflow_path).absolute()].append(record)
    pending = set(map(id, selected))

    def ready(record: SubmissionRecord) -> None:
        pending.discard(id(record))
        if on_update:
            on_update(CollectionProgress(selected, frozenset(pending)))

    if len(groups) <= 1:
        for record in selected:
            _collect_record(app, record, loaded, include_summary, no_update, progress)
            ready(record)
        return

    events: Queue[str | ReadyRecord | None] = Queue()

    @TimeIt.decorator
    def read_group(records: list[SubmissionRecord]) -> None:
        try:
            keys = {WorkflowKey(row.workflow_path, row.workflow_id) for row in records}
            worker_loaded = {key: value for key, value in loaded.items() if key in keys}
            for original in records:
                observation = deepcopy(original)
                _collect_record(
                    app,
                    observation,
                    worker_loaded,
                    include_summary,
                    no_update,
                    events.put if progress else None,
                )
                events.put(ReadyRecord(original, observation))
        finally:
            events.put(None)

    with ThreadPoolExecutor(max_workers=min(4, len(groups))) as executor:
        futures = [executor.submit(read_group, records) for records in groups.values()]
        unfinished = len(futures)
        while unfinished:
            event = events.get()
            if event is None:
                unfinished -= 1
            elif isinstance(event, str):
                if progress:
                    progress(event)
            else:
                for field in fields(SubmissionRecord):
                    setattr(
                        event.original, field.name, getattr(event.observation, field.name)
                    )
                ready(event.original)
        for future in futures:
            future.result()


@TimeIt.decorator
def collect(
    app: BaseApp,
    workflow_ref=None,
    ref_type=None,
    max_recent=None,
    no_update=False,
    include_summary: bool = True,
    template_names: Sequence[str] | str | None = None,
    progress: Callable[[str], None] | None = None,
    include_missing: bool = False,
    on_update: Callable[[CollectionProgress], None] | None = None,
    ignore_scheduler_interval: bool = False,
) -> list[SubmissionRecord]:
    """Collect active submissions and a limited recent inactive history.

    Resolve an optional workflow ID, registered name, or path, then apply exact
    template-name filtering before scheduler queries and the inactive-history limit.
    Missing workflow paths are hidden before querying schedulers or applying
    history limits unless ``include_missing`` is True. Their registry references
    remain available for inspection and cancellation. An explicit unregistered
    path can recover submission parts from its workflow.
    Active and unknown records precede inactive records and do not consume
    ``max_recent`` slots. None uses ``show_max_recent`` for the general overview,
    but includes all matching history when a workflow reference is supplied.

    Refresh action summaries only for selected records when ``include_summary`` is
    True, reusing terminal caches and a persistent run/parent index. Fresh scheduler
    queued/waiting observations can supply nonterminal states without loading EARs
    or workflows; running and missing/unknown parents use store reads. Inspection failures remain
    visible as accessibility information rather than hiding the submission.
    ``no_update`` prevents all registry/cache writes, including new scheduler
    snapshots. Existing scheduler snapshots may still be reused. Scheduler snapshots
    use ``show_scheduler_min_interval`` (20 seconds) unless explicitly bypassed.
    Otherwise merge observations with concurrent registry changes and retention.
    Return invocation-owned snapshots, without live workflow or scheduler objects.
    ``on_update`` receives initial placeholders, scheduler results, and each ready
    row. It performs no additional collection or scheduler queries. Read different
    workflows with at most four workers; submissions sharing a workflow remain
    sequential. Publish ready rows and call progress callbacks on the caller thread.
    """
    if max_recent is None and workflow_ref is None:
        max_recent = get_limit(app.config, "show_max_recent", 3)
    if max_recent is not None and (type(max_recent) is not int or max_recent < 0):
        raise ValueError("max_recent must be a non-negative integer")
    registry = SubmissionRegistry(app)
    records = resolve_records(registry.read().records, workflow_ref, ref_type)
    loaded: dict[WorkflowKey, Workflow] = {}
    if workflow_ref is not None and not records:
        # An explicit path may refer to history that has already been pruned.
        path = str(Path(workflow_ref).resolve())
        if progress:
            progress(f"Loading workflow: {path}")
        try:
            workflow = app.Workflow(path)
        except FileNotFoundError:
            if not include_missing:
                return []
            raise
        for sub in workflow.submissions:
            for part in sub._submission_parts:
                record = new_record(
                    path,
                    workflow.id_,
                    sub.index,
                    part,
                    workflow_name=workflow.name,
                    template_name=workflow.template.name,
                )
                record.local_id = -(len(records) + 1)
                record.legacy = True
                records.append(record)
        loaded[WorkflowKey(path, workflow.id_)] = workflow
    records = existing_records(records, include_missing)
    records = filter_templates(records, template_names)
    if on_update:
        initial = _selected_records(records, max_recent)
        on_update(CollectionProgress(initial, frozenset(map(id, initial)), True))
    for record in records:
        if record.legacy and record.activity != "inactive":
            try:
                hydrate(app, record, loaded, progress)
            except Exception as error:
                note_failure(app, record, error)
    min_interval = get_limit(app.config, "show_scheduler_min_interval", 20)
    if progress is None:
        observe_activity(
            app,
            records,
            min_interval=min_interval,
            ignore_scheduler_interval=ignore_scheduler_interval,
            no_update=no_update,
        )
    else:
        observe_activity(
            app,
            records,
            progress,
            min_interval=min_interval,
            ignore_scheduler_interval=ignore_scheduler_interval,
            no_update=no_update,
        )
    selected = _selected_records(records, max_recent)
    pending = {id(record) for record in selected}
    if on_update:
        on_update(CollectionProgress(selected, frozenset(pending)))
    _collect_selected(
        app, selected, loaded, include_summary, no_update, progress, on_update
    )
    # Remote/archive completion may have changed activity while reading states.
    selected = _selected_records(selected, max_recent)
    if not no_update:
        retained = registry.update([r for r in records if r.local_id >= 0])
        selected = [r for r in selected if r.local_id < 0 or r.local_id in retained]
    # These models are owned by this invocation; no live store/scheduler objects escape.
    return selected


@TimeIt.decorator
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
