"""Scheduler-derived run summaries avoid repeated EAR and workflow-store reads."""

from contextlib import nullcontext
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import Mock
import json
import logging
import socket
from zipfile import ZipFile

import numpy as np
import pytest
import zstandard as zstd

from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.monitoring.cache import (
    cache_path,
    read_cache,
    refresh_actions,
    select_states,
)
from hpcflow.sdk.monitoring.models import (
    JobObservation,
    JobRecord,
    SubmissionRecord,
    to_json,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json
from hpcflow.sdk.monitoring.run_index import index_path, read_index, scheduler_entries
from hpcflow.sdk.monitoring.status import collect, observe_activity
from hpcflow.sdk.submission.enums import JobscriptElementState


@pytest.fixture
def source(tmp_path):
    record = SubmissionRecord(
        str(tmp_path / "workflow"),
        "id",
        "workflow",
        0,
        "part",
        "",
        local_id=0,
        activity="active",
        accessibility="readable",
        jobs=[
            JobRecord(
                0,
                "slurm",
                "posix",
                hostname=socket.gethostname(),
                scheduler_reference="100",
                is_array=True,
                run_ids=list(range(6)),
            )
        ],
    )
    states = {
        0: EARStatus.success,
        1: EARStatus.submitted,
        2: EARStatus.submitted,
        3: EARStatus.submitted,
        4: EARStatus.error,
        5: EARStatus.running,
    }

    def get_runs(ids):
        result = []
        for run_id in ids:
            state = states[run_id]
            time = datetime(2026, 11, 7, 12, tzinfo=timezone.utc)
            terminal = state in EARStatus.get_non_running_submitted_states()
            result.append(
                SimpleNamespace(
                    id_=run_id,
                    status=state,
                    start_time=time if terminal or state is EARStatus.running else None,
                    end_time=time if terminal else None,
                    task=SimpleNamespace(index=0, unique_name="compute"),
                    element=SimpleNamespace(index=100 + run_id // 2),
                    element_iteration=SimpleNamespace(index=0),
                    element_action=SimpleNamespace(action_idx=run_id % 2),
                    index=0,
                    skip_reason=(
                        SkipReason.TASK_CONDITION_NOT_MET
                        if state is EARStatus.skipped
                        else None
                    ),
                )
            )
        return result

    workflow = SimpleNamespace(
        _store=SimpleNamespace(cache_ctx=nullcontext),
        submissions=[
            SimpleNamespace(
                jobscripts=[
                    SimpleNamespace(
                        blocks=[SimpleNamespace(EAR_ID=np.array([[0, 2, 4], [1, 3, 5]]))]
                    )
                ]
            )
        ],
        get_EARs_from_IDs=Mock(side_effect=get_runs),
        get_all_EARs=Mock(
            side_effect=AssertionError("Must not enumerate all workflow EARs")
        ),
    )
    record.jobs[0].observation = JobObservation(elements={0: "running", 1: "pending"})
    return record, workflow, states


@pytest.mark.parametrize("indexed", [False, True])
@pytest.mark.parametrize("array", [False, True])
@pytest.mark.parametrize(
    "parent_state,interrupted",
    [
        (None, True),
        ("finished", True),
        ("cancelled", True),
        ("errored", True),
        ("running", False),
        ("pending", False),
        ("waiting", False),
        ("configuring", False),
        ("unavailable", False),
    ],
)
def test_interrupted_parent_in_active_submission(
    source, indexed, array, parent_state, interrupted
):
    record, workflow, _ = source
    if indexed:
        refresh_actions(record, workflow)
    job = record.jobs[0]
    job.is_array = array
    job.observation = (
        None
        if parent_state == "unavailable"
        else (
            JobObservation(
                elements={
                    0: "running",
                    1: "pending",
                    **({2: parent_state} if parent_state else {}),
                }
            )
            if array
            else JobObservation(state=parent_state)
        )
    )

    summary = refresh_actions(record, workflow)

    assert record.activity == "active"
    # Once indexed, pending parents derive Q without reading individual states.
    expected = (
        "scheduler_pending"
        if indexed and parent_state in ("pending", "waiting", "configuring")
        else ("interrupted" if interrupted else "running")
    )
    assert summary.display_counts.get(expected, 0) >= 1
    assert summary.display_counts.get("interrupted", 0) == int(interrupted)
    assert 5 not in read_cache(record).terminal_states
    assert not summary.complete
    assert not summary.finished_at


@pytest.mark.parametrize("parent_state", [None, "finished", "running", "pending"])
def test_cached_parent_requires_actual_run_reads_without_interruption(
    source, parent_state
):
    record, workflow, _ = source
    refresh_actions(record, workflow)
    record.jobs[0].observation = JobObservation(state=parent_state, fresh=False)
    workflow.get_EARs_from_IDs.reset_mock()
    summary = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 2, 3, 5])
    assert summary.display_counts.get("running") == 1
    assert "scheduler_pending" not in summary.display_counts
    assert "interrupted" not in summary.display_counts


def test_first_scan_then_running_and_finished_parent_elements_are_read(source):
    record, workflow, states = source
    refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with(list(range(6)))
    assert len(read_index(record).runs) == 6
    assert read_index(record).runs[3].jobscript_element == 1
    assert read_index(record).runs[3].element == 101  # Not the scheduler index.
    assert set(read_cache(record).terminal_states) == {0, 4}

    workflow.get_EARs_from_IDs.reset_mock()
    states[1] = EARStatus.running
    states[5] = EARStatus.success
    result = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 5])
    assert result.display_counts == {
        "success": 2,
        "error": 1,
        "running": 1,
        "scheduler_pending": 2,
    }
    assert not result.complete and not result.finished_at
    assert set(read_cache(record).terminal_states) == {0, 4, 5}

    # A previously queued element leaves the queue: read its actual terminal states.
    record.jobs[0].observation = JobObservation(elements={0: "running"})
    states.update({2: EARStatus.skipped, 3: EARStatus.success})
    workflow.get_EARs_from_IDs.reset_mock()
    result = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 2, 3])
    assert result.display_counts["condition_skipped"] == 1
    assert (
        read_cache(record).terminal_states[2].skip_reason
        is SkipReason.TASK_CONDITION_NOT_MET
    )
    workflow.get_all_EARs.assert_not_called()


def test_all_cached_or_scheduled_runs_do_not_open_workflow(source, tmp_path):
    record, workflow, states = source
    states[1] = EARStatus.success
    states[5] = EARStatus.success
    refresh_actions(record, workflow)
    atomic_json(
        tmp_path / "workflow" / ".zattrs",
        {"creation_info": {"id": "id"}, "name": "workflow"},
    )
    scheduler = Mock(
        get_job_state_info=Mock(
            return_value={
                "100": {
                    0: JobscriptElementState.running,
                    1: JobscriptElementState.pending,
                }
            }
        )
    )
    app = SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy",
        config={"show_max_recent": 3},
        get_scheduler=Mock(return_value=scheduler),
        Workflow=Mock(side_effect=AssertionError("Must not open workflow")),
        submission_logger=logging.getLogger(__name__),
    )
    with SubmissionRegistry(app).transaction() as registry:
        registry.records = [record]
    workflow.get_EARs_from_IDs.reset_mock()
    (result,) = collect(app)
    assert result.last_action_summary.display_counts["scheduler_pending"] == 2
    app.Workflow.assert_not_called()
    workflow.get_EARs_from_IDs.assert_not_called()
    scheduler.get_job_state_info.assert_called_once_with(js_refs=["100"])
    stored = json.loads(SubmissionRegistry(app).path.read_text())
    assert "observation" not in stored["records"][0]["jobs"][0]


@pytest.mark.parametrize(
    "observation",
    [
        None,
        JobObservation(),
        JobObservation(state="finished"),
        JobObservation(elements={2: "errored"}),
    ],
)
def test_unknown_missing_or_terminal_scheduler_states_require_store_reads(
    source, observation
):
    record, workflow, states = source
    refresh_actions(record, workflow)
    record.jobs[0].observation = observation
    workflow.get_EARs_from_IDs.reset_mock()
    refresh_actions(record, workflow, no_update=True)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 2, 3, 5])


def test_scheduler_failures_invalidate_previous_element_observations(source, tmp_path):
    record, workflow, states = source
    refresh_actions(record, workflow)
    scheduler = Mock(get_job_state_info=Mock(side_effect=OSError("unavailable")))
    app = SimpleNamespace(
        config={},
        get_scheduler=Mock(return_value=scheduler),
        submission_logger=logging.getLogger(__name__),
    )
    observe_activity(app, [record])
    assert record.jobs[0].observation is None
    assert record.activity == "unknown"
    assert select_states(record, read_cache(record)).unresolved == [1, 2, 3, 5]


def test_index_and_terminal_cache_are_read_only_with_no_update(source):
    record, workflow, states = source
    refresh_actions(record, workflow, no_update=True)
    assert not index_path(record).exists() and not cache_path(record).exists()
    refresh_actions(record, workflow)
    before = index_path(record).read_bytes(), cache_path(record).read_bytes()
    states[5] = EARStatus.success
    refresh_actions(record, workflow, no_update=True)
    assert before == (index_path(record).read_bytes(), cache_path(record).read_bytes())


@pytest.mark.parametrize("corruption", ["invalid", "identity", "coordinates", "parent"])
def test_bad_index_falls_back_and_is_rebuilt(source, corruption):
    record, workflow, states = source
    refresh_actions(record, workflow)
    payload = json.loads(
        zstd.ZstdDecompressor().decompress(index_path(record).read_bytes())
    )
    if corruption == "identity":
        payload["workflow_id"] = "other"
    elif corruption == "coordinates":
        payload["runs"]["1"]["element"] = -1
    elif corruption == "parent":
        payload["runs"]["1"]["jobscript_index"] = 99
    index_path(record).write_bytes(
        b"invalid"
        if corruption == "invalid"
        else zstd.ZstdCompressor().compress(json.dumps(payload).encode())
    )
    selection = select_states(record, read_cache(record))
    assert 1 in selection.unresolved
    refresh_actions(record, workflow)
    assert read_index(record).runs[1].jobscript_index == 0


def test_running_parent_requires_lookup_and_only_queued_parent_is_derived(source):
    record, workflow, states = source
    refresh_actions(record, workflow)
    assert scheduler_entries(record, read_index(record), [1]) == {}
    derived = scheduler_entries(record, read_index(record), [2])[2]
    assert derived.state is EARStatus.submitted
    assert derived.display_state == "scheduler_pending"
    assert "display_state" not in to_json(derived)
    assert derived.finished_at == ""


def test_index_tracks_elements_across_blocks_and_missing_run_ids(source):
    record, workflow, states = source
    workflow.submissions[0].jobscripts[0].blocks = [
        SimpleNamespace(EAR_ID=np.array([[0, 2, -1]])),
        SimpleNamespace(EAR_ID=np.array([[1, 3, 4], [-1, -1, 5]])),
    ]
    refresh_actions(record, workflow)
    index = read_index(record)
    assert [index.runs[i].jobscript_element for i in range(6)] == [0, 0, 1, 1, 2, 2]


@pytest.mark.parametrize("inactive", [False, True])
def test_old_scheduler_summary_is_unknown_when_parent_activity_is_not_current(
    source, inactive
):
    from hpcflow.sdk.monitoring.action_render import display_summary

    record, workflow, states = source
    record.jobs[0].observation.elements[2] = "running"
    refresh_actions(record, workflow)
    result = refresh_actions(record, workflow)
    assert result.display_counts["scheduler_pending"] == 2
    view = display_summary(result, inactive=inactive, unknown=not inactive)
    assert view.display_counts["unknown"] == (2 if inactive else 3)
    assert "scheduler_running" not in view.display_counts
    assert "scheduler_pending" not in view.display_counts
    assert view.display_counts["error"] == 1


def test_archive_index_is_readable_but_does_not_reuse_saved_scheduler_evidence(
    source, tmp_path
):
    record, workflow, states = source
    refresh_actions(record, workflow)
    archive_path = tmp_path / "workflow.zip"
    with ZipFile(archive_path, "w") as archive:
        archive.writestr(
            "artifacts/submissions/0/tmp/run_index.json.zst",
            index_path(record).read_bytes(),
        )
        archive.writestr(
            "artifacts/submissions/0/tmp/terminal_states.json.zst",
            cache_path(record).read_bytes(),
        )
    record.workflow_path = str(archive_path)
    before = archive_path.read_bytes()
    assert len(read_index(record).runs) == 6
    app = SimpleNamespace(config={}, submission_logger=logging.getLogger(__name__))
    observe_activity(app, [record])
    assert record.jobs[0].observation is None
    workflow.get_EARs_from_IDs.reset_mock()
    refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 2, 3, 5])
    assert archive_path.read_bytes() == before


def test_index_merges_metadata_from_other_submission_parts(source):
    from concurrent.futures import ThreadPoolExecutor
    from hpcflow.sdk.monitoring.run_index import write_index

    record, workflow, states = source
    refresh_actions(record, workflow)
    runs = read_index(record).runs
    index_path(record).unlink()
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(write_index, record, {key: runs[key] for key in ids})
            for ids in ([0, 1, 2], [3, 4, 5])
        ]
        for future in futures:
            future.result()
    assert read_index(record).runs == runs


def test_no_new_terminal_states_do_not_rewrite_large_terminal_cache(source, monkeypatch):
    from hpcflow.sdk.monitoring import cache

    record, workflow, states = source
    refresh_actions(record, workflow)
    writer = Mock(side_effect=AssertionError("Unchanged cache must not be rewritten"))
    monkeypatch.setattr(cache, "write_cache", writer)
    refresh_actions(record, workflow)
    writer.assert_not_called()


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_real_workflow_parent_index_and_zero_warm_ear_reads_for_queued_jobs(
    tmp_path, store, monkeypatch
):
    from hpcflow.app import app as hf
    from hpcflow.sdk.monitoring.registry import job_record

    task = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[hf.ValueSequence(path="resources.any.resources_id", values=[0, 1])],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="scheduler-index", path=tmp_path, tasks=[task], store=store
    )
    submission = workflow.add_submission(status=False)
    record = SubmissionRecord(
        str(workflow.path),
        workflow.id_,
        workflow.name,
        0,
        "part",
        "",
        jobs=[job_record(job, inspect_scheduler=False) for job in submission.jobscripts],
    )
    for job in record.jobs:
        job.observation = JobObservation(state="pending")
    refresh_actions(record, workflow)
    assert set(read_index(record).runs) == set(submission.all_EAR_IDs)
    forbidden = Mock(side_effect=AssertionError("Warm observations must not load EARs"))
    monkeypatch.setattr(type(workflow), "get_EARs_from_IDs", forbidden)
    result = refresh_actions(record, workflow)
    assert result.display_counts == {
        "scheduler_pending": len(list(submission.all_EAR_IDs))
    }
    forbidden.assert_not_called()


def test_foreign_scheduler_never_reuses_local_observations(source):
    record, workflow, states = source
    refresh_actions(record, workflow)
    record.jobs[0].hostname = "another-machine"
    scheduler = Mock(side_effect=AssertionError("Must not query foreign scheduler"))
    app = SimpleNamespace(
        config={}, get_scheduler=scheduler, submission_logger=logging.getLogger(__name__)
    )
    observe_activity(app, [record])
    assert record.jobs[0].observation is None
    assert record.activity == "unknown"
    assert select_states(record, read_cache(record)).unresolved == [1, 2, 3, 5]
    scheduler.assert_not_called()


def test_index_write_failure_keeps_summary_and_terminal_cache(source, monkeypatch):
    from hpcflow.sdk.monitoring import run_index

    record, workflow, states = source
    monkeypatch.setattr(
        run_index, "atomic_bytes", Mock(side_effect=PermissionError("index read only"))
    )
    result = refresh_actions(record, workflow)
    assert result.counts["success"] == 1
    assert "index read only" in result.cache_error
    assert set(read_cache(record).terminal_states) == {0, 4}
    assert not index_path(record).exists()


@pytest.mark.parametrize("view", ["counts", "tasks", "detail", "auto"])
def test_scheduler_states_render_and_roll_up_in_both_columns(source, view):
    from io import StringIO
    from rich.console import Console
    from hpcflow.sdk.monitoring.columns import prepare_context
    from hpcflow.sdk.monitoring.render import render

    record, workflow, states = source
    states[5] = EARStatus.success
    refresh_actions(record, workflow)
    record.last_action_summary = refresh_actions(record, workflow)
    context = prepare_context(
        [record], ["actions", "elements"], False, False, False, view=view
    )
    assert context.elements(record).counts == {
        "submitted": 1,
        "scheduler_pending": 1,
        "error": 1,
    }
    output = StringIO()
    render(
        [record],
        columns=["actions", "elements"],
        view=view,
        console=Console(file=output, width=180),
    )
    assert "Q" in output.getvalue()


def test_running_whole_job_reads_actual_states_after_bootstrap(source):
    record, workflow, states = source
    refresh_actions(record, workflow)
    record.jobs[0].observation = JobObservation(state="running")
    states[1] = EARStatus.success
    states[2] = EARStatus.running
    workflow.get_EARs_from_IDs.reset_mock()
    result = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 2, 3, 5])
    assert result.display_counts == {
        "success": 2,
        "running": 2,
        "submitted": 1,
        "error": 1,
    }
    assert "scheduler_running" not in result.display_counts


def test_legacy_running_parent_snapshots_render_as_unknown():
    from hpcflow.sdk.monitoring.action_render import action_text
    from hpcflow.sdk.monitoring.models import (
        ActionSummary,
        ElementGroup,
        IterationActionState,
    )
    from hpcflow.sdk.monitoring.states import ACTION_STATES

    old = ActionSummary(
        display_counts={"scheduler_running": 2, "scheduler_pending": 1},
        groups=[
            ElementGroup(
                "task", (IterationActionState(0, 0, 0, "scheduler_running"),), [0, 1]
            )
        ],
    )
    assert "?:2" in action_text(old, full=False).plain
    assert "scheduler_running" not in ACTION_STATES
