"""Ordered plans and cursors bound reads to preceding/current actions, not future ones."""

from contextlib import nullcontext
from datetime import datetime, timezone
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import numpy as np
import pytest

from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.monitoring import active_runs
from hpcflow.sdk.monitoring.active_runs import (
    ActiveRunTracker,
    read_activity,
    track_run,
    note_changed_runs,
)
from hpcflow.sdk.monitoring.cache import refresh_actions, select_states, read_cache
from hpcflow.sdk.monitoring.models import JobRecord, JobObservation, SubmissionRecord


@pytest.fixture
def source(tmp_path):
    job = JobRecord(
        0,
        run_ids=list(range(12)),
        is_array=True,
        observation=JobObservation(elements={0: "running", 1: "pending"}),
    )
    record = SubmissionRecord(
        str(tmp_path), "workflow-id", "workflow", 0, "part", "", jobs=[job]
    )
    states = {i: EARStatus.submitted for i in range(12)}
    states.update({0: EARStatus.success, 1: EARStatus.running})
    stamp = datetime(2026, 11, 8, 12, tzinfo=timezone.utc)

    def read_runs(ids):
        return [
            SimpleNamespace(
                id_=i,
                task=SimpleNamespace(index=0, unique_name="task"),
                element=SimpleNamespace(index=100 + i // 6),
                element_iteration=SimpleNamespace(index=i % 6),
                element_action=SimpleNamespace(action_idx=0),
                index=0,
                status=states[i],
                start_time=stamp if states[i] is EARStatus.running else None,
                end_time=(
                    stamp
                    if states[i] in EARStatus.get_non_running_submitted_states()
                    else None
                ),
                skip_reason=SkipReason.LOOP_TERMINATION,
            )
            for i in ids
        ]

    workflow = SimpleNamespace(
        path=tmp_path,
        id_="workflow-id",
        _app=SimpleNamespace(package_name="hpcflow", submission_logger=Mock()),
        _store=SimpleNamespace(cache_ctx=nullcontext),
        submissions=[
            SimpleNamespace(
                jobscripts=[
                    SimpleNamespace(
                        is_array=True,
                        blocks=[
                            SimpleNamespace(
                                EAR_ID=np.array([list(range(6)), list(range(6, 12))]).T
                            )
                        ],
                    )
                ]
            )
        ],
        get_EARs_from_IDs=Mock(side_effect=read_runs),
    )
    active_runs.enable_tracking(workflow, 0)
    refresh_actions(record, workflow)  # Initial index bootstrap.
    workflow.get_EARs_from_IDs.reset_mock()
    tracker = ActiveRunTracker(workflow, 0, 0, 0)
    tracker.start([0])
    tracker.finish([0])
    tracker.start([1])
    return record, workflow, states, tracker


def activity(workflow, item=0):
    return read_activity(str(workflow.path), workflow.id_, 0, 0, item)


def test_reads_current_and_newly_completed_runs_without_polling_future_runs(source):
    record, workflow, states, tracker = source
    assert activity(workflow).active == 1
    assert activity(workflow).cursor == 1
    result = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1])
    assert result.display_counts == {
        "success": 1,
        "running": 1,
        "submitted": 4,
        "scheduler_pending": 6,
    }
    states[1] = EARStatus.success
    tracker.finish([1])
    states[2] = EARStatus.running
    tracker.start([2])
    workflow.get_EARs_from_IDs.reset_mock()
    refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 2])
    assert set(read_cache(record).terminal_states) == {0, 1}
    workflow.get_EARs_from_IDs.reset_mock()
    refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([2])


@pytest.mark.parametrize("cached_state", ["running", "pending", "errored", None])
def test_cached_scheduler_uses_fresh_cursors_without_reading_future_runs(
    source, cached_state
):
    record, workflow, states, tracker = source
    record.jobs[0].observation = JobObservation(state=cached_state, fresh=False)
    result = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1])
    assert result.display_counts == {"success": 1, "running": 1, "submitted": 10}
    states[1] = EARStatus.success
    tracker.finish([1])
    states[2] = EARStatus.running
    tracker.start([2])
    workflow.get_EARs_from_IDs.reset_mock()
    result = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 2])
    assert result.display_counts == {"success": 2, "running": 1, "submitted": 9}


def test_idle_item_still_inspects_uncached_visited_runs(source):
    record, workflow, states, tracker = source
    tracker.finish([1])
    assert not activity(workflow).active
    refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1])


def test_skip_notifications_override_unvisited_and_queued_inference(source):
    record, workflow, states, tracker = source
    with tracker.context():
        note_changed_runs(workflow, [4, 8])
    states[4] = states[8] = EARStatus.skipped
    result = refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1, 4, 8])
    assert result.display_counts["loop_skipped"] == 2
    assert {4, 8} <= read_cache(record).terminal_states.keys()
    workflow.get_EARs_from_IDs.reset_mock()
    refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1])


def test_plan_is_immutable_and_cursor_survives_idle_gaps(source):
    record, workflow, states, tracker = source
    plan = tracker.plan.read_bytes()
    tracker.finish([1])
    assert activity(workflow).active is None
    assert activity(workflow).cursor == 1
    tracker.start([2])
    assert activity(workflow).active == 2
    assert activity(workflow).cursor == 2
    assert tracker.plan.read_bytes() == plan
    assert not (tracker.directory / "visited").exists()
    assert not hasattr(tracker, "journal")


@pytest.mark.parametrize("damage", ["identity", "active", "length", "partial", "missing"])
def test_bad_or_missing_markers_fall_back_to_store_reads(source, damage):
    record, workflow, states, tracker = source
    data = json.loads(tracker.marker.read_bytes())
    if damage == "identity":
        data["workflow_id"] = "other"
    elif damage == "active":
        data["active_run_id"] = 5  # Does not match the run at the cursor.
    elif damage == "length":
        data["cursor"] = 100
    elif damage == "partial":
        tracker.plan.write_bytes(b"bad")
    elif damage == "missing":
        tracker.marker.unlink()
    if damage != "missing":
        tracker.marker.write_text(json.dumps(data))
    assert activity(workflow) is None
    refresh_actions(record, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with(list(range(1, 6)))


def test_ordinary_execution_context_cleans_active_id_on_failure(source, monkeypatch):
    record, workflow, states, tracker = source
    monkeypatch.setenv("HPCFLOW_JS_ELEM_IDX", "0")
    with pytest.raises(RuntimeError):
        with track_run(workflow, 0, 0, 2):
            assert activity(workflow).active == 2
            raise RuntimeError("execution failed")
    assert not activity(workflow).active
    assert activity(workflow).cursor == 2
    assert select_states(record, read_cache(record)).unresolved == [1, 2]


def test_nonarray_item_zero_uses_a_sequential_plan(source, monkeypatch):
    record, workflow, states, tracker = source
    workflow.path = workflow.path / "serial"
    workflow.submissions[0].jobscripts[0].is_array = False
    active_runs.enable_tracking(workflow, 0)
    monkeypatch.setenv("HPCFLOW_JS_ELEM_IDX", "99")
    tracker = ActiveRunTracker.for_workflow(workflow, 0, 0)
    assert tracker.run_ids == tuple(range(12))
    tracker.start([0])
    tracker.finish([0])
    tracker.start([1])
    assert activity(workflow).active == 1
    tracker.clear()
    assert activity(workflow).active is None
    assert activity(workflow).cursor == 1


def test_marker_failure_invalidates_old_evidence_without_failing_execution(
    source, monkeypatch
):
    record, workflow, states, tracker = source
    monkeypatch.setattr(active_runs.os, "replace", Mock(side_effect=OSError("read only")))
    tracker.start([2])
    assert not tracker.marker.exists()
    workflow._app.submission_logger.warning.assert_called_once()
    assert select_states(record, read_cache(record)).unresolved == list(range(1, 6))


def test_corrupt_changed_log_forces_safe_store_fallback(source):
    record, workflow, states, tracker = source
    (tracker.directory / "changed.jsonl").write_bytes(b"partial")
    assert select_states(record, read_cache(record)).unresolved == list(range(1, 12))


def test_inactive_items_do_not_revisit_future_runs(source):
    record, workflow, states, tracker = source
    record.activity = "inactive"
    record.jobs[0].observation = None
    refresh_actions(record, workflow)
    # Item 1 retains its initial cursor, proving it never started.
    workflow.get_EARs_from_IDs.assert_called_once_with([1])


def test_older_submission_cannot_enable_partial_tracking_mid_execution(
    source, monkeypatch
):
    record, workflow, states, tracker = source
    (tracker.directory / "enabled.json").unlink()
    monkeypatch.setenv("HPCFLOW_JS_ELEM_IDX", "0")
    assert ActiveRunTracker.for_workflow(workflow, 0, 0) is None
    assert select_states(record, read_cache(record)).unresolved == list(range(1, 6))


@pytest.mark.parametrize("starts", [[3], [0], [1, 2]])
def test_out_of_order_or_concurrent_starts_force_fallback(source, starts):
    record, workflow, states, tracker = source
    tracker.start(starts)
    assert not tracker.marker.exists()
    assert select_states(record, read_cache(record)).unresolved == list(range(1, 6))


def test_real_workflow_execution_wrapper_tracks_and_cleans_up(tmp_path, monkeypatch):
    from hpcflow.app import app as hf

    workflow = hf.Workflow.from_template_data(
        template_name="active-pointer",
        path=tmp_path,
        tasks=[
            hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": 100})
        ],
    )
    submission = workflow.add_submission(status=False)
    job = submission.jobscripts[0]
    run_id = int(job.all_EAR_IDs[0])

    def execute(self, submission_idx, block_act_key, run_ID):
        assert (
            read_activity(str(self.path), self.id_, submission_idx, job.index, 0).active
            == run_ID
        )
        self.set_EAR_skip({run_ID: SkipReason.TASK_CONDITION_NOT_MET})
        raise RuntimeError("execution failure")

    monkeypatch.setattr(type(workflow), "_execute_run", execute)
    with pytest.raises(RuntimeError, match="execution failure"):
        workflow.execute_run(0, (job.index, 0, 0), run_id)
    snapshot = read_activity(str(workflow.path), workflow.id_, 0, job.index, 0)
    assert snapshot.active is None and snapshot.cursor == 0
    assert active_runs.read_changed_runs(str(workflow.path), workflow.id_, 0) == set()
    assert workflow.get_EAR_skipped(run_id) == SkipReason.TASK_CONDITION_NOT_MET.value


def test_generated_combined_script_contains_tracking_and_compiles(tmp_path, monkeypatch):
    from hpcflow.app import app as hf

    workflow = hf.Workflow.from_template_data(
        template_name="active-combined",
        path=tmp_path,
        tasks=[
            hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": 100})
        ],
    )
    job = workflow.add_submission(status=False).jobscripts[0]
    monkeypatch.setattr(
        job, "_resources", SimpleNamespace(combine_scripts=True, write_app_logs=False)
    )
    snippet = tmp_path / "snippet.py"
    snippet.write_text("def snippet(*args, **kwargs):\n    return {}\n")
    script, *_ = job.compose_combined_script([[("action", snippet, False)]])
    compile(script, "combined.py", "exec")
    assert "monitoring_tracker.start(" in script
    assert "monitoring_tracker.finish(" in script
    assert "monitoring_stack.callback(monitoring_tracker.clear)" in script


def test_large_item_reads_its_marker_once_for_many_future_runs(source, monkeypatch):
    from hpcflow.sdk.monitoring import run_index
    from hpcflow.sdk.monitoring.run_index import RunIndex, RunMetadata, scheduler_entries

    record, workflow, states, tracker = source
    count = 20_000
    workflow.path = workflow.path / "large"
    record.workflow_path = str(workflow.path)
    workflow.submissions[0].jobscripts[0].blocks = [
        SimpleNamespace(EAR_ID=np.arange(count).reshape(count, 1))
    ]
    active_runs.enable_tracking(workflow, 0)
    tracker = ActiveRunTracker(workflow, 0, 0, 0)
    tracker.start([0])
    tracker.finish([0])
    tracker.start([1])
    record.jobs[0].run_ids = list(range(count))
    record.jobs[0].observation = JobObservation(state="running")
    index = RunIndex(
        record.workflow_id,
        0,
        {i: RunMetadata(0, "task", 0, i, 0, 0, 0, 0) for i in range(count)},
    )
    reader = Mock(wraps=run_index.read_activity)
    monkeypatch.setattr(run_index, "read_activity", reader)
    entries = scheduler_entries(record, index, list(range(1, count)))
    assert (
        len(entries) == count - 2
    )  # Run 1 is current; all other requested IDs are future.
    assert 1 not in entries
    assert all(entry.state is EARStatus.submitted for entry in entries.values())
    reader.assert_called_once()


def test_cursor_uses_explicit_order_not_sorted_run_ids(source):
    record, workflow, states, tracker = source
    workflow.path = workflow.path / "non-numeric-order"
    workflow.submissions[0].jobscripts[0].blocks = [
        SimpleNamespace(EAR_ID=np.array([[30], [10], [20]]))
    ]
    active_runs.enable_tracking(workflow, 0)
    tracker = ActiveRunTracker(workflow, 0, 0, 0)
    tracker.start([30])
    tracker.finish([30])
    tracker.start([10])
    snapshot = activity(workflow)
    assert snapshot.run_ids == (30, 10, 20)
    assert snapshot.cursor == 1 and snapshot.active == 10
    assert not snapshot.is_future(30) and not snapshot.is_future(10)
    assert snapshot.is_future(20)


def test_execution_plan_matches_shell_combined_and_array_loop_order():
    from hpcflow.sdk.monitoring.active_runs import execution_plans

    job = SimpleNamespace(
        is_array=False,
        resources=SimpleNamespace(combine_scripts=False),
        blocks=[
            SimpleNamespace(EAR_ID=np.array([[30, 11], [10, -1], [20, 12]])),
            SimpleNamespace(EAR_ID=np.array([[90, 80]])),
        ],
    )
    assert execution_plans(job) == {0: (30, 10, 20, 11, 12, 90, 80)}
    job.resources.combine_scripts = True
    assert execution_plans(job) == {0: (30, 11, 10, 20, 12, 90, 80)}
    job.is_array = True
    job.blocks = job.blocks[:1]
    assert execution_plans(job) == {0: (30, 10, 20), 1: (11, 12)}


def test_plan_and_cursor_are_tied_by_fingerprint(source):
    record, workflow, states, tracker = source
    data = json.loads(tracker.plan.read_bytes())
    data["run_ids"] = list(reversed(data["run_ids"]))
    tracker.plan.write_text(json.dumps(data))
    assert activity(workflow) is None
    assert select_states(record, read_cache(record)).unresolved == list(range(1, 6))


def test_legacy_history_protocol_is_not_used_as_cursor_evidence(source, monkeypatch):
    record, workflow, states, tracker = source
    enabled = tracker.directory / "enabled.json"
    data = json.loads(enabled.read_bytes())
    data["schema_version"] = 1
    enabled.write_text(json.dumps(data))
    monkeypatch.setenv("HPCFLOW_JS_ELEM_IDX", "0")
    assert ActiveRunTracker.for_workflow(workflow, 0, 0) is None
    assert select_states(record, read_cache(record)).unresolved == list(range(1, 6))


def test_reenabling_does_not_reset_progress_or_rewrite_plan(source):
    record, workflow, states, tracker = source
    before = tracker.marker.read_bytes(), tracker.plan.read_bytes()
    active_runs.enable_tracking(workflow, 0)
    assert before == (tracker.marker.read_bytes(), tracker.plan.read_bytes())


def test_shard_boundaries_and_separate_lock_temporary_directories(tmp_path):
    paths = {
        item: active_runs._paths(tmp_path, 0, item) for item in (0, 499, 500, 999, 1000)
    }
    assert paths[0][0].parent == paths[499][0].parent
    assert paths[500][0].parent == paths[999][0].parent
    assert len({paths[item][0].parent for item in (0, 500, 1000)}) == 3
    assert all(
        marker.parent.name == "markers" and plan.parent.name == "plans"
        for marker, plan in paths.values()
    )
    assert active_runs._paths(tmp_path, 1, 0)[0].parent != paths[0][0].parent
    # Parent fanout is bounded even above 256 job/item-shard buckets.
    marker, _ = active_runs._paths(tmp_path, 65_536, 65_536 * 500)
    assert marker.parts.count("children") == 6


def test_many_items_keep_every_directory_below_500_entries(tmp_path):
    count = 1001
    block = SimpleNamespace(EAR_ID=np.arange(count).reshape(1, count))
    job = SimpleNamespace(is_array=True, blocks=[block])
    workflow = SimpleNamespace(
        path=tmp_path,
        id_="large-shards",
        _app=SimpleNamespace(submission_logger=Mock()),
        submissions=[SimpleNamespace(jobscripts=[job])],
    )
    active_runs.enable_tracking(workflow, 0)
    temporary_parents = set()
    for item in range(count):
        tracker = ActiveRunTracker(workflow, 0, 0, item)
        tracker.start([item])
        assert tracker.lock.parent.name == "locks"
        temporary_parents.add(tracker.marker.parent.parent / "temporary")
    root = active_runs.activity_directory(str(workflow.path), 0)
    for directory in [root, *(path for path in root.rglob("*") if path.is_dir())]:
        assert len(list(directory.iterdir())) <= 500, directory
    assert len(temporary_parents) == 3
    assert not any(path.name.startswith(".active-") for path in root.rglob("*"))
    assert read_activity(str(workflow.path), workflow.id_, 0, 0, 1000).active == 1000


def test_flat_cursor_layout_remains_readable_and_writable(source):
    record, workflow, states, tracker = source
    old_marker = tracker.directory / tracker.marker.name
    old_plan = tracker.directory / "plans" / tracker.plan.name
    old_plan.parent.mkdir(parents=True)
    tracker.marker.replace(old_marker)
    tracker.plan.replace(old_plan)
    assert activity(workflow).cursor == 1
    continued = ActiveRunTracker(workflow, 0, 0, 0)
    assert continued.marker == old_marker
    continued.start([2])
    assert activity(workflow).cursor == 2
    assert activity(workflow).active == 2


def test_sharded_staging_reuses_a_killed_writers_file(source):
    record, workflow, states, tracker = source
    temporary = (
        tracker.marker.parent.parent
        / "temporary"
        / "markers"
        / f"{tracker.marker.name}.tmp"
    )
    temporary.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_bytes(b"incomplete previous update")
    tracker.start([2])
    assert activity(workflow).active == 2
    assert not temporary.exists()
