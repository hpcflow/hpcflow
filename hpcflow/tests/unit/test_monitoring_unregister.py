"""Explicit ID reuse, registry isolation, and concurrent monitoring protection."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
import logging
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_unregister_CLI
from hpcflow.sdk.monitoring.models import (
    JobRecord,
    ProcessIdentity,
    RegistryData,
    SubmissionRecord,
    to_json,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry


@pytest.fixture
def app(tmp_path):
    return SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy",
        config={
            "known_submissions_max_inactive": 100,
            "known_submissions_max_unknown": 10,
        },
        submission_logger=logging.getLogger(__name__),
        get_info=lambda: {},
        Workflow=Mock(side_effect=AssertionError("Unregister must not load a workflow")),
        get_scheduler=Mock(
            side_effect=AssertionError("Unregister must not query or cancel jobs")
        ),
    )


def records(tmp_path, count):
    return [
        SubmissionRecord(
            str(tmp_path / "workflow"),
            "workflow-id",
            "workflow",
            i,
            f"part-{i}",
            "2026-11-07T12:00:00Z",
            local_id=i,
            activity="active",
            jobs=[JobRecord(0, scheduler_reference=f"job-{i}")],
        )
        for i in range(count)
    ]


def seed(app, records):
    registry = SubmissionRegistry(app)
    with registry.transaction() as data:
        data.records = records
        data.next_local_id = max((r.local_id for r in records), default=-1) + 1
    return registry


def submission(tmp_path):
    return SimpleNamespace(
        workflow=SimpleNamespace(
            path=tmp_path / "new-workflow", id_="new-id", name="new-workflow"
        ),
        index=0,
    )


def test_unregister_only_removes_requested_part_and_preserves_workflow(app, tmp_path):
    original = records(tmp_path, 3)
    registry = seed(app, original)
    path = tmp_path / "workflow"
    path.mkdir()
    marker = path / "still-running.txt"
    marker.write_text("preserve")
    removed = registry.unregister(1)
    assert removed == original[1]
    stored = registry.read()
    assert [r.local_id for r in stored.records] == [0, 2]
    assert stored.records[0].jobs == original[0].jobs
    assert stored.records[1].jobs == original[2].jobs
    assert stored.free_local_ids == {1: 1}
    assert marker.read_text() == "preserve"
    app.Workflow.assert_not_called()
    app.get_scheduler.assert_not_called()


def test_freed_ids_persist_and_are_reused_lowest_first(app, tmp_path):
    registry = seed(app, records(tmp_path, 3))
    registry.unregister(2)
    registry.unregister(0)
    reopened = SubmissionRegistry(app)
    assert reopened.begin(submission(tmp_path), "new-part-1") == 0
    assert reopened.begin(submission(tmp_path), "new-part-2") == 2
    assert reopened.begin(submission(tmp_path), "new-part-3") == 3
    assert reopened.read().next_local_id == 4
    assert reopened.read().free_local_ids == {}


def test_automatic_pruning_does_not_free_ids(app, tmp_path):
    original = records(tmp_path, 3)
    for r in original:
        r.activity = "inactive"
    app.config["known_submissions_max_inactive"] = 0
    registry = seed(app, original)
    assert registry.read().records == []
    assert registry.read().free_local_ids == {}
    assert registry.begin(submission(tmp_path), "new-part") == 3


def test_stale_observations_cannot_overwrite_a_reused_id(app, tmp_path):
    registry = seed(app, records(tmp_path, 1))
    stale = deepcopy(registry.read().records[0])
    stale.last_checked_at = "stale-observation"
    stale.activity = "inactive"
    registry.unregister(0)
    assert registry.begin(submission(tmp_path), "new-part") == 0
    registry.update([stale])
    current = registry.read().records[0]
    assert current.workflow_id == "new-id"
    assert current.submission_part_id == "new-part"
    assert current.revision > stale.revision
    assert current.last_checked_at == "" and current.jobs == []
    assert current.activity == "active"


def test_unregister_rejects_in_progress_writes_then_allows_finished_submission(
    app, tmp_path
):
    registry = SubmissionRegistry(app)
    local_id = registry.begin(submission(tmp_path), "new-part")
    before = registry.path.read_bytes()
    with pytest.raises(ValueError, match="while jobs are being submitted"):
        registry.unregister(local_id)
    assert registry.path.read_bytes() == before
    registry.add_job(local_id, JobRecord(0, scheduler_reference="job"))
    registry.finish(local_id)
    registry.unregister(local_id)
    assert local_id in registry.read().free_local_ids


@pytest.mark.parametrize("exited", [False, True])
def test_stale_submitting_flag_does_not_prevent_unregister(
    app, tmp_path, monkeypatch, exited
):
    import psutil

    original = records(tmp_path, 1)
    original[0].submitting = True
    original[0].submitting_owner = ProcessIdentity(123, 100.0)
    registry = seed(app, original)
    process = (
        Mock(side_effect=psutil.NoSuchProcess(123))
        if exited
        else Mock(return_value=Mock(create_time=lambda: 200.0))
    )
    monkeypatch.setattr(psutil, "Process", process)
    registry.unregister(0)
    assert registry.read().free_local_ids == {0: 1}


def test_concurrent_registration_does_not_allocate_freed_ids_twice(app, tmp_path):
    registry = seed(app, records(tmp_path, 4))
    for local_id in (2, 1, 0):
        registry.unregister(local_id)
    with ThreadPoolExecutor(max_workers=4) as pool:
        allocated = list(
            pool.map(lambda i: registry.begin(submission(tmp_path), f"new-{i}"), range(6))
        )
    assert sorted(allocated) == [0, 1, 2, 4, 5, 6]
    assert len({r.local_id for r in registry.read().records}) == 7


@pytest.mark.parametrize("local_id", [99, -1, True, "0"])
def test_unknown_or_invalid_ids_leave_registry_unchanged(app, tmp_path, local_id):
    registry = seed(app, records(tmp_path, 1))
    before = registry.path.read_bytes()
    with pytest.raises(ValueError):
        registry.unregister(local_id)
    assert registry.path.read_bytes() == before


@pytest.mark.parametrize(
    "free_ids",
    [{"0": 1}, {"3": 1}, {"-1": 1}, {"1": 0}, {"1": True}, {"1": "2"}, {"01": 1}, [1]],
)
def test_invalid_reusable_id_data_is_rejected(tmp_path, free_ids):
    data = to_json(RegistryData(records(tmp_path, 1), next_local_id=3))
    data["free_local_ids"] = free_ids
    with pytest.raises(ValueError):
        RegistryData.from_json(data)


def test_cli_unregister_releases_id_through_app_mixin(app, tmp_path, monkeypatch):
    registry = seed(app, records(tmp_path, 2))
    monkeypatch.setattr(hf, "_user_data_hostname_dir", app.user_data_hostname_dir)
    result = CliRunner().invoke(_make_unregister_CLI(hf), ["0"])
    assert result.exit_code == 0, result.output
    assert "Unregistered submission 0" in result.output
    assert [r.local_id for r in registry.read().records] == [1]
    assert 0 in registry.read().free_local_ids
    result = CliRunner().invoke(_make_unregister_CLI(hf), ["0"])
    assert result.exit_code == 1 and "Unknown registered submission ID" in result.output
    assert CliRunner().invoke(_make_unregister_CLI(hf), ["-1"]).exit_code == 2


@pytest.mark.parametrize("include_active", [False, True])
def test_bulk_unregister_filters_activity_and_releases_every_selected_id(
    app, tmp_path, include_active
):
    original = records(tmp_path, 4)
    original[0].activity = original[1].activity = "inactive"
    original[3].activity = "unknown"
    registry = seed(app, original)
    # Explicit removal must not apply a newly lowered retention limit to records
    # outside the selection, or silently retire their IDs without freeing them.
    app.config["known_submissions_max_unknown"] = 0
    removed = registry.unregister_all(include_active)
    expected = {0, 1, 2, 3} if include_active else {0, 1}
    assert {r.local_id for r in removed} == expected
    current = registry.read()
    assert set(current.free_local_ids) == expected
    assert {r.local_id for r in current.records} == set(range(4)) - expected
    assert registry.begin(submission(tmp_path), "new-part") == 0
    app.Workflow.assert_not_called()
    app.get_scheduler.assert_not_called()


def test_bulk_unregister_validates_all_records_before_removing_any(app, tmp_path):
    original = records(tmp_path, 1)
    original[0].activity = "inactive"
    registry = seed(app, original)
    active_id = registry.begin(submission(tmp_path), "submitting-now")
    before = registry.path.read_bytes()
    with pytest.raises(ValueError, match="while jobs are being submitted"):
        registry.unregister_all(include_active=True)
    assert registry.path.read_bytes() == before
    assert [r.local_id for r in registry.unregister_all()] == [0]
    assert [r.local_id for r in registry.read().records] == [active_id]


def test_bulk_unregister_no_matches_keeps_records_and_free_id_pool(app, tmp_path):
    registry = seed(app, records(tmp_path, 2))
    registry.unregister(0)
    before = registry.read()
    assert registry.unregister_all() == []
    assert registry.read() == before


@pytest.mark.parametrize("include_active", [False, True])
def test_cli_bulk_unregister_through_app_mixin(
    app, tmp_path, monkeypatch, include_active
):
    original = records(tmp_path, 4)
    original[0].activity = original[1].activity = "inactive"
    original[3].activity = "unknown"
    registry = seed(app, original)
    monkeypatch.setattr(hf, "_user_data_hostname_dir", app.user_data_hostname_dir)
    args = ["--all"] + (["--include-active"] if include_active else [])
    result = CliRunner().invoke(_make_unregister_CLI(hf), args)
    assert result.exit_code == 0, result.output
    assert (
        "Unregistered 4 submissions."
        if include_active
        else "Unregistered 2 inactive submissions."
    ) in result.output
    assert len(registry.read().records) == (0 if include_active else 2)
    assert len(registry.read().free_local_ids) == (4 if include_active else 2)


@pytest.mark.parametrize(
    "args", [[], ["0", "--all"], ["0", "--include-active"], ["--include-active"]]
)
def test_cli_bulk_unregister_requires_unambiguous_selection(args):
    app = SimpleNamespace(unregister=Mock())
    result = CliRunner().invoke(_make_unregister_CLI(app), args)
    assert result.exit_code == 2
    app.unregister.assert_not_called()


def test_sdk_bulk_unregister_returns_removed_ids(app, tmp_path, monkeypatch):
    original = records(tmp_path, 2)
    for r in original:
        r.activity = "inactive"
    registry = seed(app, original)
    monkeypatch.setattr(hf, "_user_data_hostname_dir", app.user_data_hostname_dir)
    assert hf.unregister(all_submissions=True) == [0, 1]
    assert registry.read().records == []


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"local_id": 0, "all_submissions": True},
        {"local_id": 0, "include_active": True},
        {"all_submissions": 1},
    ],
)
def test_sdk_bulk_unregister_rejects_invalid_selection_without_changes(
    app, tmp_path, monkeypatch, kwargs
):
    registry = seed(app, records(tmp_path, 1))
    monkeypatch.setattr(hf, "_user_data_hostname_dir", app.user_data_hostname_dir)
    before = registry.path.read_bytes()
    with pytest.raises(ValueError):
        hf.unregister(**kwargs)
    assert registry.path.read_bytes() == before
