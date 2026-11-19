"""Persist rate limits independently of workflow state collection."""

from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from pathlib import Path
import socket
from threading import Event
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.config.errors import ConfigChangeValidationError
from hpcflow.sdk.monitoring import scheduler_cache, status
from hpcflow.sdk.monitoring.cache import StateSelection
from hpcflow.sdk.monitoring.columns import RenderContext, status_cell
from hpcflow.sdk.monitoring.models import ActionSummary, JobRecord, SubmissionRecord
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json
from hpcflow.sdk.monitoring.run_index import RunIndex
from hpcflow.sdk.submission.enums import JobscriptElementState


@pytest.fixture
def source(tmp_path, monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(scheduler_cache.time, "time", lambda: clock[0])
    adapter = SimpleNamespace(
        show_cmd=["squeue", "--me"],
        get_job_state_info=Mock(return_value={"100": JobscriptElementState.running}),
    )
    app = SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy",
        config={},
        submission_logger=Mock(),
        get_scheduler=Mock(return_value=adapter),
    )
    row = SubmissionRecord(
        str(tmp_path / "workflow"),
        "id",
        "workflow",
        0,
        "part",
        "",
        local_id=0,
        activity="active",
        jobs=[
            JobRecord(
                0,
                "slurm",
                "posix",
                hostname=socket.gethostname(),
                scheduler_reference="100",
                run_ids=[0],
            )
        ],
    )
    return app, row, adapter.get_job_state_info, clock


def observe(app, rows, **kwargs):
    status.observe_activity(app, rows, min_interval=20, **kwargs)


def test_reuse_does_not_extend_the_interval(source):
    app, row, fetch, clock = source
    observe(app, [row])
    checked = row.jobs[0].last_checked_at
    assert not row.scheduler_cached
    assert row.jobs[0].observation.fresh
    for now in (1005, 1019):
        clock[0] = now
        observe(app, [deepcopy(row)])
        fetch.assert_called_once()
    clock[0] = 1019
    observe(app, [row])
    assert row.scheduler_cached
    assert not row.jobs[0].observation.fresh
    assert row.jobs[0].last_checked_at == checked
    assert "(cached)" in status_cell(row, RenderContext()).plain
    clock[0] = 1020
    observe(app, [row])
    assert fetch.call_count == 2
    assert not row.scheduler_cached


def test_override_refreshes_snapshot_and_resets_interval(source):
    app, row, fetch, clock = source
    observe(app, [row])
    clock[0] += 1
    observe(app, [row], ignore_scheduler_interval=True)
    assert fetch.call_count == 2
    assert not row.scheduler_cached
    clock[0] += 19
    observe(app, [row])
    assert fetch.call_count == 2
    assert row.scheduler_cached


def test_new_references_wait_for_interval_and_remain_unknown(source):
    app, row, fetch, clock = source
    observe(app, [row])
    new = deepcopy(row)
    new.local_id = 1
    new.jobs[0].scheduler_reference = "101"
    clock[0] += 1
    observe(app, [row, new])
    fetch.assert_called_once()
    assert row.activity == "active"
    assert new.activity == "unknown"
    assert new.jobs[0].observation is None
    clock[0] += 20
    fetch.return_value["101"] = JobscriptElementState.pending
    observe(app, [row, new])
    assert fetch.call_count == 2
    assert new.activity == "active"


def test_scheduler_failures_are_rate_limited_too(source):
    app, row, fetch, clock = source
    fetch.side_effect = OSError("scheduler unavailable")
    observe(app, [row])
    assert row.activity == "unknown"
    clock[0] += 1
    observe(app, [row])
    fetch.assert_called_once()
    assert "Cached scheduler query failure" in row.scheduler_error
    clock[0] += 20
    fetch.side_effect = None
    observe(app, [row])
    assert fetch.call_count == 2
    assert row.activity == "active"


def test_different_targets_have_independent_intervals(source):
    app, row, fetch, _ = source
    other = deepcopy(row)
    other.local_id = 1
    other.jobs[0].scheduler_args = {"show_cmd": ["squeue", "--clusters", "other"]}
    adapters = [
        SimpleNamespace(show_cmd=command, get_job_state_info=fetch)
        for command in (["squeue", "--me"], ["squeue", "--clusters", "other"])
    ]
    app.get_scheduler.side_effect = [*adapters, *adapters]
    observe(app, [row, other])
    assert fetch.call_count == 2
    observe(app, [row, other])
    assert fetch.call_count == 2


def test_concurrent_invocations_share_one_query(source):
    app, row, fetch, _ = source
    entered, release = Event(), Event()

    def query(**kwargs):
        entered.set()
        assert release.wait(timeout=10)
        return {"100": JobscriptElementState.running}

    fetch.side_effect = query
    other = deepcopy(row)
    with ThreadPoolExecutor(max_workers=2) as pool:
        first = pool.submit(observe, app, [row])
        try:
            assert entered.wait(timeout=10)
            second = pool.submit(observe, app, [other])
        finally:
            release.set()
        first.result(timeout=10)
        second.result(timeout=10)
    fetch.assert_called_once()
    assert {row.scheduler_cached, other.scheduler_cached} == {False, True}


def test_zero_interval_and_read_only_do_not_write_snapshots(source):
    app, row, fetch, _ = source
    for _ in range(2):
        status.observe_activity(app, [row], min_interval=0)
    assert fetch.call_count == 2
    assert not app.user_data_hostname_dir.exists()
    observe(app, [row], no_update=True)
    assert fetch.call_count == 3
    assert not app.user_data_hostname_dir.exists()


def test_read_only_can_reuse_an_existing_snapshot_without_writes(source):
    app, row, fetch, _ = source
    observe(app, [row])
    before = {
        p: p.read_bytes() for p in app.user_data_hostname_dir.rglob("*") if p.is_file()
    }
    observe(app, [row], no_update=True)
    assert row.scheduler_cached
    fetch.assert_called_once()
    assert before == {
        p: p.read_bytes() for p in app.user_data_hostname_dir.rglob("*") if p.is_file()
    }


def test_corrupt_snapshot_is_rebuilt(source):
    app, row, fetch, _ = source
    observe(app, [row])
    path = next(app.user_data_hostname_dir.rglob("*.json.zst"))
    path.write_bytes(b"not a snapshot")
    observe(app, [row])
    assert fetch.call_count == 2
    assert not row.scheduler_cached


@pytest.mark.parametrize("interval,expected_calls", [(None, 1), (0, 2), (50, 1)])
def test_collect_updates_workflow_states_when_scheduler_is_cached(
    source, monkeypatch, interval, expected_calls
):
    app, row, fetch, clock = source
    if interval is not None:
        app.config["show_scheduler_min_interval"] = interval
    atomic_json(
        Path(row.workflow_path) / ".zattrs",
        {"creation_info": {"id": "id"}, "name": "workflow"},
    )
    registry = SubmissionRegistry(app)
    with registry.transaction() as data:
        data.records = [row]
    monkeypatch.setattr(status, "read_cache", lambda row: None)
    monkeypatch.setattr(
        status, "select_states", lambda *args: StateSelection({}, [0], RunIndex("id", 0))
    )
    loader = Mock(return_value=object())
    monkeypatch.setattr(status, "load_workflow", loader)
    reader = Mock(
        side_effect=[
            ActionSummary(counts={"running": 1}),
            ActionSummary(counts={"success": 1}),
        ]
    )
    monkeypatch.setattr(status, "refresh_actions", reader)
    first = status.collect(app)
    assert first[0].last_action_summary.counts == {"running": 1}
    clock[0] += 1
    second = status.collect(app)
    assert fetch.call_count == expected_calls
    assert reader.call_count == 2
    assert loader.call_count == 2
    assert second[0].last_action_summary.counts == {"success": 1}
    assert second[0].scheduler_cached is (expected_calls == 1)


@pytest.mark.parametrize("value", [-1, True, 1.5, "20"])
def test_config_rejects_invalid_intervals(value):
    with pytest.raises(ConfigChangeValidationError):
        hf.config.set("show_scheduler_min_interval", value)


def test_cli_accepts_ignore_interval():
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    result = CliRunner().invoke(_make_show_CLI(app), ["--ignore-scheduler-interval"])
    assert result.exit_code == 0, result.output
    assert app.show.call_args.kwargs["ignore_scheduler_interval"] is True
