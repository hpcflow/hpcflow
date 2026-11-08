"""Batch real scheduler commands across submissions and isolate query endpoints."""

import logging
from pathlib import Path
import socket
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.monitoring.models import JobRecord, ProcessReference, SubmissionRecord
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json
from hpcflow.sdk.monitoring.status import observe_activity
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.schedulers import slurm


@pytest.fixture
def app(tmp_path):
    return SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy",
        config={"show_max_recent": 3, "known_submissions_max_inactive": 100},
        submission_logger=logging.getLogger(__name__),
        get_scheduler=hf.get_scheduler,
    )


def submission(tmp_path, index, args=None):
    r = SubmissionRecord(
        str(tmp_path / "workflow"),
        "workflow-id",
        "workflow",
        index,
        f"part-{index}",
        "2026-11-06T12:00:00Z",
        local_id=index,
        jobs=[
            JobRecord(
                0,
                "slurm",
                "posix",
                scheduler_args=args or {},
                hostname=socket.gethostname(),
                scheduler_reference=str(100 + index),
            )
        ],
    )
    return r


def test_show_runs_one_slurm_command_for_different_submission_settings(
    app, tmp_path, monkeypatch
):
    records = []
    for index in range(4):
        args = {
            "directives": {
                "--time": f"0{index}:00:00",
                "--output": f"submission-{index}.out",
            },
            "submit_cmd": f"sbatch-{index}",
            "del_cmd": f"scancel-{index}",
        }
        # Normalize omitted and explicit default queue commands into one query.
        if index % 2:
            args["show_cmd"] = ["squeue", "--me"]
        records.append(submission(tmp_path, index, args))
    records[3].jobs[0].scheduler_reference = "100"
    atomic_json(
        Path(records[0].workflow_path) / ".zattrs",
        {"creation_info": {"id": "workflow-id"}, "name": "workflow"},
    )
    registry = SubmissionRegistry(app)
    with registry.transaction() as data:
        data.records = records
    monkeypatch.setattr(hf, "_user_data_hostname_dir", app.user_data_hostname_dir)
    command = Mock(return_value=("100 RUNNING\n101 PENDING\n999 RUNNING\n", ""))
    monkeypatch.setattr(slurm, "run_cmd", command)
    hf.show(columns=["name", "status"], status=False, no_update=True)
    command.assert_called_once()
    assert command.call_args.args[0][:2] == ["squeue", "--me"]
    assert "--jobs" not in command.call_args.args[0]
    # A later show obtains a fresh snapshot rather than caching between commands.
    hf.show(columns=["name", "status"], status=False, no_update=True)
    assert command.call_count == 2


def test_one_snapshot_populates_every_submission(app, tmp_path, monkeypatch):
    records = [
        submission(tmp_path, i, {"directives": {"--time": str(i)}}) for i in range(3)
    ]
    command = Mock(return_value=("100 RUNNING\n101_[1-2] PENDING\n999 RUNNING\n", ""))
    monkeypatch.setattr(slurm, "run_cmd", command)
    observe_activity(app, records)
    command.assert_called_once()
    assert [r.activity for r in records] == ["active", "active", "inactive"]
    assert records[0].scheduler_states == ["running"]
    assert records[1].scheduler_states == ["pending", "pending"]
    assert records[2].scheduler_states == []


def test_different_query_targets_do_not_share_states(app, tmp_path, monkeypatch):
    records = [
        submission(tmp_path, i, {"show_cmd": ["squeue", "--clusters", cluster]})
        for i, cluster in enumerate(["one", "two"])
    ]
    records[1].jobs[0].scheduler_reference = "100"
    command = Mock(
        side_effect=lambda cmd, **kwargs: (
            ("100 RUNNING\n", "") if "one" in cmd else ("", "")
        )
    )
    monkeypatch.setattr(slurm, "run_cmd", command)
    observe_activity(app, records)
    assert command.call_count == 2
    assert records[0].activity == "active" and records[1].activity == "inactive"


def test_shared_cluster_login_hosts_share_one_snapshot(app, tmp_path, monkeypatch):
    app.config["machine"] = "shared-cluster"
    records = [submission(tmp_path, i) for i in range(2)]
    for i, r in enumerate(records):
        r.jobs[0].hostname = f"login-{i}"
        r.jobs[0].machine = "shared-cluster"
    command = Mock(return_value=("100 RUNNING\n101 RUNNING\n", ""))
    monkeypatch.setattr(slurm, "run_cmd", command)
    observe_activity(app, records)
    command.assert_called_once()
    assert all(r.activity == "active" for r in records)


def test_failed_snapshot_is_not_retried_for_each_submission(app, tmp_path, monkeypatch):
    records = [submission(tmp_path, i, {"del_cmd": f"cancel-{i}"}) for i in range(3)]
    command = Mock(return_value=("", "Scheduler unavailable"))
    monkeypatch.setattr(slurm, "run_cmd", command)
    observe_activity(app, records)
    command.assert_called_once()
    assert all(r.activity == "unknown" and r.scheduler_error for r in records)


def test_invalid_reference_does_not_prevent_healthy_jobs_being_queried(
    app, tmp_path, monkeypatch
):
    records = [submission(tmp_path, i) for i in range(2)]
    records[0].jobs[0].scheduler_reference = ""
    command = Mock(return_value=("101 RUNNING\n", ""))
    monkeypatch.setattr(slurm, "run_cmd", command)
    observe_activity(app, records)
    command.assert_called_once()
    assert records[0].activity == "unknown" and records[0].scheduler_error
    assert records[1].activity == "active" and records[1].scheduler_error is None


def test_duplicate_direct_references_are_checked_once(app, tmp_path):
    records = [submission(tmp_path, i) for i in range(3)]
    for i, r in enumerate(records):
        r.jobs[0].scheduler_type = "direct"
        r.jobs[0].scheduler_args = {"shebang_executable": [f"shell-{i}"]}
        r.jobs[0].scheduler_reference = ProcessReference(123, ("python", "workflow.py"))
    adapter = Mock(
        get_job_state_info=Mock(return_value={"123": JobscriptElementState.running})
    )
    app.get_scheduler = Mock(return_value=adapter)
    observe_activity(app, records)
    adapter.get_job_state_info.assert_called_once_with(
        js_refs=[(123, ["python", "workflow.py"])]
    )
    assert all(r.activity == "active" for r in records)


@pytest.mark.parametrize(
    "refs,expected",
    [(None, {"100", "101"}), (["100", "finished-id"], {"100"}), ([], set())],
)
def test_slurm_filters_a_single_snapshot_without_invalid_id_retries(
    monkeypatch, refs, expected
):
    command = Mock(return_value=("100 RUNNING\n101_[1-2] PENDING\n", ""))
    monkeypatch.setattr(slurm, "run_cmd", command)
    states = hf.get_scheduler("slurm", "posix").get_job_state_info(js_refs=refs)
    assert set(states) == expected
    assert command.call_count == (0 if refs == [] else 1)


def test_slurm_does_not_expand_unrelated_large_arrays(monkeypatch):
    command = Mock(return_value=("100_[1-2] PENDING\n999_[1-1000000] RUNNING\n", ""))
    monkeypatch.setattr(slurm, "run_cmd", command)
    scheduler = hf.get_scheduler("slurm", "posix")
    parse_ids = Mock(wraps=scheduler._parse_job_IDs)
    monkeypatch.setattr(scheduler, "_parse_job_IDs", parse_ids)

    states = scheduler.get_job_state_info(js_refs=["100", "finished-id"])

    command.assert_called_once()
    parse_ids.assert_called_once_with("100_[1-2]")
    assert states == {
        "100": {0: JobscriptElementState.pending, 1: JobscriptElementState.pending}
    }
