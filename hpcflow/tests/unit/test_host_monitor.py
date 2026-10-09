from __future__ import annotations

import copy
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from hpcflow.app import app as hf
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.host_monitor import (
    known_workflow_path,
    parse_host_show,
    prepare_host_show,
)
from hpcflow.sdk.submission.schedulers.direct import DirectScheduler
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix
from hpcflow.sdk.submission.schedulers.sge import SGEPosix
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.cli import _make_internal_CLI


@pytest.fixture
def host_submission(tmp_path, monkeypatch):
    registry = tmp_path / "registry"
    registry.mkdir()
    monkeypatch.setattr(hf, "_user_data_hostname_dir", registry)
    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(
            name="monitored",
            tasks=[
                hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": 101})
            ],
        ),
        path=tmp_path,
    )
    workflow.submit(containerised=True, status=False)
    workflow._record_host_submission(
        {
            "schema_version": 1,
            "workflow_id": workflow.id_,
            "submission_index": 0,
            "jobscript_index": 0,
            "process_ID": 12345,
            "submit_command": ["host-shell", "script"],
            "submit_time": "2026-10-09T13:00:00+00:00",
            "submit_hostname": "host",
            "submit_machine": hf.config.get("machine"),
        }
    )
    return workflow


@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("no_update", [False, True])
def test_host_show_uses_snapshot_not_container_processes(
    host_submission, monkeypatch, active, no_update
):
    monkeypatch.setattr(
        DirectScheduler,
        "get_job_state_info",
        Mock(side_effect=AssertionError("Must not query container processes")),
    )
    plan = prepare_host_show(hf)
    assert len(plan["jobs"]) == 1
    snapshot = copy.deepcopy(plan)
    snapshot["jobs"][0]["active"] = active
    states = parse_host_show(hf, snapshot)
    monkeypatch.setattr(hf.run_time_info, "container_host_os", "nt")
    before = hf.known_subs_file_path.read_bytes()
    items = hf._get_known_submissions(host_states=states, no_update=no_update)
    assert bool(items[0]["active_jobscripts"]) is active
    if active:
        assert set(items[0]["active_jobscripts"][0][0].values()) == {
            JobscriptElementState.running
        }
    if no_update:
        assert hf.known_subs_file_path.read_bytes() == before
    else:
        assert hf.read_known_submissions_file()[0]["is_active"] is active


@pytest.mark.parametrize("invalid", ["missing", "duplicate", "identity", "activity"])
def test_invalid_host_snapshot_preserves_registry(host_submission, invalid):
    snapshot = prepare_host_show(hf)
    snapshot["jobs"][0]["active"] = True
    if invalid == "missing":
        snapshot["jobs"] = []
    elif invalid == "duplicate":
        snapshot["jobs"] *= 2
    elif invalid == "identity":
        snapshot["jobs"][0]["workflow_id"] = "other"
    else:
        snapshot["jobs"][0]["active"] = "true"
    before = hf.known_subs_file_path.read_bytes()
    with pytest.raises(ValueError):
        parse_host_show(hf, snapshot)
    assert hf.known_subs_file_path.read_bytes() == before


def test_unbridged_container_monitoring_rejected(host_submission, monkeypatch):
    monkeypatch.setattr(hf.run_time_info, "container_host_os", "nt")
    before = hf.known_subs_file_path.read_bytes()
    with pytest.raises(ValueError, match="host wrapper"):
        hf._get_known_submissions()
    assert hf.known_subs_file_path.read_bytes() == before


def test_host_known_json_uses_host_snapshot(host_submission):
    snapshot = prepare_host_show(hf)
    snapshot["jobs"][0]["active"] = True
    command = _make_internal_CLI(hf).commands["get-known-host"]
    result = CliRunner().invoke(command, ["--json"], input=json.dumps(snapshot))
    assert result.exit_code == 0, result.output
    items = json.loads(result.stdout)
    assert len(items) == 1
    assert set(items[0]["active_jobscripts"]["0"]["0"].values()) == {"running"}


def test_missing_host_workflow_is_not_resolved_under_another_mount(host_submission):
    path = Path(host_submission.path)
    path.rename(path.with_name(path.name + "-moved"))
    snapshot = prepare_host_show(hf)
    assert not snapshot["jobs"]
    before = hf.known_subs_file_path.read_bytes()
    items = hf._get_known_submissions(
        host_states=parse_host_show(hf, snapshot),
        no_update=True,
    )
    assert items[0]["deleted"]
    assert not items[0]["active_jobscripts"]
    assert hf.known_subs_file_path.read_bytes() == before


def test_persistent_mount_identity(monkeypatch):
    monkeypatch.setenv("HPCFLOW_CONTAINER_WORK_MOUNT", "/host-workflows/mount-one")
    first = known_workflow_path(hf, Path("/work/demo"))
    monkeypatch.setenv("HPCFLOW_CONTAINER_WORK_MOUNT", "/host-workflows/mount-two")
    second = known_workflow_path(hf, Path("/work/demo"))
    assert first != second
    assert str(first).replace("\\", "/") == "/host-workflows/mount-one/demo"


def test_legacy_registry_migrates_only_matching_workflow(host_submission):
    entry = hf.read_known_submissions_file()[0]
    hf.known_subs_file_path.write_text(
        hf._format_known_submissions_line(
            entry["local_id"],
            entry["workflow_id"],
            entry["submit_time"],
            0,
            True,
            "/work/demo",
            "",
            "",
        ),
        encoding="utf-8",
        newline="\n",
    )
    with pytest.raises(ValueError, match="Legacy"):
        prepare_host_show(hf)
    local_id = hf._add_to_known_submissions(
        "/host-workflows/mount/demo",
        entry["workflow_id"],
        0,
        entry["submit_time"],
    )
    assert local_id == entry["local_id"]
    entries = hf.read_known_submissions_file()
    assert len(entries) == 1
    assert entries[0]["path"] == "/host-workflows/mount/demo"


@pytest.mark.parametrize("scheduler_cls", [SlurmPosix, SGEPosix])
@pytest.mark.parametrize("array", [False, True])
def test_host_scheduler_output_uses_shared_parser(tmp_path, scheduler_cls, array):
    scheduler = object.__new__(scheduler_cls)
    scheduler.show_cmd = ["host-query"]
    js = SimpleNamespace(
        index=0,
        submit_machine="host",
        scheduler=scheduler,
        scheduler_name="slurm" if scheduler_cls is SlurmPosix else "sge",
        process_ID=None,
        submit_cmdline=["host-submit"],
        is_scheduled=True,
        scheduler_job_ID="123",
        is_array=array,
        all_EARs=[SimpleNamespace(status=EARStatus.running)],
        blocks=[SimpleNamespace(num_elements=2)],
    )
    sub = SimpleNamespace(index=0, submitted_jobscripts=(0,), jobscripts=[js])
    workflow = SimpleNamespace(id_="workflow", submissions=[sub])
    app = SimpleNamespace(
        read_known_submissions_file=lambda: [
            {
                "path": str(tmp_path),
                "is_active": True,
                "workflow_id": "workflow",
                "sub_idx": 0,
            }
        ],
        Workflow=lambda path: workflow,
        config={"machine": "host"},
    )
    snapshot = prepare_host_show(app)
    if scheduler_cls is SlurmPosix:
        stdout = "123_[1-2] RUNNING" if array else "123 RUNNING"
    else:
        heading = "job-ID  prior name user state slots ja-task-ID"
        state_index = heading.index("state")
        task_index = heading.index("ja-task-ID")
        rows = [
            ("123").ljust(state_index)
            + "r".ljust(task_index - state_index)
            + (str(index) if array else "")
            for index in ((1, 2) if array else (1,))
        ]
        stdout = heading + "\n" + "-" * len(heading) + "\n" + "\n".join(rows)
    snapshot["jobs"][0]["stdout"] = stdout
    states = parse_host_show(app, snapshot)
    assert states[(str(tmp_path), 0)][0][0] == {
        0: JobscriptElementState.running,
        1: JobscriptElementState.running,
    }
    snapshot["jobs"][0]["stdout"] = ""
    assert parse_host_show(app, snapshot) == {(str(tmp_path), 0): {}}
