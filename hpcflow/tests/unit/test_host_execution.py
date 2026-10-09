from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import Mock

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core import NO_COMMANDS_EXIT_CODE


def make_workflow(tmp_path):
    return hf.Workflow.from_template(
        hf.WorkflowTemplate(
            name="host execution",
            tasks=[
                hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": 101})
            ],
        ),
        path=tmp_path,
    )


@pytest.mark.parametrize("exit_code", [0, 7])
def test_host_run_preparation_and_completion(tmp_path, monkeypatch, exit_code):
    workflow = make_workflow(tmp_path)
    workflow.submit(containerised=True, status=False)
    run = workflow.submissions[0].jobscripts[0].all_EARs[0]
    executor = Mock(side_effect=AssertionError("Must not execute commands in container"))
    monkeypatch.setattr(hf, "Executor", executor)
    monkeypatch.setattr(hf.run_time_info, "container_host_os", "nt")
    monkeypatch.setattr(hf.run_time_info, "execution_hostname", "host-execution-machine")
    cwd = Path.cwd()
    try:
        plan = workflow._prepare_host_run(0, (0, 0, 0), run.id_)
        assert plan is not None
        assert plan["workflow_id"] == workflow.id_
        assert Path(plan["command"][-1]).is_file()
        executor.assert_not_called()
        reloaded = hf.Workflow(workflow.path)
        prepared_run = reloaded.get_EARs_from_IDs([run.id_])[0]
        assert prepared_run.start_time is not None
        assert prepared_run.end_time is None
        assert prepared_run.port_number is None
        assert prepared_run.run_hostname == "host-execution-machine"
        with pytest.raises(ValueError, match="already"):
            reloaded._prepare_host_run(0, (0, 0, 0), run.id_)
        reloaded._complete_host_run(0, (0, 0, 0), run.id_, exit_code)
        completed = hf.Workflow(workflow.path).get_EARs_from_IDs([run.id_])[0]
        assert completed.exit_code == exit_code
        assert completed.end_time is not None
        reloaded._complete_host_run(0, (0, 0, 0), run.id_, exit_code)
        with pytest.raises(ValueError, match="Conflicting"):
            reloaded._complete_host_run(0, (0, 0, 0), run.id_, exit_code + 1)
    finally:
        os.chdir(cwd)


def test_host_completion_requires_preparation(tmp_path):
    workflow = make_workflow(tmp_path)
    workflow.submit(containerised=True, status=False)
    run = workflow.submissions[0].jobscripts[0].all_EARs[0]
    with pytest.raises(ValueError, match="not been prepared"):
        workflow._complete_host_run(0, (0, 0, 0), run.id_, 0)
    with pytest.raises(ValueError, match="non-negative"):
        workflow._complete_host_run(-1, (0, 0, 0), run.id_, 0)


def test_host_run_without_commands_finalises_in_container(tmp_path, monkeypatch):
    workflow = make_workflow(tmp_path)
    workflow.submit(containerised=True, status=False)
    run = workflow.submissions[0].jobscripts[0].all_EARs[0]
    monkeypatch.setattr(workflow, "ensure_commands_file", Mock(return_value=False))
    cwd = Path.cwd()
    try:
        assert workflow._prepare_host_run(0, (0, 0, 0), run.id_) is None
    finally:
        os.chdir(cwd)
    completed = hf.Workflow(workflow.path).get_EARs_from_IDs([run.id_])[0]
    assert completed.exit_code == NO_COMMANDS_EXIT_CODE
    assert completed.end_time is not None


@pytest.mark.parametrize("unsupported", ["combined", "abortable"])
def test_host_run_rejects_unsupported_actions(tmp_path, monkeypatch, unsupported):
    workflow = make_workflow(tmp_path)
    workflow.submit(containerised=True, status=False)
    jobscript = workflow.submissions[0].jobscripts[0]
    run = jobscript.all_EARs[0]
    if unsupported == "combined":
        monkeypatch.setattr(jobscript.resources, "combine_scripts", True)
    else:
        monkeypatch.setattr(run.action, "abortable", True)
    with pytest.raises(NotImplementedError, match="combined scripts or abortable"):
        workflow._prepare_host_run(0, (0, 0, 0), run.id_)
    assert hf.Workflow(workflow.path).get_EARs_from_IDs([run.id_])[0].start_time is None
