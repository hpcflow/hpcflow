"""Individual action-run inspection, state filtering, and workflow CLI integration."""

from contextlib import nullcontext
from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_workflow_CLI
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.monitoring.runs import collect_runs, show_runs


def run(run_id, state, attempt=0, exit_code=None):
    return SimpleNamespace(
        id_=run_id,
        status=state,
        task=SimpleNamespace(insert_ID=2, unique_name="analysis"),
        element=SimpleNamespace(index=3),
        element_iteration=SimpleNamespace(index=4),
        element_action=SimpleNamespace(action_idx=5),
        action=SimpleNamespace(short_name=lambda: "solve-model"),
        index=attempt,
        submission_idx=0,
        exit_code=exit_code,
    )


@pytest.fixture
def workflow():
    return SimpleNamespace(
        _store=SimpleNamespace(cached_load=nullcontext, cache_ctx=nullcontext),
        get_all_EARs=Mock(
            return_value=[
                run(11, EARStatus.success, attempt=1, exit_code=0),
                run(10, EARStatus.error, exit_code=2),
                run(12, EARStatus.running),
            ]
        ),
        submissions=[
            SimpleNamespace(
                jobscripts=[SimpleNamespace(index=7, all_EAR_IDs=[10, 11, 12])]
            )
        ],
    )


@pytest.mark.parametrize("state", ["error", "ERROR", EARStatus.error])
def test_state_filter_preserves_failed_attempt_after_successful_retry(workflow, state):
    records = collect_runs(workflow, state)
    assert len(records) == 1
    r = records[0]
    assert (r.run_id, r.task_id, r.task_name) == (10, 2, "analysis")
    assert (r.element, r.iteration, r.action, r.attempt, r.submission) == (3, 4, 5, 0, 0)
    assert r.state is EARStatus.error and r.exit_code == 2
    assert r.jobscript == 7 and r.action_name == "solve-model"
    assert [r.run_id for r in collect_runs(workflow)] == [10, 11, 12]


def test_invalid_state_fails_before_store_access(workflow):
    with pytest.raises(ValueError, match="Unknown run state"):
        collect_runs(workflow, "invalid")
    workflow.get_all_EARs.assert_not_called()


def test_runs_table_and_empty_selection(workflow):
    output = StringIO()
    console = Console(file=output, width=200)
    show_runs(workflow, "error", console)
    text = output.getvalue()
    assert all(
        title in text
        for title in (
            "Run ID",
            "Element",
            "Iteration",
            "Action",
            "Attempt",
            "Submission",
            "Jobscript",
            "State",
            "Exit code",
        )
    )
    assert "analysis" in text and "E error" in text
    assert "5: solve-model" in text
    assert "success" not in text and "running" not in text
    output.seek(0)
    output.truncate()
    show_runs(workflow, "skipped", console)
    assert output.getvalue().strip() == "No runs with state 'skipped'."
    workflow.get_all_EARs.return_value = []
    output.seek(0)
    output.truncate()
    show_runs(workflow, console=console)
    assert output.getvalue().strip() == "No runs."


@pytest.mark.parametrize("state", [None, "error", "ERROR", "running"])
def test_cli_passes_state_to_workflow(state):
    wf = Mock(spec=Workflow)
    command = _make_workflow_CLI(hf).commands["show-runs"]
    result = CliRunner().invoke(command, ["--state", state] if state else [], obj=wf)
    assert result.exit_code == 0, result.output
    wf.show_runs.assert_called_once_with(state=state.lower() if state else None)


def test_cli_rejects_unknown_state():
    wf = Mock(spec=Workflow)
    command = _make_workflow_CLI(hf).commands["show-runs"]
    result = CliRunner().invoke(command, ["--state", "invalid"], obj=wf)
    assert result.exit_code == 2
    assert "Invalid value for '--state'" in result.output
    wf.show_runs.assert_not_called()


def test_jobscript_indices_are_scoped_to_each_submission(workflow):
    workflow.get_all_EARs.return_value[-1].submission_idx = 1
    workflow.submissions.append(
        SimpleNamespace(jobscripts=[SimpleNamespace(index=2, all_EAR_IDs=[12])])
    )
    assert [(r.run_id, r.submission, r.jobscript) for r in collect_runs(workflow)] == [
        (10, 0, 7),
        (11, 0, 7),
        (12, 1, 2),
    ]


def test_unassigned_run_has_no_jobscript(workflow):
    pending = run(13, EARStatus.pending)
    pending.submission_idx = None
    workflow.get_all_EARs.return_value = [pending]
    record = collect_runs(workflow)[0]
    assert record.submission is None and record.jobscript is None


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_real_workflow_runs_and_full_cli(tmp_path, store, monkeypatch):
    task = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[hf.ValueSequence(path="resources.any.resources_id", values=[0, 1])],
    )
    wf = hf.Workflow.from_template_data(
        template_name="inspect-runs", path=tmp_path, tasks=[task], store=store
    )
    sub = wf.add_submission(status=False)
    ids = list(sub.all_EAR_IDs)
    assert len(ids) >= 2
    for i, run_id in enumerate(ids):
        wf._store.set_EAR_start(run_id, None, None)
        wf._store.set_EAR_end(run_id, 17 if i == 0 else 0, i != 0, False)
    # Reload from disk to exercise the CLI's ordinary workflow construction path.
    loaded = hf.Workflow(wf.path)
    records = collect_runs(loaded, "error")
    assert len(records) == 1 and records[0].run_id == ids[0]
    assert records[0].exit_code == 17
    expected_jobscript = next(
        job.index for job in sub.jobscripts if ids[0] in job.all_EAR_IDs
    )
    assert records[0].jobscript == expected_jobscript
    assert records[0].action_name == loaded.get_EARs_from_IDs(ids[0]).action.short_name()
    scheduler = Mock(
        side_effect=AssertionError("Run inspection must not query schedulers")
    )
    monkeypatch.setattr(hf, "get_scheduler", scheduler)
    result = CliRunner().invoke(
        _make_workflow_CLI(hf),
        [str(wf.path), "show-runs", "--state", "error"],
        env={"COLUMNS": "200"},
    )
    assert result.exit_code == 0, result.output
    assert "E error" in result.output and "17" in result.output
    assert "Jobscript" in result.output
    assert "success" not in result.output
    scheduler.assert_not_called()
