from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from unittest.mock import Mock

import pytest

from hpcflow.sdk.app import BaseApp
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.submission.jobscript import Jobscript
from hpcflow.sdk.submission.schedulers import QueuedScheduler
from hpcflow.sdk.submission.schedulers.direct import DirectScheduler


def _make_workflow() -> Mock:
    workflow = Mock(spec=Workflow)
    workflow._validate_follow_jobscripts = Workflow._validate_follow_jobscripts
    return workflow


def _make_jobscript(
    path: Path, index: int = 0, *, array: bool = False, combined: bool = False
) -> Mock:
    job = Mock(spec=Jobscript)
    job.index = index
    job.submission.index = 0
    job.is_array = array
    job.blocks = [Mock(num_elements=2)]
    job.resources.combine_jobscript_std = combined
    job.process_ID = None if array else index + 100
    job.scheduler_job_ID = "123" if array else None
    job.scheduler_js_ref = "123" if array else (job.process_ID, ["test"])
    job.scheduler = Mock(spec=QueuedScheduler if array else DirectScheduler)
    job.scheduler.get_job_state_info.return_value = {}
    job.is_submitted = True
    job.all_EARs = [Mock(status=EARStatus.running)]

    def stdout(array_idx: int | None = None) -> Path:
        return path / f"js_{index}_{array_idx}.out"

    def stderr(array_idx: int | None = None) -> Path:
        return stdout(array_idx) if combined else path / f"js_{index}_{array_idx}.err"

    job.get_stdout_path.side_effect = stdout
    job.get_stderr_path.side_effect = stderr
    return job


def test_follow_streams_append_and_delayed_creation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    job = _make_jobscript(tmp_path)
    stdout = job.get_stdout_path()
    stderr = job.get_stderr_path()
    stdout.write_bytes(b"initial\n")
    job.scheduler.get_job_state_info.side_effect = [
        {"100": "running"},
        {"100": "running"},
        {},
    ]
    ticks = 0

    def tick(interval: float) -> None:
        nonlocal ticks
        ticks += 1
        if ticks == 1:
            stdout.write_bytes(b"initial\nappended\n")
            stderr.write_bytes(b"error\n")
        else:
            with stdout.open("ab") as handle:
                handle.write(b"final")
            with stderr.open("ab") as handle:
                handle.write(b"final error")

    monkeypatch.setattr("hpcflow.sdk.core.workflow.time.sleep", tick)
    Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)
    captured = capsys.readouterr()
    assert captured.out == "initial\nappended\nfinal"
    assert captured.err == "error\nfinal error"
    assert ticks == 2
    assert job.scheduler.get_job_state_info.call_count == 3
    job.scheduler.get_job_state_info.assert_called_with(js_refs=[job.scheduler_js_ref])


@pytest.mark.parametrize("array", [False, True])
@pytest.mark.parametrize("combined", [False, True])
def test_follow_parallel_jobs_arrays_and_combined_streams(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], array: bool, combined: bool
) -> None:
    jobs = [
        _make_jobscript(tmp_path, index, array=array, combined=combined)
        for index in range(2)
    ]
    jobs[1].submission.index = 1
    for job in jobs:
        for array_idx in ([1, 2] if array else [None]):
            job.get_stdout_path(array_idx).write_bytes(
                f"out {job.index} {array_idx}\n".encode()
            )
            if not combined:
                job.get_stderr_path(array_idx).write_bytes(
                    f"err {job.index} {array_idx}\n".encode()
                )
        job.get_stdout_path.reset_mock()
        job.get_stderr_path.reset_mock()

    if array:
        with pytest.raises(NotImplementedError, match="only for direct"):
            Workflow._follow_jobscripts(_make_workflow(), jobs, quiet=False)
        for job in jobs:
            job.scheduler.get_job_state_info.assert_not_called()
            job.get_stdout_path.assert_not_called()
            job.get_stderr_path.assert_not_called()
        assert capsys.readouterr().out == ""
        return

    Workflow._follow_jobscripts(_make_workflow(), jobs, quiet=False)
    captured = capsys.readouterr()
    for job in jobs:
        for array_idx in ([1, 2] if array else [None]):
            assert captured.out.count(f"out {job.index} {array_idx}\n") == 1
            assert (
                f"submission {job.submission.index}, jobscript {job.index}"
                in captured.out
            )
            if array:
                assert f"array {array_idx}" in captured.out
            if not combined:
                assert captured.err.count(f"err {job.index} {array_idx}\n") == 1
            job.get_stdout_path.assert_called_once_with()
            job.get_stderr_path.assert_called_once_with()
    if combined:
        assert captured.err == ""
        assert "stdout/stderr" in captured.out


def test_follow_decodes_split_utf8_and_flushes_incomplete_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    job = _make_jobscript(tmp_path)
    stdout = job.get_stdout_path()
    stdout.write_bytes(b"\xe2")
    job.scheduler.get_job_state_info.side_effect = [{"100": "running"}, {}]

    def tick(interval: float) -> None:
        stdout.write_bytes(b"\xe2\x82\xac\n\xe2")

    monkeypatch.setattr("hpcflow.sdk.core.workflow.time.sleep", tick)
    Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)
    assert capsys.readouterr().out == "\u20ac\n\ufffd"


def test_follow_truncated_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    job = _make_jobscript(tmp_path)
    stdout = job.get_stdout_path()
    stdout.write_bytes(b"long initial output\n")
    job.scheduler.get_job_state_info.side_effect = [{"100": "running"}, {}]

    def tick(interval: float) -> None:
        stdout.write_bytes(b"short\n")

    monkeypatch.setattr("hpcflow.sdk.core.workflow.time.sleep", tick)
    Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)
    assert capsys.readouterr().out == "long initial output\nshort\n"


def test_follow_propagates_read_and_scheduler_errors(tmp_path: Path) -> None:
    job = _make_jobscript(tmp_path)
    job.scheduler.get_job_state_info.side_effect = RuntimeError("scheduler failed")
    with pytest.raises(RuntimeError, match="scheduler failed"):
        Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)
    job.scheduler.get_job_state_info.side_effect = None
    job.get_stdout_path().mkdir()
    with pytest.raises((PermissionError, IsADirectoryError)):
        Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)


@pytest.mark.parametrize("follow", [False, True])
@pytest.mark.parametrize("wait", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
def test_submit_follow_implies_wait(follow: bool, wait: bool, cancel: bool) -> None:
    workflow = _make_workflow()
    workflow._store = Mock()
    workflow._store.cached_load.return_value = nullcontext()
    workflow.batch_update.return_value = nullcontext()
    workflow._store.parameters_metadata_cache.return_value = nullcontext()
    workflow._store.parameters_array_cache.return_value = nullcontext()
    workflow._store.cache_ctx.return_value = nullcontext()
    workflow._submit.return_value = [], {0: [0, 1]}
    result = Workflow.submit(
        workflow,
        follow=follow,
        wait=wait,
        cancel=cancel,
        return_idx=True,
        status=False,
        quiet=True,
    )
    assert result == {0: [0, 1]}
    assert workflow._submit.call_args.kwargs["follow"] == follow
    if cancel:
        workflow.cancel.assert_called_once_with(status=False, quiet=True)
        workflow.wait.assert_not_called()
    elif follow or wait:
        workflow.wait.assert_called_once_with({0: [0, 1]}, quiet=True, follow=follow)
    else:
        workflow.wait.assert_not_called()


def test_wait_follow_dispatch_and_interrupt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workflow = _make_workflow()
    job = _make_jobscript(tmp_path)
    workflow.submissions = [Mock(jobscripts=[job])]
    workflow._follow_jobscripts.side_effect = KeyboardInterrupt
    Workflow.wait(workflow, {0: [0]}, follow=True)
    workflow._follow_jobscripts.assert_called_once_with([job], quiet=False)
    workflow.cancel.assert_not_called()
    assert "workflow execution will continue" in capsys.readouterr().out


def test_wait_empty_selection_does_not_follow_other_jobs() -> None:
    workflow = _make_workflow()
    Workflow.wait(workflow, {}, quiet=True, follow=True)
    workflow._follow_jobscripts.assert_not_called()


def test_wait_follow_finds_active_jobscripts(tmp_path: Path) -> None:
    workflow = _make_workflow()
    job = _make_jobscript(tmp_path)
    submission = Mock(index=0, jobscripts=[job])
    submission.get_active_jobscripts.return_value = {0: {}}
    workflow.submissions = [submission]
    Workflow.wait(workflow, quiet=True, follow=True)
    workflow._follow_jobscripts.assert_called_once_with([job], quiet=True)


@pytest.mark.parametrize("explicit_selection", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_wait_follow_rejects_non_direct_before_polling(
    tmp_path: Path, explicit_selection: bool, mixed: bool
) -> None:
    workflow = _make_workflow()
    jobs = [_make_jobscript(tmp_path, array=True)]
    if mixed:
        jobs.insert(0, _make_jobscript(tmp_path, index=1))
    workflow.submissions = [Mock(index=0, jobscripts=jobs)]
    selection = {0: list(range(len(jobs)))} if explicit_selection else None
    with pytest.raises(NotImplementedError, match="non-direct scheduler"):
        Workflow.wait(workflow, selection, follow=True)
    for job in jobs:
        job.scheduler.get_job_state_info.assert_not_called()
        job.get_active_states.assert_not_called()
    workflow._follow_jobscripts.assert_not_called()


def test_wait_follow_ignores_finished_non_direct_jobs(tmp_path: Path) -> None:
    workflow = _make_workflow()
    direct = _make_jobscript(tmp_path)
    queued = _make_jobscript(tmp_path, index=1, array=True)
    queued.all_EARs = [Mock(status=EARStatus.success)]
    workflow.submissions = [Mock(index=0, jobscripts=[direct, queued])]
    Workflow.wait(workflow, follow=True, quiet=True)
    workflow._follow_jobscripts.assert_called_once_with([direct], quiet=True)
    queued.scheduler.get_job_state_info.assert_not_called()
    queued.get_active_states.assert_not_called()


@pytest.mark.parametrize("new_submission", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_submit_follow_rejects_non_direct_before_launch(
    tmp_path: Path, new_submission: bool, mixed: bool
) -> None:
    workflow = _make_workflow()
    workflow._store = Mock()
    workflow.execution_path = tmp_path / "execute"
    workflow.task_artifacts_path = tmp_path / "tasks"
    queued = _make_jobscript(tmp_path, array=True)
    submissions = [
        Mock(needs_submit=True, jobscripts=[queued], outstanding_jobscripts=[0])
    ]
    if mixed:
        direct = _make_jobscript(tmp_path, index=1)
        submissions.insert(
            0, Mock(needs_submit=True, jobscripts=[direct], outstanding_jobscripts=[0])
        )
    workflow.submissions = [] if new_submission else submissions
    workflow._add_submission.return_value = submissions[-1]
    with pytest.raises(NotImplementedError, match="only for direct"):
        Workflow._submit(workflow, follow=True)
    for submission in submissions:
        submission.submit.assert_not_called()
    queued.scheduler.get_job_state_info.assert_not_called()


@pytest.mark.parametrize("method", ["make", "demo", "existing"])
@pytest.mark.parametrize("return_idx", [False, True])
@pytest.mark.parametrize("follow", [False, True])
def test_app_forwards_follow(method: str, return_idx: bool, follow: bool) -> None:
    app = Mock(spec=BaseApp)
    workflow = Mock(spec=Workflow)
    app._make_workflow.return_value = workflow
    app._make_demo_workflow.return_value = workflow
    app.Workflow = Mock(return_value=workflow)
    workflow.submit.return_value = {0: [0]}
    if method == "make":
        BaseApp._make_and_submit_workflow(
            app, "workflow.yaml", follow=follow, return_idx=return_idx
        )
    elif method == "demo":
        BaseApp._make_and_submit_demo_workflow(
            app, "workflow.yaml", follow=follow, return_idx=return_idx
        )
    else:
        BaseApp._submit_workflow(app, "workflow", follow=follow, return_idx=return_idx)
    assert workflow.submit.call_args.kwargs["follow"] == follow
