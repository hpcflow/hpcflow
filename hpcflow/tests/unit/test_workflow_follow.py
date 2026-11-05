from __future__ import annotations

from contextlib import nullcontext
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import Mock, call

import pytest

from hpcflow.sdk.app import BaseApp
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.errors import WorkflowSubmissionFailure
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.submission.jobscript import Jobscript

if TYPE_CHECKING:
    from hpcflow.sdk.core.types import FollowSpec


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
    job.array_indices = (0, 1, 2) if array else None
    job.resources.combine_jobscript_std = combined
    job.process_ID = None if array else index + 100
    job.scheduler_job_ID = "123" if array else None
    job.is_submitted = True
    job.all_EARs = [Mock(status=EARStatus.running)]
    # by default, jobscripts (and array items) are already complete:
    job.completion_obj = Mock()
    job.completion_obj.is_complete.return_value = True
    job.completion_obj.is_array_item_complete.return_value = True

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
    job.completion_obj.is_complete.side_effect = [False, False, True]
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
    assert job.completion_obj.is_complete.call_count == 3
    job.completion_obj.is_array_item_complete.assert_not_called()


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
    array_indices = [0, 1, 2] if array else [None]
    for job in jobs:
        for array_idx in array_indices:
            job.get_stdout_path(array_idx).write_bytes(
                f"out {job.index} {array_idx}\n".encode()
            )
            if not combined:
                job.get_stderr_path(array_idx).write_bytes(
                    f"err {job.index} {array_idx}\n".encode()
                )
        job.get_stdout_path.reset_mock()
        job.get_stderr_path.reset_mock()

    Workflow._follow_jobscripts(
        _make_workflow(), jobs, quiet=False, follow="all" if array else True
    )
    captured = capsys.readouterr()
    for job in jobs:
        for array_idx in array_indices:
            assert captured.out.count(f"out {job.index} {array_idx}\n") == 1
            assert (
                f"submission {job.submission.index}, jobscript {job.index}"
                in captured.out
            )
            if array:
                assert f"array {array_idx}" in captured.out
            if not combined:
                assert captured.err.count(f"err {job.index} {array_idx}\n") == 1
            job.get_stdout_path.assert_any_call(array_idx=array_idx)
            job.get_stderr_path.assert_any_call(array_idx=array_idx)
        assert job.get_stdout_path.call_count == len(array_indices)
        assert job.get_stderr_path.call_count == len(array_indices)
    if combined:
        assert captured.err == ""
        assert "stdout/stderr" in captured.out


@pytest.mark.parametrize("follow", [0, 1, 2])
def test_follow_single_array_item(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], follow: int
) -> None:
    job = _make_jobscript(tmp_path, array=True)
    for array_idx in range(3):
        job.get_stdout_path(array_idx).write_bytes(f"out {array_idx}\n".encode())
        job.get_stderr_path(array_idx).write_bytes(f"err {array_idx}\n".encode())
    Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True, follow=follow)
    captured = capsys.readouterr()
    assert captured.out == f"out {follow}\n"
    assert captured.err == f"err {follow}\n"
    job.completion_obj.is_array_item_complete.assert_called_once_with(follow)
    job.completion_obj.is_complete.assert_not_called()


def test_follow_array_items_finish_independently(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    job = _make_jobscript(tmp_path, array=True, combined=True)
    for array_idx in range(3):
        job.get_stdout_path(array_idx).write_bytes(f"first {array_idx}\n".encode())
    polls = 0
    complete_after = {0: 0, 1: 1, 2: 2}
    job.completion_obj.is_array_item_complete.side_effect = (
        lambda array_idx: polls >= complete_after[array_idx]
    )

    def tick(interval: float) -> None:
        nonlocal polls
        polls += 1
        for array_idx in (1, 2):
            with job.get_stdout_path(array_idx).open("ab") as handle:
                handle.write(f"poll {polls}\n".encode())

    monkeypatch.setattr("hpcflow.sdk.core.workflow.time.sleep", tick)
    Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True, follow="all")
    out = capsys.readouterr().out
    # completed items are not read again:
    assert out.count("poll 1") == 2
    assert out.count("poll 2") == 1
    assert out.count("poll 3") == 0
    calls = job.completion_obj.is_array_item_complete.call_args_list
    assert [calls.count(call(idx)) for idx in range(3)] == [1, 2, 3]


@pytest.mark.parametrize(
    ("follow", "array", "error"),
    [
        (True, False, None),
        (0, False, None),
        ("all", False, None),
        (True, True, "specifying the array"),
        (0, True, None),
        (2, True, None),
        (3, True, "does not exist"),
        ("all", True, None),
    ],
)
def test_validate_follow_jobscripts(
    tmp_path: Path, follow: FollowSpec, array: bool, error: str | None
) -> None:
    jobs = [_make_jobscript(tmp_path, array=array)]
    with pytest.raises(ValueError, match=error) if error else nullcontext():
        Workflow._validate_follow_jobscripts(jobs, follow)


def test_follow_decodes_split_utf8_and_flushes_incomplete_output(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    job = _make_jobscript(tmp_path)
    stdout = job.get_stdout_path()
    stdout.write_bytes(b"\xe2")
    job.completion_obj.is_complete.side_effect = [False, True]

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
    job.completion_obj.is_complete.side_effect = [False, True]

    def tick(interval: float) -> None:
        stdout.write_bytes(b"short\n")

    monkeypatch.setattr("hpcflow.sdk.core.workflow.time.sleep", tick)
    Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)
    assert capsys.readouterr().out == "long initial output\nshort\n"


def test_follow_propagates_read_and_completion_errors(tmp_path: Path) -> None:
    job = _make_jobscript(tmp_path)
    job.completion_obj.is_complete.side_effect = RuntimeError("completion failed")
    with pytest.raises(RuntimeError, match="completion failed"):
        Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)
    job.completion_obj.is_complete.side_effect = None
    job.get_stdout_path().mkdir()
    with pytest.raises((PermissionError, IsADirectoryError)):
        Workflow._follow_jobscripts(_make_workflow(), [job], quiet=True)


@pytest.mark.parametrize("delete", [False, True])
@pytest.mark.parametrize("follow", [False, True])
@pytest.mark.parametrize("wait", [False, True])
@pytest.mark.parametrize("cancel", [False, True])
def test_submit_follow_and_delete_imply_wait(
    follow: bool, wait: bool, cancel: bool, delete: bool
) -> None:
    workflow = _make_workflow()
    workflow._store = Mock()
    workflow._store.cached_load.return_value = nullcontext()
    workflow.batch_update.return_value = nullcontext()
    workflow._store.parameters_metadata_cache.return_value = nullcontext()
    workflow._store.parameters_array_cache.return_value = nullcontext()
    workflow._store.cache_ctx.return_value = nullcontext()
    workflow._submit.return_value = [], {0: [0, 1]}
    workflow.wait.return_value = True
    result = Workflow.submit(
        workflow,
        follow=follow,
        wait=wait,
        cancel=cancel,
        delete=delete,
        return_idx=True,
        status=False,
        quiet=True,
    )
    assert result == {0: [0, 1]}
    assert workflow._submit.call_args.kwargs["follow"] == follow
    if cancel:
        workflow.cancel.assert_called_once_with(status=False, quiet=True)
        if delete:
            # wait for cancelled jobscripts to stop before deleting:
            workflow.wait.assert_called_once_with({0: [0, 1]}, quiet=True)
        else:
            workflow.wait.assert_not_called()
    elif follow or wait or delete:
        workflow.wait.assert_called_once_with({0: [0, 1]}, quiet=True, follow=follow)
    else:
        workflow.wait.assert_not_called()
    if delete:
        workflow._delete_no_confirm.assert_called_once_with()
    else:
        workflow._delete_no_confirm.assert_not_called()


@pytest.mark.parametrize("cancel", [False, True])
def test_submit_delete_skipped_if_wait_interrupted(cancel: bool) -> None:
    workflow = _make_workflow()
    workflow._store = Mock()
    workflow._store.cached_load.return_value = nullcontext()
    workflow.batch_update.return_value = nullcontext()
    workflow._store.parameters_metadata_cache.return_value = nullcontext()
    workflow._store.parameters_array_cache.return_value = nullcontext()
    workflow._store.cache_ctx.return_value = nullcontext()
    workflow._submit.return_value = [], {0: [0]}
    workflow.wait.return_value = False
    Workflow.submit(workflow, delete=True, cancel=cancel, status=False, quiet=True)
    workflow.wait.assert_called_once()
    workflow._delete_no_confirm.assert_not_called()


def test_submit_delete_skipped_on_submission_failure() -> None:
    workflow = _make_workflow()
    workflow._store = Mock()
    workflow._store.cached_load.return_value = nullcontext()
    workflow.batch_update.return_value = nullcontext()
    workflow._store.parameters_metadata_cache.return_value = nullcontext()
    workflow._store.parameters_array_cache.return_value = nullcontext()
    workflow._store.cache_ctx.return_value = nullcontext()
    workflow._submit.return_value = [Mock(message="failed")], {}
    with pytest.raises(WorkflowSubmissionFailure):
        Workflow.submit(workflow, delete=True, status=False, quiet=True)
    workflow.wait.assert_not_called()
    workflow._delete_no_confirm.assert_not_called()


def test_wait_follow_dispatch_and_interrupt(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    workflow = _make_workflow()
    job = _make_jobscript(tmp_path)
    workflow.submissions = [Mock(jobscripts=[job])]
    workflow._follow_jobscripts.side_effect = KeyboardInterrupt
    assert Workflow.wait(workflow, {0: [0]}, follow=True) is False
    workflow._follow_jobscripts.assert_called_once_with([job], quiet=False, follow=True)
    workflow.cancel.assert_not_called()
    assert "workflow execution will continue" in capsys.readouterr().out


def test_wait_empty_selection_does_not_follow_other_jobs() -> None:
    workflow = _make_workflow()
    assert Workflow.wait(workflow, {}, quiet=True, follow=True) is True
    workflow._follow_jobscripts.assert_not_called()


def test_wait_follow_finds_active_jobscripts(tmp_path: Path) -> None:
    workflow = _make_workflow()
    job = _make_jobscript(tmp_path)
    submission = Mock(index=0, jobscripts=[job])
    submission.get_active_jobscripts.return_value = {0: {}}
    workflow.submissions = [submission]
    assert Workflow.wait(workflow, quiet=True, follow=True) is True
    workflow._follow_jobscripts.assert_called_once_with([job], quiet=True, follow=True)


@pytest.mark.parametrize("explicit_selection", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_wait_follow_rejects_bare_follow_for_arrays_before_following(
    tmp_path: Path, explicit_selection: bool, mixed: bool
) -> None:
    workflow = _make_workflow()
    jobs = [_make_jobscript(tmp_path, array=True)]
    if mixed:
        jobs.insert(0, _make_jobscript(tmp_path, index=1))
    workflow.submissions = [Mock(index=0, jobscripts=jobs)]
    selection = {0: list(range(len(jobs)))} if explicit_selection else None
    with pytest.raises(ValueError, match="specifying the array"):
        Workflow.wait(workflow, selection, follow=True)
    workflow._follow_jobscripts.assert_not_called()


@pytest.mark.parametrize("follow", [0, 2, "all"])
def test_wait_follow_array_spec_dispatch(tmp_path: Path, follow: FollowSpec) -> None:
    workflow = _make_workflow()
    job = _make_jobscript(tmp_path, array=True)
    workflow.submissions = [Mock(index=0, jobscripts=[job])]
    Workflow.wait(workflow, {0: [0]}, follow=follow, quiet=True)
    workflow._follow_jobscripts.assert_called_once_with([job], quiet=True, follow=follow)


def test_wait_follow_ignores_finished_array_jobs(tmp_path: Path) -> None:
    workflow = _make_workflow()
    direct = _make_jobscript(tmp_path)
    queued = _make_jobscript(tmp_path, index=1, array=True)
    queued.all_EARs = [Mock(status=EARStatus.success)]
    queued.get_active_states.return_value = {}
    workflow.submissions = [Mock(index=0, jobscripts=[direct, queued])]
    Workflow.wait(workflow, follow=True, quiet=True)
    workflow._follow_jobscripts.assert_called_once_with([direct], quiet=True, follow=True)


@pytest.mark.parametrize("new_submission", [False, True])
@pytest.mark.parametrize("mixed", [False, True])
def test_submit_follow_rejects_bare_follow_for_arrays_before_launch(
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
    with pytest.raises(ValueError, match="specifying the array"):
        Workflow._submit(workflow, follow=True)
    for submission in submissions:
        submission.submit.assert_not_called()


def test_follow_zero_is_enabled_in_submit() -> None:
    workflow = _make_workflow()
    workflow._store = Mock()
    workflow._store.cached_load.return_value = nullcontext()
    workflow.batch_update.return_value = nullcontext()
    workflow._store.parameters_metadata_cache.return_value = nullcontext()
    workflow._store.parameters_array_cache.return_value = nullcontext()
    workflow._store.cache_ctx.return_value = nullcontext()
    workflow._submit.return_value = [], {0: [0]}
    workflow.wait.return_value = True
    Workflow.submit(workflow, follow=0, status=False, quiet=True)
    workflow.wait.assert_called_once_with({0: [0]}, quiet=True, follow=0)


@pytest.mark.parametrize("method", ["make", "demo", "existing"])
@pytest.mark.parametrize("return_idx", [False, True])
@pytest.mark.parametrize("flag", [False, True])
def test_app_forwards_follow_and_delete(
    method: str, return_idx: bool, flag: bool
) -> None:
    app = Mock(spec=BaseApp)
    workflow = Mock(spec=Workflow)
    app._make_workflow.return_value = workflow
    app._make_demo_workflow.return_value = workflow
    app.Workflow = Mock(return_value=workflow)
    workflow.submit.return_value = {0: [0]}
    if method == "make":
        BaseApp._make_and_submit_workflow(
            app, "workflow.yaml", follow=flag, delete=flag, return_idx=return_idx
        )
    elif method == "demo":
        BaseApp._make_and_submit_demo_workflow(
            app, "workflow.yaml", follow=flag, delete=flag, return_idx=return_idx
        )
    else:
        BaseApp._submit_workflow(
            app, "workflow", follow=flag, delete=flag, return_idx=return_idx
        )
    assert workflow.submit.call_args.kwargs["follow"] == flag
    assert workflow.submit.call_args.kwargs["delete"] == flag
