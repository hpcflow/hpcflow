import os
from pathlib import Path
import time

import pytest

from hpcflow.sdk.core.test_utils import make_workflow_to_run_command


@pytest.mark.integration
def test_submit_follow_streams_before_jobscript_finishes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    command = (
        "Write-Output 'FOLLOW_START'; Start-Sleep -Seconds 2; "
        "$host.ui.WriteErrorLine('FOLLOW_ERROR'); Write-Output 'FOLLOW_END'"
        if os.name == "nt"
        else "echo FOLLOW_START; sleep 2; echo FOLLOW_ERROR >&2; echo FOLLOW_END"
    )
    workflow = make_workflow_to_run_command(
        command, tmp_path, resources={"any": {"combine_jobscript_std": False}}
    )
    sleep = time.sleep
    stdout: list[str] = []
    stderr: list[str] = []
    observed_live_output = False

    def observe(interval: float) -> None:
        nonlocal observed_live_output
        captured = capsys.readouterr()
        stdout.append(captured.out)
        stderr.append(captured.err)
        output_so_far = "".join(stdout)
        if "FOLLOW_START" in output_so_far and "FOLLOW_END" not in output_so_far:
            observed_live_output = True
        sleep(interval)

    monkeypatch.setattr("hpcflow.sdk.core.workflow.time.sleep", observe)
    workflow.submit(follow=True, add_to_known=False, status=False, quiet=True)
    captured = capsys.readouterr()
    stdout.append(captured.out)
    stderr.append(captured.err)
    out, err = "".join(stdout), "".join(stderr)
    assert observed_live_output
    assert out.count("FOLLOW_START") == 1
    assert out.count("FOLLOW_END") == 1
    assert err.count("FOLLOW_ERROR") == 1
    assert "FOLLOW_ERROR" not in out
    jobscript = workflow.submissions[0].jobscripts[0]
    assert not jobscript.get_active_states()
    assert out == jobscript.get_stdout()
    assert err == jobscript.get_stderr()


@pytest.mark.integration
@pytest.mark.parametrize("follow", [False, True])
def test_submit_delete_removes_workflow_after_completion(
    tmp_path: Path, capsys: pytest.CaptureFixture[str], follow: bool
) -> None:
    workflow = make_workflow_to_run_command("echo DELETE_ME", tmp_path)
    workflow_path = Path(workflow.path)
    assert workflow_path.exists()
    workflow.submit(
        delete=True, follow=follow, add_to_known=False, status=False, quiet=True
    )
    assert not workflow_path.exists()
    if follow:
        assert "DELETE_ME" in capsys.readouterr().out
