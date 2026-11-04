import asyncio
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock, patch

import psutil
import pytest

from hpcflow.sdk.core.test_utils import (
    _find_run_output_paths,
    report_jobscript_timeout,
    wait_for_array_item_completion,
)
from hpcflow.sdk.submission.jobscript import Jobscript
from hpcflow.sdk.wait.completion import JobscriptCompletion


def _jobscript_with_output_paths(tmp_path: Path, *, combined: bool = False) -> Mock:
    js = Mock(spec=Jobscript)
    stdout_path = tmp_path / "jobscript-stdout.log"
    stderr_path = stdout_path if combined else tmp_path / "jobscript-stderr.log"
    js.get_stdout_path.return_value = stdout_path
    js.get_stderr_path.return_value = stderr_path
    js.get_app_std_path.return_value = tmp_path / "app-std" / "js_0.txt"
    js.completion_obj = JobscriptCompletion(
        tmp_path,
        submission_idx=0,
        jobscript_idx=0,
        num_jobscripts=1,
        array_indices=(0, 1),
    )
    return js


@pytest.mark.asyncio
async def test_array_timeout_reports_exited_process_and_redirected_output(tmp_path: Path):
    js = _jobscript_with_output_paths(tmp_path)
    run_output = tmp_path / "app-std" / "0-249999" / "0-499" / "1.txt"
    run_output.parent.mkdir(parents=True)
    run_output.write_text("run-level traceback", encoding="utf-8")
    with (
        js.get_stdout_path().open("wb") as stdout,
        js.get_stderr_path().open("wb") as stderr,
    ):
        process = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import sys; print('child stdout'); print('child stderr', "
                "file=sys.stderr); sys.exit(7)",
            ],
            stdout=stdout,
            stderr=stderr,
        )
        try:
            assert process.wait(timeout=10) == 7
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()

    with patch(
        "hpcflow.sdk.core.test_utils.psutil.Process",
        side_effect=psutil.NoSuchProcess(process.pid),
    ):
        with pytest.raises(asyncio.TimeoutError) as error:
            with report_jobscript_timeout(js, 1, process.pid):
                await wait_for_array_item_completion(js.completion_obj, 1, timeout=0.01)

    message = str(error.value)
    assert f"array item 1 (PID {process.pid})" in message
    assert "NoSuchProcess" in message
    assert "array item complete=False, jobscript complete=False" in message
    assert "child stdout" in message
    assert "child stderr" in message
    assert "run-level traceback" in message
    assert "FileNotFoundError" in message
    assert isinstance(error.value.__cause__, asyncio.TimeoutError)


def test_jobscript_timeout_reports_combined_output_and_live_process(tmp_path: Path):
    js = _jobscript_with_output_paths(tmp_path, combined=True)
    js.get_stdout_path().write_bytes(b"discarded-prefix" + b"x" * 33_000 + b"output-tail")
    js.completion_obj.mark_array_item_complete(0)
    js.completion_obj.mark_array_item_complete(1)

    with pytest.raises(asyncio.TimeoutError) as error:
        with report_jobscript_timeout(js, 1, os.getpid()):
            raise asyncio.TimeoutError("original timeout")

    message = str(error.value)
    assert f"PID {os.getpid()} (" in message
    assert "array item complete=True, jobscript complete=True" in message
    assert "Jobscript stdout/stderr" in message
    assert "earlier bytes omitted" in message
    assert "discarded-prefix" not in message
    assert message.count("output-tail") == 1
    assert "Could not collect all run output:" in message
    assert str(error.value.__cause__) == "original timeout"


def test_run_output_collection_is_bounded(tmp_path: Path):
    app_std = tmp_path / "app-std"
    run_dir = app_std / "0-249999" / "0-499"
    run_dir.mkdir(parents=True)
    (app_std / "js_0.txt").write_text("jobscript output", encoding="utf-8")
    for idx in range(10):
        (run_dir / f"{idx:02}.txt").write_text(str(idx), encoding="utf-8")

    paths, errors = _find_run_output_paths(app_std)
    assert paths == [run_dir / f"{idx:02}.txt" for idx in range(10)]
    assert not errors

    (run_dir / "10.txt").write_text("10", encoding="utf-8")
    paths, errors = _find_run_output_paths(app_std)
    assert len(paths) == 10
    assert errors == ["Additional run output files omitted after 10."]


def test_jobscript_timeout_does_not_change_other_exceptions(tmp_path: Path):
    js = _jobscript_with_output_paths(tmp_path)
    with report_jobscript_timeout(js, 1, os.getpid()):
        pass

    with pytest.raises(ValueError, match="not a timeout"):
        with report_jobscript_timeout(js, 1, os.getpid()):
            raise ValueError("not a timeout")
