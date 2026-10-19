"""Tests concerning the directory structure of a created or submitted workflow"""

import os
from pathlib import Path
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import (
    make_test_data_YAML_workflow,
    make_workflow_to_run_command,
)


@pytest.mark.integration
@pytest.mark.parametrize("exit_code", [0, 1])
@pytest.mark.parametrize("delete_execute_dir", [True, False])
def test_execute_directory_lifecycle(
    tmp_path: Path, exit_code: int, delete_execute_dir: bool
):
    result_file = hf.FileSpec(label="result", name="result.txt")
    schema = hf.TaskSchema(
        objective="directory_lifecycle",
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Set-Content result.txt 'saved'"
                        if os.name == "nt"
                        else "echo saved > result.txt"
                    )
                ],
                requires_dir=True,
            ),
            hf.Action(
                commands=[
                    hf.Command(
                        "Get-Content result.txt" if os.name == "nt" else "cat result.txt"
                    ),
                    hf.Command(f"exit {exit_code}"),
                ],
                requires_dir=True,
                output_files=[result_file],
                clean_up=[result_file],
            ),
        ],
        save_files=["result"],
    )
    wk = hf.Workflow.from_template_data(
        template_name="directory_lifecycle",
        path=tmp_path,
        tasks=[hf.Task(schema=schema)],
        config={"delete_execute_dir": delete_execute_dir},
    )
    wk.add_submission()
    directories = wk.get_run_directories()
    assert all(path is not None and not path.exists() for path in directories)
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert runs[0].success
    assert runs[1].success is (exit_code == 0)
    assert all(
        path is not None and path.exists() is (exit_code != 0 or not delete_execute_dir)
        for path in directories
    )
    saved = wk.task_artifacts_path / "result.txt"
    assert saved.read_text().strip() == "saved"
    assert runs[0].dir_diff is not None
    assert "result.txt" in runs[0].dir_diff.files_created
    if not delete_execute_dir:
        final_directory = directories[-1]
        assert final_directory is not None
        assert not (final_directory / "result.txt").exists()


@pytest.mark.integration
@pytest.mark.parametrize(
    ("task_save_files", "expected_files"),
    [
        (None, {"retained.txt"}),
        (["discarded"], {"discarded.txt"}),
        (True, {"retained.txt", "discarded.txt"}),
        (False, set()),
        ([], set()),
    ],
)
def test_task_save_files_policy(
    tmp_path: Path,
    task_save_files: bool | list[str] | None,
    expected_files: set[str],
):
    retained_file = hf.FileSpec(label="retained", name="retained.txt")
    discarded_file = hf.FileSpec(label="discarded", name="discarded.txt")
    command = (
        "Set-Content retained.txt 'retained'; " "Set-Content discarded.txt 'discarded'"
        if os.name == "nt"
        else "printf retained > retained.txt; printf discarded > discarded.txt"
    )
    schema = hf.TaskSchema(
        objective="file_retention",
        save_files=["retained"],
        actions=[
            hf.Action(
                commands=[hf.Command(command)],
                output_files=[retained_file, discarded_file],
            )
        ],
    )
    task = hf.Task(schema=schema, save_files=task_save_files)
    wk = hf.Workflow.from_template_data(
        template_name="file_retention",
        path=tmp_path,
        tasks=[task],
    )

    wk.submit(wait=True, add_to_known=False, status=False)

    saved_files = {path.name for path in wk.task_artifacts_path.rglob("*.txt")}
    assert saved_files == expected_files


@pytest.mark.integration
@pytest.mark.parametrize("exit_code", [0, 1])
@pytest.mark.parametrize(
    ("task_save_files", "expected_files"),
    [
        (None, {"a.log", "b.log", "declared.txt"}),
        (["path:other.txt"], {"other.txt"}),
    ],
)
def test_save_files_path_entries(
    tmp_path: Path,
    exit_code: int,
    task_save_files: list[str] | None,
    expected_files: set[str],
):
    """Working-directory files selected by `path:` entries are saved once, when the
    iteration's final run completes, regardless of its exit code."""
    declared = hf.FileSpec(label="declared", name="declared.txt")
    if os.name == "nt":
        cmd_1 = (
            "Set-Content a.log 'a'; New-Item -ItemType Directory sub | Out-Null; "
            "Set-Content sub/b.log 'b'; Set-Content other.txt 'other'; "
            "Set-Content declared.txt 'declared'"
        )
        cmd_2 = "Add-Content a.log 'a2'"
    else:
        cmd_1 = (
            "echo a > a.log; mkdir sub; echo b > sub/b.log; echo other > other.txt; "
            "echo declared > declared.txt"
        )
        cmd_2 = "echo a2 >> a.log"
    schema = hf.TaskSchema(
        objective="save_files_paths",
        save_files=["declared", "path:*.log", "path:sub/b.log", "path:missing.txt"],
        actions=[
            hf.Action(
                commands=[hf.Command(cmd_1)],
                output_files=[declared],
                requires_dir=True,
            ),
            hf.Action(
                commands=[hf.Command(cmd_2), hf.Command(f"exit {exit_code}")],
                requires_dir=True,
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="save_files_paths",
        path=tmp_path,
        tasks=[hf.Task(schema=schema, save_files=task_save_files)],
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    saved = {
        path.name
        for path in wk.task_artifacts_path.rglob("*")
        if path.is_file() and path.suffix in {".log", ".txt"}
    }
    assert saved == expected_files
    if "a.log" in expected_files:
        # saved after the final action modified it:
        a_log = next(wk.task_artifacts_path.rglob("a.log"))
        assert a_log.read_text().split() == ["a", "a2"]
        b_log = next(wk.task_artifacts_path.rglob("b.log"))
        assert b_log.parent.name == "sub"
    directories = [i for i in wk.get_run_directories() if i is not None]
    assert all(path.exists() is (exit_code != 0) for path in directories)


@pytest.mark.integration
def test_std_stream_file_not_created(tmp_path):
    """Normally, the app standard stream file should not be written."""
    wk = make_test_data_YAML_workflow("workflow_1.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    run = wk.get_all_EARs()[0]
    std_stream_path = run.get_app_std_path()
    assert not std_stream_path.is_file()


@pytest.mark.integration
def test_std_stream_file_created_on_exception_raised(tmp_path):
    command = 'wkflow_app --std-stream "$HPCFLOW_RUN_STD_PATH" internal noop --raise'
    wk = make_workflow_to_run_command(command=command, path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    run = wk.get_all_EARs()[0]
    std_stream_path = run.get_app_std_path()
    assert std_stream_path.is_file()
    assert "ValueError: internal noop raised!" in std_stream_path.read_text()
