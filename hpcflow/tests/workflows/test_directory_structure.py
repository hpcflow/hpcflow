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
                save_files=[result_file],
                clean_up=[result_file],
            ),
        ],
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
