"""Tests concerning the directory structure of a created or submitted workflow"""

import os
from pathlib import Path
import pytest

from hpcflow.sdk.core.test_utils import (
    make_test_data_YAML_workflow,
    make_workflow_to_run_command,
)


@pytest.mark.integration
def test_std_stream_file_not_created(tmp_path, new_null_config):
    """Normally, the app standard stream file should not be written."""
    wk = make_test_data_YAML_workflow("workflow_1.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    run = wk.get_all_EARs()[0]
    std_stream_path = run.get_app_std_path()
    assert not std_stream_path.is_file()


@pytest.mark.integration
def test_std_stream_file_created_on_exception_raised(tmp_path, new_null_config):
    command = 'wkflow_app --std-stream "$HPCFLOW_RUN_STD_PATH" internal noop --raise'
    wk = make_workflow_to_run_command(command=command, path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    run = wk.get_all_EARs()[0]
    std_stream_path = run.get_app_std_path()
    assert std_stream_path.is_file()
    assert "ValueError: internal noop raised!" in std_stream_path.read_text()
