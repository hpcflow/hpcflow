from __future__ import annotations

from pathlib import Path
from unittest.mock import Mock

import pytest

from hpcflow.app import app as hf


@pytest.mark.parametrize("containerised", [False, True])
def test_containerised_passed_to_submission_submit(tmp_path, monkeypatch, containerised):
    """Forward container preparation through Workflow.submit while preserving native results."""
    workflow = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
    assert isinstance(workflow, hf.Workflow)
    submit = Mock(return_value=[] if containerised else [0])
    monkeypatch.setattr(hf.Submission, "submit", submit)
    indices = workflow.submit(
        containerised=containerised, return_idx=True, status=False, quiet=True
    )
    submit.assert_called_once()
    assert submit.call_args.kwargs.get("containerised", False) is containerised
    if containerised:
        assert indices == {
            "schema_version": 1,
            "workflow_id": workflow.id_,
            "workflow_path": str(Path(workflow.path).resolve()),
            "jobscripts": [],
        }
    else:
        assert indices == {0: [0]}
