from __future__ import annotations

from unittest.mock import Mock

import pytest

from hpcflow.app import app as hf


@pytest.mark.parametrize(
    ("containerised", "modify_js"),
    [(False, False), (False, True), (True, False), (True, True)],
)
@pytest.mark.parametrize("return_idx", [False, True])
@pytest.mark.parametrize(
    "api_name",
    ["make_and_submit_workflow", "make_and_submit_demo_workflow", "submit_workflow"],
)
def test_containerised_passed_to_workflow_submit(
    tmp_path, monkeypatch, api_name, return_idx, containerised, modify_js
):
    """Forward container preparation options through every application submission API."""
    workflow = hf.Workflow.from_template(hf.WorkflowTemplate(name="w1"), path=tmp_path)
    indices = {0: [0]}
    submit = Mock(return_value=indices)
    monkeypatch.setattr(hf.Workflow, "submit", submit)
    if api_name == "submit_workflow":
        args = {"workflow_path": workflow.path}
    else:
        make_name = (
            "_make_workflow"
            if api_name == "make_and_submit_workflow"
            else "_make_demo_workflow"
        )
        monkeypatch.setattr(hf, make_name, Mock(return_value=workflow))
        args = (
            {"template_file_or_str": "template.yaml"}
            if api_name == "make_and_submit_workflow"
            else {"workflow_name": "workflow_1"}
        )
    result = getattr(hf, api_name)(
        **args, containerised=containerised, return_idx=return_idx, modify_js=modify_js
    )
    submit.assert_called_once()
    assert submit.call_args.kwargs["containerised"] is containerised
    assert submit.call_args.kwargs["modify_js"] is modify_js
    if api_name == "submit_workflow":
        assert result == (indices if return_idx or containerised else None)
    else:
        assert result == (
            (workflow, indices) if return_idx or containerised else workflow
        )
