import pytest
from hpcflow.sdk.core.utils import get_file_context
from hpcflow.app import app as hf


@pytest.mark.integration
@pytest.mark.parametrize("modify_js", [False, True])
def test_api_submit_after_containerised_preparation(tmp_path, monkeypatch, modify_js):
    prompts = []

    def confirm(prompt):
        prompts.append(prompt)
        return "y"

    monkeypatch.setattr("builtins.input", confirm)
    wk = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
    assert isinstance(wk, hf.Workflow)
    plan = wk.submit(containerised=True, status=False)
    assert len(plan["jobscripts"]) == 1
    assert not wk.submissions[0].submitted_jobscripts
    wk.submit(wait=True, add_to_known=False, status=False, modify_js=modify_js)
    assert len(prompts) == (1 if modify_js else 0)
    assert wk.submissions[0].submitted_jobscripts == (0,)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == "201"


@pytest.mark.integration
def test_api_make_and_submit_workflow(tmp_path):
    with get_file_context("hpcflow.tests.data", "workflow_1.yaml") as file_path:
        wk = hf.make_and_submit_workflow(
            file_path,
            path=tmp_path,
            status=False,
            add_to_known=False,
            wait=True,
        )
        p2 = wk.tasks[0].elements[0].outputs.p2
        assert isinstance(p2, hf.ElementParameter)
        assert p2.value == "201"


@pytest.mark.integration
def test_api_make_and_submit_demo_workflow(tmp_path):
    wk = hf.make_and_submit_demo_workflow(
        "workflow_1",
        path=tmp_path,
        status=False,
        add_to_known=False,
        wait=True,
    )
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == "201"
