import pytest
from hpcflow.sdk.core.utils import get_file_context
from hpcflow.app import app as hf


@pytest.mark.integration
def test_api_make_and_submit_workflow(null_config, tmp_path):
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
def test_api_make_and_submit_demo_workflow(null_config, tmp_path):
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
