import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow


@pytest.mark.integration
def test_workflow_1_zip(tmp_path, new_null_config):
    wk = make_test_data_YAML_workflow("workflow_1.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False, status=False)

    zip_path = wk.zip(path=tmp_path)
    wkz = hf.Workflow(zip_path)
    assert wkz.tasks[0].elements[0].outputs.p2.value == "201"
