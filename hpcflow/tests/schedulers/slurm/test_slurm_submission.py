from pathlib import Path
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow


@pytest.mark.slurm
def test_workflow_1(tmp_path: Path, null_config):
    hf.config.add_scheduler("slurm")
    wk = make_test_data_YAML_workflow("workflow_1_slurm.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == "201"


@pytest.mark.slurm
def test_workflow_2(tmp_path: Path, null_config):
    hf.config.add_scheduler("slurm")
    wk = make_test_data_YAML_workflow("workflow_2_slurm.yaml", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    bash_location = wk.tasks[0].elements[0].outputs.bash_location
    assert isinstance(bash_location, hf.ElementParameter)
    assert "bash" in bash_location.value


def test_slurm_partition_select():
    slurm = hf.SlurmPosix()
    resources = hf.ElementResources()
    config = {"partitions": {"serial": {"num_nodes": [1,1,1], "num_cores": [1,1,1], "num_cores_per_node": [1,1,1]}}}
    slurm.process_resources(resources, config)
    assert resources.SLURM_partition == "serial"
