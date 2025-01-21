import pytest
from hpcflow.app import app as hf


def test_SGE_process_resources_multi_core_with_parallel_env(null_config):

    scheduler_config = {
        "parallel_environments": {
            None: {"num_cores": [1, 1, 1]},  # [start, step, stop]
            "my_parallel_env": {"num_cores": [2, 1, 32]},
        }
    }

    scheduler = hf.SGEPosix()
    resources = hf.ElementResources(num_cores=2, SGE_parallel_env="my_parallel_env")

    scheduler.process_resources(resources, scheduler_config)

    assert resources.num_cores == 2
    assert resources.SGE_parallel_env == "my_parallel_env"


def test_SGE_process_resources_raises_on_single_core_with_parallel_env(null_config):

    scheduler_config = {
        "parallel_environments": {
            None: {"num_cores": [1, 1, 1]},  # [start, step, stop]
            "my_parallel_env": {"num_cores": [2, 1, 32]},
        }
    }

    scheduler = hf.SGEPosix()
    resources = hf.ElementResources(num_cores=1, SGE_parallel_env="my_parallel_env")

    with pytest.raises(ValueError):
        scheduler.process_resources(resources, scheduler_config)
