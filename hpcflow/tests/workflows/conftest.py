from __future__ import annotations

from typing import TYPE_CHECKING


import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_test_data_YAML_workflow

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def workflow_1(tmp_path: Path):
    return make_test_data_YAML_workflow("workflow_1.yaml", path=tmp_path)


@pytest.fixture
def workflow_1_add_sub(workflow_1):
    workflow_1.add_submission()
    return workflow_1


def assert_workflow_1_success(workflow):
    p2 = workflow.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == "201"


@pytest.fixture
def wk_1_repeats_2(tmp_path: Path):
    return make_test_data_YAML_workflow(
        "workflow_1.yaml", path=tmp_path, updates={("tasks", 0, "repeats"): 2}
    )


@pytest.fixture
def wk_1_reps_2_add_sub(wk_1_repeats_2):
    wk_1_repeats_2.add_submission()
    return wk_1_repeats_2


def assert_wk_1_repeats_success(workflow):
    for element in workflow.tasks[0].elements:
        p2 = element.outputs.p2
        assert isinstance(p2, hf.ElementParameter)
        assert p2.value == "201"


@pytest.fixture
def workflow_2(tmp_path: Path):
    return make_test_data_YAML_workflow("workflow_2.yaml", path=tmp_path)


@pytest.fixture
def workflow_2_sub(workflow_2):
    return workflow_2.add_submission()


@pytest.fixture
def wk_skip(tmp_path):
    return make_test_data_YAML_workflow("workflow_skip.yaml", path=tmp_path)


@pytest.fixture
def wk_sleep(tmp_path):
    return make_test_data_YAML_workflow("workflow_sleep.yaml", path=tmp_path)
