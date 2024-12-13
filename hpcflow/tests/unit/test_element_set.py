from __future__ import annotations
from typing import TYPE_CHECKING
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import MalformedNestingOrderPath

if TYPE_CHECKING:
    from pathlib import Path
    from hpcflow.sdk.core.parameters import Parameter
    from hpcflow.sdk.core.types import ResourceSpecArgs
    from hpcflow.sdk.core.workflow import Workflow


@pytest.fixture
def null_config(tmp_path: Path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.fixture
def param_p1() -> Parameter:
    return hf.Parameter("p1")


@pytest.fixture
def param_p2() -> Parameter:
    return hf.Parameter("p2")


@pytest.fixture
def param_p3() -> Parameter:
    return hf.Parameter("p3")


@pytest.fixture
def workflow_w1(
    null_config, tmp_path: Path, param_p1: Parameter, param_p2: Parameter
) -> Workflow:
    s1 = hf.TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p2])
    s2 = hf.TaskSchema("t2", actions=[], inputs=[param_p2])

    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = hf.Task(schema=s2, nesting_order={"inputs.p2": 1})

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def workflow_w2(workflow_w1: Workflow) -> Workflow:
    """Add another element set to the second task."""
    workflow_w1.tasks.t2.add_elements(nesting_order={"inputs.p2": 1})
    return workflow_w1


def test_resources_init_equivalence_dict_list_of_obj() -> None:
    es1 = hf.ElementSet(resources={"any": {"num_cores": 1}})
    es2 = hf.ElementSet(resources=[hf.ResourceSpec(scope="any", num_cores=1)])
    assert es1 == es2


def test_resources_init_equivalence_list_list_of_obj() -> None:
    res_1_kwargs: ResourceSpecArgs = {"scope": "any", "num_cores": 1}
    es1 = hf.ElementSet(resources=[res_1_kwargs])
    es2 = hf.ElementSet(resources=[hf.ResourceSpec(**res_1_kwargs)])
    assert es1 == es2


def test_resources_init_equivalence_list_of_obj_resource_list_obj() -> None:
    res_1_kwargs: ResourceSpecArgs = {"scope": "any", "num_cores": 1}
    es1 = hf.ElementSet(resources=[hf.ResourceSpec(**res_1_kwargs)])
    es2 = hf.ElementSet(resources=hf.ResourceList([hf.ResourceSpec(**res_1_kwargs)]))
    assert es1 == es2


def test_repeats_single_int_equivalence(null_config) -> None:
    es1 = hf.ElementSet(repeats=2)
    es2 = hf.ElementSet(repeats=[{"name": "", "number": 2, "nesting_order": 0}])
    assert es1 == es2


def test_merge_envs(null_config) -> None:
    envs = {"my_env": {"version": "1.0"}}
    es = hf.ElementSet(environments=envs)
    assert es.resources.get(scope=hf.ActionScope.any()).environments == envs


def test_merge_envs_existing_any_resources(null_config) -> None:
    envs = {"my_env": {"version": "1.0"}}
    num_cores = 2
    es = hf.ElementSet(resources={"any": {"num_cores": num_cores}}, environments=envs)
    assert es.resources.get(scope=hf.ActionScope.any()).environments == envs
    assert es.resources.get(scope=hf.ActionScope.any()).num_cores == num_cores


def test_merge_envs_resource_envs_precedence(null_config) -> None:
    envs = {"my_env": {"version": "1.0"}}
    res_envs = {"other_env": {"version": "2.0"}}
    es = hf.ElementSet(resources={"any": {"environments": res_envs}}, environments=envs)
    assert es.resources.get(scope=hf.ActionScope.any()).environments == res_envs


def test_merge_envs_no_envs_with_resource_envs(null_config) -> None:
    envs = {"my_env": {"version": "1.0"}}
    es = hf.ElementSet(resources={"any": {"environments": envs}})
    assert es.resources.get(scope=hf.ActionScope.any()).environments == envs


def test_raise_env_and_envs_specified(null_config) -> None:
    with pytest.raises(ValueError):
        hf.ElementSet(env_preset="my_preset", environments={"my_env": {"version": 1}})


def test_nesting_order_paths_raise(null_config) -> None:
    with pytest.raises(MalformedNestingOrderPath):
        hf.ElementSet(nesting_order={"bad_path.p1": 1})


def test_nesting_order_paths_no_raise(null_config) -> None:
    hf.ElementSet(nesting_order={"inputs.p1": 1, "resources.any": 2, "repeats": 3})
