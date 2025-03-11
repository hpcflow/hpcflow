import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import (
    make_test_data_YAML_workflow_template,
)


def test_merge_template_level_resources_into_element_set(null_config):
    wkt = hf.WorkflowTemplate(
        name="w1",
        tasks=[hf.Task(schema=[hf.task_schemas.test_t1_ps])],
        resources={"any": {"num_cores": 1}},
    )
    assert wkt.tasks[0].element_sets[0].resources == hf.ResourceList.from_json_like(
        {"any": {"num_cores": 1}}
    )


def test_equivalence_from_YAML_and_JSON_files(null_config):
    wkt_yaml = make_test_data_YAML_workflow_template("workflow_1.yaml")
    wkt_json = make_test_data_YAML_workflow_template("workflow_1.json")
    assert wkt_json == wkt_yaml


def test_reuse(null_config, tmp_path):
    """Test we can re-use a template that has already been made persistent."""
    wkt = hf.WorkflowTemplate(name="test", tasks=[])
    wk1 = hf.Workflow.from_template(wkt, name="test_1", path=tmp_path)
    wk2 = hf.Workflow.from_template(wkt, name="test_2", path=tmp_path)


def test_workflow_template_vars(tmp_path, new_null_config):
    num_repeats = 2
    wkt = make_test_data_YAML_workflow_template(
        workflow_name="benchmark_N_elements.yaml",
        variables={"N": num_repeats},
    )
    assert wkt.tasks[0].element_sets[0].repeats[0]["number"] == num_repeats


def test_env_preset_merge_simple(null_config):
    s1 = hf.TaskSchema(
        objective="s1",
        actions=[hf.Action(environments=[hf.ActionEnvironment("my_env")])],
        environment_presets={"my_env_preset": {"my_env": {"version": 1}}},
    )
    wkt = hf.WorkflowTemplate(
        name="test",
        env_presets="my_env_preset",
        tasks=[hf.Task(schema=s1)],
    )
    assert wkt.tasks[0].element_sets[0].env_preset == "my_env_preset"
    assert wkt.tasks[0].element_sets[0].resources[0].environments == {
        "my_env": {"version": 1}
    }


def test_env_preset_merge_simple_list(null_config):
    s1 = hf.TaskSchema(
        objective="s1",
        actions=[hf.Action(environments=[hf.ActionEnvironment("my_env")])],
        environment_presets={"my_env_preset": {"my_env": {"version": 1}}},
    )
    wkt = hf.WorkflowTemplate(
        name="test",
        env_presets=["my_env_preset", "my_other_env_preset"],
        tasks=[hf.Task(schema=s1)],
    )
    assert wkt.tasks[0].element_sets[0].env_preset == "my_env_preset"
    assert wkt.tasks[0].element_sets[0].resources[0].environments == {
        "my_env": {"version": 1}
    }


def test_env_preset_no_merge_existing_env_preset(null_config):
    s1 = hf.TaskSchema(
        objective="s1",
        actions=[hf.Action(environments=[hf.ActionEnvironment("my_env")])],
        environment_presets={
            "env_preset_1": {"my_env": {"version": 1}},
            "env_preset_2": {"my_env": {"version": 2}},
        },
    )
    wkt = hf.WorkflowTemplate(
        name="test",
        env_presets="env_preset_1",
        tasks=[hf.Task(schema=s1, env_preset="env_preset_2")],
    )
    assert wkt.tasks[0].element_sets[0].env_preset == "env_preset_2"
    assert wkt.tasks[0].element_sets[0].resources[0].environments == {
        "my_env": {"version": 2}
    }


def test_environments_merge_simple(null_config):
    s1 = hf.TaskSchema(
        objective="s1",
        actions=[hf.Action(environments=[hf.ActionEnvironment("my_env")])],
    )
    wkt = hf.WorkflowTemplate(
        name="test",
        environments={"my_env": {"version": 1}, "my_other_env": {"version": 2}},
        tasks=[hf.Task(schema=s1)],
    )
    assert wkt.tasks[0].element_sets[0].environments == {"my_env": {"version": 1}}
    assert wkt.tasks[0].element_sets[0].resources[0].environments == {
        "my_env": {"version": 1}
    }


def test_environments_no_merge_existing_envs(null_config):
    s1 = hf.TaskSchema(
        objective="s1",
        actions=[hf.Action(environments=[hf.ActionEnvironment("my_env")])],
    )
    wkt = hf.WorkflowTemplate(
        name="test",
        environments={"my_env": {"version": 1}, "my_other_env": {"version": 2}},
        tasks=[hf.Task(schema=s1, environments={"my_env": {"version": 2}})],
    )
    assert wkt.tasks[0].element_sets[0].environments == {"my_env": {"version": 2}}
    assert wkt.tasks[0].element_sets[0].resources[0].environments == {
        "my_env": {"version": 2}
    }


def test_raise_on_env_preset_and_environments(null_config):
    with pytest.raises(ValueError):
        wkt = hf.WorkflowTemplate(
            name="test",
            env_presets="my_env_preset",
            environments={"my_env": {"version": 1}},
        )


def test_default_env_preset_used_if_available(null_config):
    """Test that if no env_presets or environments are specified at template-level or task
    level, the default (named as an empty string) env preset is used if available."""

    s1 = hf.TaskSchema(
        objective="s1",
        actions=[hf.Action(environments=[hf.ActionEnvironment("my_env")])],
        environment_presets={
            "": {"my_env": {"version": 1}},
            "env_preset_1": {"my_env": {"version": 2}},
        },
    )
    wkt = hf.WorkflowTemplate(
        name="test",
        tasks=[hf.Task(schema=s1)],
    )
    assert wkt.tasks[0].element_sets[0].env_preset == ""
    assert wkt.tasks[0].element_sets[0].resources[0].environments == {
        "my_env": {"version": 1}
    }
