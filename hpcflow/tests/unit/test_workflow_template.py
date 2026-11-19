from textwrap import dedent
from pathlib import Path
import json
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    MissingVariableSubstitutionError,
    UnavailableInputSource,
)
from hpcflow.sdk.core.test_utils import (
    make_test_data_YAML_workflow_template,
)


def test_merge_template_level_resources_into_element_set():
    seed = 0
    wkt = hf.WorkflowTemplate(
        name="w1",
        tasks=[hf.Task(schema=[hf.task_schemas.test_t1_ps])],
        resources={"any": {"num_cores": 1, "random_seed": seed}},
    )
    assert wkt.tasks[0].element_sets[0].resources == hf.ResourceList.from_json_like(
        {"any": {"num_cores": 1, "random_seed": seed}}
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize(
    "mode", ["shared", "local", "sequence", "task_input", "task_output", "explicit"]
)
def test_workflow_inputs(tmp_path: Path, store: str, mode: str) -> None:
    schema = hf.TaskSchema(
        objective="consumer",
        inputs=[hf.SchemaInput("p1", default_value=99)],
        actions=[hf.Action(commands=[hf.Command("echo <<parameter:p1>>")])],
    )
    tasks = []
    if mode == "task_input":
        tasks.append(hf.Task(schema, inputs={"p1": 20}))
    elif mode in ("task_output", "explicit"):
        tasks.append(
            hf.Task(
                hf.TaskSchema(
                    objective="producer",
                    outputs=[hf.SchemaOutput("p1")],
                    actions=[
                        hf.Action(
                            commands=[hf.Command("echo 20", stdout="<<parameter:p1>>")]
                        )
                    ],
                )
            )
        )
    tasks.append(
        hf.Task(
            schema,
            inputs={"p1": 20} if mode in ("local", "explicit") else {},
            sequences=(
                [hf.ValueSequence("inputs.p1", [20, 30])] if mode == "sequence" else []
            ),
            input_sources=(
                {"p1": [hf.InputSource.workflow()]} if mode == "explicit" else {}
            ),
        )
    )
    template = hf.WorkflowTemplate(
        "shared_inputs", inputs={"p1": 10, "unused_parameter": 42}, tasks=tasks
    )
    wk = hf.Workflow.from_template(template, path=tmp_path, store=store)
    if mode == "task_output":
        wk.add_submission()
        output_idx = wk.tasks[0].elements[0].get_data_idx()["outputs.p1"]
        wk.set_parameter_value(output_idx, 20, commit=True)
    expected = [10] if mode in ("shared", "explicit") else [20]
    if mode == "sequence":
        expected.append(30)
    for workflow in (wk, hf.Workflow(wk.path)):
        assert [el.get("inputs.p1") for el in workflow.tasks[-1].elements] == expected
        assert workflow.template._inputs["p1"].value == 10
        if mode in ("shared", "explicit"):
            element = workflow.tasks[-1].elements[0]
            assert element.input_sources["inputs.p1"] == hf.InputSource.workflow()
            assert element.get_parameter_sources("inputs.p1", as_strings=True) == {
                "inputs.p1": "workflow"
            }
    assert template._inputs["p1"].value == 10
    assert template._inputs["p1"]._value_group_idx is None


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("list_form", [False, True])
def test_workflow_inputs_yaml(tmp_path: Path, store: str, list_form: bool) -> None:
    inputs_yaml = (
        "inputs:\n  - parameter: p1[label].nested.value\n    value: 10\n"
        if list_form
        else "inputs:\n  p1[label].nested.value: 10\n"
    )
    template = hf.WorkflowTemplate.from_YAML_string(inputs_yaml + dedent("""\
            name: shared_yaml
            template_components:
              task_schemas:
                - objective: shared_yaml_consumer
                  inputs:
                    - parameter: p1
                      multiple: true
                      labels: {label: {}}
                  actions:
                    - commands:
                        - command: echo <<parameter:p1[label]>>
            tasks:
              - schema: shared_yaml_consumer
                inputs:
                  p1[label]: {nested: {other: 20}}
        """))
    wk = hf.Workflow.from_template(template, path=tmp_path, store=store)
    for workflow in (wk, hf.Workflow(wk.path)):
        assert workflow.tasks[0].elements[0].get("inputs.p1[label]") == {
            "nested": {"value": 10, "other": 20}
        }


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_workflow_inputs_reuse_and_add_task(tmp_path: Path, store: str) -> None:
    schema = hf.TaskSchema("consumer", inputs=[hf.SchemaInput("p1")])
    template = hf.WorkflowTemplate(
        "shared", inputs=[hf.InputValue("p1", [1, 2])], tasks=[hf.Task(schema)]
    )
    for name in ("first", "second"):
        wk = hf.Workflow.from_template(template, path=tmp_path, name=name, store=store)
        wk = hf.Workflow(wk.path)
        wk.add_task(hf.Task(schema, input_sources={"p1": [hf.InputSource.workflow()]}))
        assert wk.tasks[1].elements[0].get("inputs.p1") == [1, 2]
        assert (
            wk.tasks[0].elements[0].get_data_idx()["inputs.p1"]
            == wk.tasks[1].elements[0].get_data_idx()["inputs.p1"]
        )


def test_workflow_inputs_duplicate_paths() -> None:
    with pytest.raises(ValueError, match="multiple value definitions"):
        hf.WorkflowTemplate(
            "duplicate", inputs=[hf.InputValue("p1", 1), hf.InputValue("p1", 2)]
        )


def test_workflow_input_source_round_trip() -> None:
    source = hf.InputSource.workflow()
    assert hf.InputSource.from_string(source.to_string()) == source
    assert hf.InputSource.from_json_like(source.to_json_like()[0]) == source
    with pytest.raises(ValueError):
        hf.InputSource.from_string("workflow.invalid")


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_workflow_inputs_json(tmp_path: Path, store: str) -> None:
    wk = hf.Workflow.from_JSON_string(
        json.dumps(
            {
                "name": "shared_json",
                "inputs": {"p1": 10},
                "template_components": {
                    "task_schemas": [
                        {
                            "objective": "shared_json_consumer",
                            "inputs": [{"parameter": "p1"}],
                        }
                    ]
                },
                "tasks": [
                    {
                        "schema": "shared_json_consumer",
                        "input_sources": {"p1": "workflow"},
                    }
                ],
            }
        ),
        path=tmp_path,
        store=store,
    )
    assert hf.Workflow(wk.path).tasks[0].elements[0].get("inputs.p1") == 10


def test_unavailable_workflow_input(tmp_path: Path) -> None:
    schema = hf.TaskSchema("consumer", inputs=[hf.SchemaInput("p1", default_value=99)])
    with pytest.raises(UnavailableInputSource):
        hf.Workflow.from_template_data(
            "missing_shared",
            tasks=[hf.Task(schema, input_sources={"p1": [hf.InputSource.workflow()]})],
            path=tmp_path,
        )


def test_unrequired_workflow_input(tmp_path: Path) -> None:
    input_file = hf.FileSpec(label="provided_input", name="provided.txt")
    file_path = tmp_path / "provided.txt"
    file_path.write_text("provided\n", encoding="ascii", newline="\n")
    schema = hf.TaskSchema(
        "unused_input",
        inputs=[hf.SchemaInput("p1")],
        actions=[
            hf.Action(
                commands=[hf.Command("cat <<file:provided_input>>")],
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=input_file,
                        inputs=[hf.Parameter("p1")],
                        script="not_run_in_this_test.py",
                    )
                ],
                environments=[hf.ActionEnvironment("python_env")],
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        "unused_shared",
        inputs={"p1": {"nested": {}}, "p1.nested.value": 10},
        tasks=[
            hf.Task(schema, input_files=[hf.InputFile(input_file, path=str(file_path))])
        ],
        path=tmp_path,
    )
    assert not any(
        key.startswith("inputs.p1") for key in wk.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_workflow_inputs_in_loops(tmp_path: Path, store: str) -> None:
    schema = hf.TaskSchema(
        "looped",
        inputs=[hf.SchemaInput("p1")],
        actions=[hf.Action(commands=[hf.Command("echo <<parameter:p1>>")])],
    )
    wk = hf.Workflow.from_template_data(
        "shared_loop",
        inputs={"p1": 10},
        tasks=[hf.Task(schema)],
        loops=[hf.Loop(name="loop", tasks=[0], num_iterations=2)],
        path=tmp_path,
        store=store,
    )
    with wk.batch_update():
        wk.loops.loop.add_iteration()
    for workflow in (wk, hf.Workflow(wk.path)):
        assert [
            iteration.get("inputs.p1")
            for iteration in workflow.tasks[0].elements[0].iterations
        ] == [10, 10, 10]


def test_equivalence_from_YAML_and_JSON_files():
    wkt_yaml = make_test_data_YAML_workflow_template("workflow_1.yaml")
    wkt_json = make_test_data_YAML_workflow_template("workflow_1.json")
    assert wkt_json == wkt_yaml


def test_reuse(tmp_path):
    """Test we can re-use a template that has already been made persistent."""
    wkt = hf.WorkflowTemplate(name="test", tasks=[])
    wk1 = hf.Workflow.from_template(wkt, name="test_1", path=tmp_path)
    wk2 = hf.Workflow.from_template(wkt, name="test_2", path=tmp_path)


def test_workflow_template_vars(tmp_path):
    num_repeats = 2
    wkt = make_test_data_YAML_workflow_template(
        workflow_name="benchmark_N_elements.yaml",
        variables={"N": num_repeats},
    )
    assert wkt.tasks[0].element_sets[0].repeats[0].number == num_repeats


def test_workflow_template_vars_raise_no_vars(tmp_path):
    # no default value for the variable is provided in `benchmark_N_elements`, so should
    # raise if the variables dict is not passed:
    with pytest.raises(MissingVariableSubstitutionError):
        make_test_data_YAML_workflow_template("benchmark_N_elements.yaml")


def test_workflow_template_vars_defaults_used(tmp_path):
    # `benchmark_script_runner` contains a default value for the variable `N`, so that
    # should be used, since we don't pass any variables:
    wkt = make_test_data_YAML_workflow_template("benchmark_script_runner.yaml")
    assert wkt.tasks[0].element_sets[0].repeats[0].number == 1


def test_workflow_template_vars_False_no_substitution(tmp_path):
    # read a yaml template, check variables are not substituted, when `variables=False`:
    wkt_yaml = dedent("""\
        name: workflow_1
        tasks:
          - schema: test_t1_conditional_OS            
            inputs:
              p1: <<var:my_var>>
    """)
    wkt = hf.WorkflowTemplate.from_YAML_string(wkt_yaml, variables=False)
    assert wkt.tasks[0].element_sets[0].inputs[0].value == "<<var:my_var>>"


def test_workflow_template_task_save_files_from_YAML():
    """A task-level `save_files` policy is a task attribute, not an element-set one."""
    wkt_yaml = dedent("""\
        name: workflow_1
        template_components:
          command_files:
            - label: result
              name:
                name: result.txt
          task_schemas:
            - objective: write_result
              actions:
                - commands:
                    - command: echo hi > result.txt
                  output_files: [result]
        tasks:
          - schema: write_result
            save_files: [result, "path:*.log"]
    """)
    wkt = hf.WorkflowTemplate.from_YAML_string(wkt_yaml)
    assert wkt.tasks[0].save_files == ["result", "path:*.log"]
    assert len(wkt.tasks[0].element_sets) == 1


def test_env_preset_merge_simple():
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


def test_env_preset_merge_simple_list():
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


def test_env_preset_no_merge_existing_env_preset():
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


def test_environments_merge_simple():
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


def test_environments_no_merge_existing_envs():
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


def test_raise_on_env_preset_and_environments():
    with pytest.raises(ValueError):
        wkt = hf.WorkflowTemplate(
            name="test",
            env_presets="my_env_preset",
            environments={"my_env": {"version": 1}},
        )


def test_default_env_preset_used_if_available():
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


def test_update_resources():
    resources_id = 13123
    wkt = make_test_data_YAML_workflow_template(
        "workflow_1.yaml", resources={"resources_id": resources_id}
    )
    assert wkt.resources.get().resources_id == resources_id
    assert wkt.resources.get().random_seed == 0  # check this item still exists


def test_update_config():
    log_level = "debug"
    wkt = make_test_data_YAML_workflow_template(
        "workflow_1.yaml", config={"log_console_level": log_level}
    )
    assert wkt.config["log_console_level"] == log_level


def test_updates():
    inp_val = 999
    wkt = make_test_data_YAML_workflow_template(
        "workflow_1.yaml", updates={("tasks", 0, "inputs", "p1"): inp_val}
    )
    assert wkt.tasks[0].element_sets[0].inputs[0].value == inp_val
