from __future__ import annotations
import copy
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import TYPE_CHECKING

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    MissingInputs,
    WorkflowBatchUpdateFailedError,
    WorkflowNotFoundError,
)
from hpcflow.sdk.core.test_utils import (
    make_workflow,
    P1_parameter_cls as P1,
    make_test_data_YAML_workflow,
)

if TYPE_CHECKING:
    from collections.abc import Iterator
    from hpcflow.sdk.core.actions import Action, ActionEnvironment
    from hpcflow.sdk.core.command_files import FileSpec
    from hpcflow.sdk.core.parameters import Parameter
    from hpcflow.sdk.core.task_schema import TaskSchema
    from hpcflow.sdk.core.workflow import Workflow


@pytest.fixture
def persistent_workflow(null_config) -> Iterator[Workflow]:
    tmp_dir = hf._ensure_user_runtime_dir().joinpath("test_data")
    tmp_dir.mkdir(exist_ok=True)
    wk = make_test_data_YAML_workflow("workflow_1.yaml", path=tmp_dir, overwrite=True)
    yield wk
    wk._delete_no_confirm()


@pytest.mark.skip(reason="Cannot delete the zip file! Something still has a handle.")
def test_workflow_zip(persistent_workflow):
    zip_path = persistent_workflow.zip()

    # check zipped is loadable:
    wk_zipped = hf.Workflow(zip_path)
    assert wk_zipped.tasks[0].elements[0].get("inputs.p1") == 101

    del wk_zipped._store.fs
    del wk_zipped._store
    del wk_zipped

    # delete the zipped workflow:
    Path(zip_path).unlink()


def modify_workflow_metadata_on_disk(workflow: Workflow):
    """Make a non-sense change to the on-disk metadata."""
    assert workflow.store_format == "zarr"
    wk_md = workflow._store.load_metadata()  # type: ignore
    changed_md = copy.deepcopy(wk_md)
    changed_md["new_key"] = "new_value"
    workflow._store._get_root_group(mode="r+").attrs.put(changed_md)  # type: ignore


def make_workflow_w1_with_config_kwargs(
    config_kwargs, path, param_p1: Parameter, param_p2: Parameter
) -> Workflow:
    hf.load_config(**config_kwargs)
    s1 = hf.TaskSchema("ts1", actions=[], inputs=[param_p1], outputs=[param_p2])
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=path)


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.fixture
def empty_workflow(null_config, tmp_path) -> Workflow:
    return hf.Workflow.from_template(hf.WorkflowTemplate(name="w1"), path=tmp_path)


@pytest.fixture
def param_p1(null_config) -> Parameter:
    return hf.Parameter("p1")


@pytest.fixture
def param_p1c(null_config) -> Parameter:
    return hf.Parameter("p1c")


@pytest.fixture
def param_p2() -> Parameter:
    return hf.Parameter("p2")


@pytest.fixture
def param_p3(null_config) -> Parameter:
    return hf.Parameter("p3")


@pytest.fixture
def act_env_1(null_config) -> ActionEnvironment:
    return hf.ActionEnvironment("env_1")


@pytest.fixture
def act_1(null_config, act_env_1: ActionEnvironment) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def act_2(null_config, act_env_1: ActionEnvironment) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p2>> <<parameter:p3>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def file_spec_fs1(
    null_config,
) -> FileSpec:
    return hf.FileSpec(label="file1", name="file1.txt")


@pytest.fixture
def act_3(
    null_config,
    act_env_1: ActionEnvironment,
    param_p2: Parameter,
    file_spec_fs1: FileSpec,
) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        output_file_parsers=[
            hf.OutputFileParser(output=param_p2, output_files=[file_spec_fs1]),
        ],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s1(null_config, param_p1: Parameter, act_1: Action) -> TaskSchema:
    return hf.TaskSchema("ts1", actions=[act_1], inputs=[param_p1])


@pytest.fixture
def schema_s2(
    null_config, param_p2: Parameter, param_p3: Parameter, act_2: Action
) -> TaskSchema:
    return hf.TaskSchema("ts2", actions=[act_2], inputs=[param_p2, param_p3])


@pytest.fixture
def schema_s3(
    null_config, param_p1: Parameter, param_p2: Parameter, act_3: Action
) -> TaskSchema:
    return hf.TaskSchema("ts1", actions=[act_3], inputs=[param_p1], outputs=[param_p2])


@pytest.fixture
def schema_s4(
    null_config,
    param_p1: Parameter,
) -> TaskSchema:
    return hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=param_p1)],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[hf.Command("Write-Output '<<parameter:p1>>'")],
            )
        ],
    )


@pytest.fixture
def schema_s4c(
    null_config,
    param_p1c: Parameter,
) -> TaskSchema:
    return hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=param_p1c)],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[hf.Command("Write-Output '<<parameter:p1c>>'")],
            )
        ],
    )


@pytest.fixture
def workflow_w1(
    null_config, tmp_path: Path, schema_s3: TaskSchema, param_p1: Parameter
) -> Workflow:
    t1 = hf.Task(schema=schema_s3, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=tmp_path)


def test_make_empty_workflow(null_config, empty_workflow: Workflow):
    assert empty_workflow.path is not None


def test_raise_on_missing_workflow(null_config, tmp_path: Path):
    with pytest.raises(WorkflowNotFoundError):
        hf.Workflow(tmp_path)


def test_add_empty_task(empty_workflow: Workflow, schema_s1: TaskSchema):
    t1 = hf.Task(schema=schema_s1)
    wk_t1 = empty_workflow._add_empty_task(t1)
    assert len(empty_workflow.tasks) == 1 and wk_t1.index == 0 and wk_t1.name == "ts1"


def test_raise_on_missing_inputs_add_first_task(
    empty_workflow: Workflow, schema_s1: TaskSchema, param_p1: Parameter
):
    t1 = hf.Task(schema=schema_s1)
    with pytest.raises(MissingInputs) as exc_info:
        empty_workflow.add_task(t1)

    assert exc_info.value.missing_inputs == (param_p1.typ,)


def test_raise_on_missing_inputs_add_second_task(
    workflow_w1: Workflow, schema_s2: TaskSchema, param_p3: Parameter
):
    t2 = hf.Task(schema=schema_s2)
    with pytest.raises(MissingInputs) as exc_info:
        workflow_w1.add_task(t2)

    assert exc_info.value.missing_inputs == (param_p3.typ,)  # p2 comes from existing task


@pytest.mark.skip(reason="TODO: Not implemented.")
def test_new_workflow_deleted_on_creation_failure():
    pass


def test_WorkflowTemplate_from_YAML_string(null_config):
    wkt_yml = dedent(
        """
        name: simple_workflow

        tasks:
        - schema: dummy_task_1
          element_sets:
          - inputs:
              p2: 201
              p5: 501
            sequences:
              - path: inputs.p1
                nesting_order: 0
                values: [101, 102]
    """
    )
    hf.WorkflowTemplate.from_YAML_string(wkt_yml)


def test_WorkflowTemplate_from_YAML_string_without_element_sets(null_config):
    wkt_yml = dedent(
        """
        name: simple_workflow

        tasks:
        - schema: dummy_task_1
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """
    )
    hf.WorkflowTemplate.from_YAML_string(wkt_yml)


def test_WorkflowTemplate_from_YAML_string_with_and_without_element_sets_equivalence(
    null_config,
):
    wkt_yml_1 = dedent(
        """
        name: simple_workflow

        tasks:
        - schema: dummy_task_1
          element_sets:
            - inputs:
                p2: 201
                p5: 501
              sequences:
                - path: inputs.p1
                  nesting_order: 0
                  values: [101, 102]
    """
    )
    wkt_yml_2 = dedent(
        """
        name: simple_workflow

        tasks:
        - schema: dummy_task_1
          inputs:
            p2: 201
            p5: 501
          sequences:
            - path: inputs.p1
              nesting_order: 0
              values: [101, 102]
    """
    )
    wkt_1 = hf.WorkflowTemplate.from_YAML_string(wkt_yml_1)
    wkt_2 = hf.WorkflowTemplate.from_YAML_string(wkt_yml_2)
    assert wkt_1 == wkt_2


def test_store_has_pending_during_add_task(
    workflow_w1: Workflow, schema_s2: TaskSchema, param_p3: Parameter
):
    t2 = hf.Task(schema=schema_s2, inputs=[hf.InputValue(param_p3, 301)])
    with workflow_w1.batch_update():
        workflow_w1.add_task(t2)
        assert workflow_w1._store.has_pending


def test_empty_batch_update_does_nothing(workflow_w1: Workflow):
    with workflow_w1.batch_update():
        assert not workflow_w1._store.has_pending


@pytest.mark.skip("need to re-implement `is_modified_on_disk`")
def test_is_modified_on_disk_when_metadata_changed(workflow_w1: Workflow):
    # this is ZarrPersistentStore-specific; might want to consider a refactor later
    with workflow_w1._store.cached_load():
        modify_workflow_metadata_on_disk(workflow_w1)
        assert workflow_w1._store.is_modified_on_disk()  # type: ignore


@pytest.mark.skip("need to re-implement `is_modified_on_disk`")
def test_batch_update_abort_if_modified_on_disk(
    workflow_w1: Workflow, schema_s2: TaskSchema, param_p3: Parameter
):
    t2 = hf.Task(schema=schema_s2, inputs=[hf.InputValue(param_p3, 301)])
    with pytest.raises(WorkflowBatchUpdateFailedError):
        with workflow_w1._store.cached_load(), workflow_w1.batch_update():
            workflow_w1.add_task(t2)
            modify_workflow_metadata_on_disk(workflow_w1)


def test_closest_task_input_source_chosen(null_config, tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p1",), "t1"),
            ({"p1": None}, ("p1",), "t2"),
            ({"p1": None}, ("p1",), "t3"),
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    assert wk.tasks.t3.get_task_dependencies(as_objects=True) == [wk.tasks.t2]


def test_WorkflowTemplate_from_JSON_string_without_element_sets(null_config):
    wkt_json = dedent(
        """
        {
            "name": "test_wk",
            "tasks": [
                {
                    "schema": "test_t1_bash",
                    "inputs": {
                        "p1": 101
                    }
                }
            ]
        }
    """
    )
    hf.WorkflowTemplate.from_JSON_string(wkt_json)


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_parameter_value_class_and_kwargs(
    null_config,
    tmp_path: Path,
    store: str,
    schema_s4c: TaskSchema,
    param_p1c: Parameter,
):
    a_value = 101
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=P1(a=a_value))],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value={"a": a_value})],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    assert (
        wk.tasks.t1_1.elements[0].inputs.p1c.value
        == wk.tasks.t1_2.elements[0].inputs.p1c.value
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_parameter_value_class_method_and_kwargs(
    null_config,
    tmp_path: Path,
    store: str,
    schema_s4c: TaskSchema,
    param_p1c: Parameter,
):
    b_val = 50
    c_val = 51
    expected_a_val = b_val + c_val
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=P1.from_data(b=b_val, c=c_val))],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[
            hf.InputValue(
                parameter=param_p1c,
                value={"b": b_val, "c": c_val},
                value_class_method="from_data",
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    assert wk.tasks.t1_1.elements[0].inputs.p1c.value.a == expected_a_val
    assert (
        wk.tasks.t1_1.elements[0].inputs.p1c.value
        == wk.tasks.t1_2.elements[0].inputs.p1c.value
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_value_class_expected_value(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    a_value = 101
    t1_value_exp = P1(a=a_value)
    t2_value_exp = {"a": a_value}
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=t1_value_exp)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=t2_value_exp)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].inputs[0].value
    value_2 = wk.tasks.t1_2.template.element_sets[0].inputs[0].value
    assert value_1 == t1_value_exp
    assert value_2 == t2_value_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_value_class_method_expected_value(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    b_val = 50
    c_val = 51
    t1_value_exp = P1.from_data(b=b_val, c=c_val)
    t2_value_exp = {"b": b_val, "c": c_val}
    t1_1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value=t1_value_exp)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        inputs=[
            hf.InputValue(
                parameter=param_p1c,
                value=t2_value_exp,
                value_class_method="from_data",
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].inputs[0].value
    value_2 = wk.tasks.t1_2.template.element_sets[0].inputs[0].value
    assert value_1 == t1_value_exp
    assert value_2 == t2_value_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_sequence_parameter_value_class_and_kwargs(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"a": 101}
    obj = P1(**data)  # type: ignore[arg-type]  # python/mypy#15317
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[data], nesting_order=0)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    val_1 = wk.tasks.t1_1.elements[0].inputs.p1c.value
    val_2 = wk.tasks.t1_2.elements[0].inputs.p1c.value
    assert val_1 == val_2 == obj


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_equivalent_element_input_sequence_parameter_value_class_method_and_kwargs(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"b": 50, "c": 51}
    obj = P1.from_data(**data)
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[
            hf.ValueSequence(
                path="inputs.p1c",
                values=[data],
                value_class_method="from_data",
                nesting_order=0,
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    val_1 = wk.tasks.t1_1.elements[0].inputs.p1c.value
    val_2 = wk.tasks.t1_2.elements[0].inputs.p1c.value
    assert val_1 == val_2 == obj


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_sequence_value_class_expected_value(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"a": 101}
    obj = P1(**data)  # type: ignore[arg-type]  # python/mypy#15317
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[data], nesting_order=0)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].sequences[0].values[0]
    value_2 = wk.tasks.t1_2.template.element_sets[0].sequences[0].values[0]
    assert value_1 == obj
    assert value_2 == data


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_sequence_value_class_method_expected_value(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema
):
    data = {"b": 50, "c": 51}
    obj = P1.from_data(**data)
    t1_1 = hf.Task(
        schema=[schema_s4c],
        sequences=[hf.ValueSequence(path="inputs.p1c", values=[obj], nesting_order=0)],
    )
    t1_2 = hf.Task(
        schema=[schema_s4c],
        sequences=[
            hf.ValueSequence(
                path="inputs.p1c",
                values=[data],
                nesting_order=0,
                value_class_method="from_data",
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1_1, t1_2],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    value_1 = wk.tasks.t1_1.template.element_sets[0].sequences[0].values[0]
    value_2 = wk.tasks.t1_2.template.element_sets[0].sequences[0].values[0]
    assert value_1 == obj
    assert value_2 == data


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_expected_element_input_parameter_value_class_merge_sequence(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    a_val = 101
    d_val = 201
    obj_exp = P1(a=a_val, d=d_val)

    t1 = hf.Task(
        schema=[schema_s4c],
        inputs=[hf.InputValue(parameter=param_p1c, value={"a": a_val})],
        sequences=[
            hf.ValueSequence(path="inputs.p1c.d", values=[d_val], nesting_order=0)
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    wk.tasks.t1.template.element_sets[0].sequences[0].values[0]
    assert wk.tasks.t1.elements[0].inputs.p1c.value == obj_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_expected_element_input_parameter_value_class_method_merge_sequence(
    null_config, tmp_path: Path, store: str, schema_s4c: TaskSchema, param_p1c: Parameter
):
    b_val = 50
    c_val = 51
    obj_exp = P1.from_data(b=b_val, c=c_val)

    t1 = hf.Task(
        schema=[schema_s4c],
        inputs=[
            hf.InputValue(
                parameter=param_p1c, value={"b": b_val}, value_class_method="from_data"
            )
        ],
        sequences=[
            hf.ValueSequence(path="inputs.p1c.c", values=[c_val], nesting_order=0)
        ],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    wk.tasks.t1.template.element_sets[0].sequences[0].values[0]
    assert wk.tasks.t1.elements[0].inputs.p1c.value == obj_exp


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_upstream_input_source_merge_with_current_input_modification(
    null_config, tmp_path: Path, store: str, param_p2: Parameter
):
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=param_p2)])
    s2 = hf.TaskSchema(objective="t2", inputs=[hf.SchemaInput(parameter=param_p2)])
    tasks = [
        hf.Task(schema=s1, inputs=[hf.InputValue("p2", {"a": 101})]),
        hf.Task(schema=s2, inputs=[hf.InputValue("p2", value=102, path="b")]),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    p2 = wk.tasks[1].elements[0].inputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == {"a": 101, "b": 102}


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_upstream_input_source_with_sub_parameter(
    null_config, tmp_path: Path, store: str, param_p2: Parameter
):
    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput(parameter=param_p2)])
    s2 = hf.TaskSchema(objective="t2", inputs=[hf.SchemaInput(parameter=param_p2)])
    tasks = [
        hf.Task(
            schema=s1,
            inputs=[
                hf.InputValue("p2", {"a": 101}),
                hf.InputValue("p2", value=102, path="b"),
            ],
        ),
        hf.Task(schema=s2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    p2 = wk.tasks[1].elements[0].inputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == {"a": 101, "b": 102}


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_template_data_workflow_reload(null_config, tmp_path: Path, store: str):
    wk_name = "temp"
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs=[hf.InputValue("p1", 101)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
        store=store,
    )
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_template_workflow_reload(null_config, tmp_path: Path, store: str):
    wk_name = "temp"
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs=[hf.InputValue("p1", 101)])
    wkt = hf.WorkflowTemplate(name=wk_name, tasks=[t1])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
        store=store,
    )
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_YAML_str_template_workflow_reload(null_config, tmp_path: Path, store: str):
    yaml_str = dedent(
        """
    name: temp
    tasks: 
      - schema: test_t1_ps
        inputs:
          p1: 101
    """
    )
    wk = hf.Workflow.from_YAML_string(
        YAML_str=yaml_str,
        path=tmp_path,
        store=store,
    )
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_from_template_workflow_add_task_reload(null_config, tmp_path: Path, store: str):
    wk_name = "temp"
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs=[hf.InputValue("p1", 101)])
    wkt = hf.WorkflowTemplate(name=wk_name)
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
        store=store,
    )
    wk.add_task(t1)
    wk_ld = hf.Workflow(wk.path)
    assert (
        wk.tasks[0].elements[0].get_data_idx()
        == wk_ld.tasks[0].elements[0].get_data_idx()
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_batch_update_mode_false_after_empty_workflow_init(
    null_config, tmp_path: Path, store: str
):
    wk_name = "temp"
    wk = hf.Workflow.from_template_data(
        tasks=[],
        path=tmp_path,
        template_name=wk_name,
        store=store,
    )
    assert wk._in_batch_mode == False
