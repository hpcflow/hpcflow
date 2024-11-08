from __future__ import annotations
from pathlib import Path
from textwrap import dedent
from typing import Any, TYPE_CHECKING
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import UnsetParameterDataError
from hpcflow.sdk.core.test_utils import (
    make_schemas,
    P1_parameter_cls as P1,
    P1_sub_parameter_cls as P1_sub,
)

if TYPE_CHECKING:
    from hpcflow.sdk.core.workflow import Workflow


@pytest.fixture
def workflow_w1(null_config, tmp_path: Path) -> Workflow:
    s1, s2 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, (), "t2"),
    )

    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = hf.Task(schema=s2, nesting_order={"inputs.p2": 1})

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


def test_element_task_dependencies(workflow_w1: Workflow):
    assert workflow_w1.tasks.t2.elements[0].get_task_dependencies(as_objects=True) == [
        workflow_w1.tasks.t1
    ]


def test_element_dependent_tasks(workflow_w1: Workflow):
    assert workflow_w1.tasks.t1.elements[0].get_dependent_tasks(as_objects=True) == [
        workflow_w1.tasks.t2
    ]


def test_element_element_dependencies(workflow_w1: Workflow):
    assert workflow_w1.tasks.t2.elements[0].get_element_dependencies() == {0}
    assert workflow_w1.tasks.t2.elements[1].get_element_dependencies() == {1}


def test_element_dependent_elements(workflow_w1: Workflow):
    assert workflow_w1.tasks.t1.elements[0].get_dependent_elements() == {2}
    assert workflow_w1.tasks.t1.elements[1].get_dependent_elements() == {3}


def test_equivalence_single_labelled_schema_input_element_get_label_and_non_label(
    new_null_config, tmp_path: Path
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"), labels={"one": {}})],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command="Write-Output (<<parameter:p1[one]>> + 100)")
                ]
            )
        ],
    )
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", label="one", value=101)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    assert wk.tasks.t1.elements[0].get("inputs.p1") == wk.tasks.t1.elements[0].get(
        "inputs.p1[one]"
    )


def test_element_dependencies_inputs_only_schema(new_null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p2>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
    )
    tasks = [
        hf.Task(
            schema=s1,
            inputs=[hf.InputValue("p1", value=101)],
        ),
        hf.Task(schema=s2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    assert wk.tasks.t1.elements[0].get_dependent_elements() == {1}
    assert wk.tasks.t2.elements[0].get_element_dependencies() == {0}
    assert wk.tasks.t2.elements[0].get_EAR_dependencies() == {0}


def test_element_get_empty_path_single_labelled_input(null_config, tmp_path: Path):
    p1_val = 101
    label = "my_label"
    s1 = hf.TaskSchema(
        objective="t1", inputs=[hf.SchemaInput(parameter="p1", labels={label: {}})]
    )
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue("p1", p1_val, label=label)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
    )
    assert wk.tasks[0].elements[0].get() == {
        "resources": {"any": {}},
        "inputs": {"p1": p1_val},
    }


def test_element_get_labelled_non_labelled_equivalence(null_config, tmp_path: Path):
    p1_val = 101
    label = "my_label"
    s1 = hf.TaskSchema(
        objective="t1", inputs=[hf.SchemaInput(parameter="p1", labels={label: {}})]
    )
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue("p1", p1_val, label=label)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="temp",
    )
    assert wk.tasks[0].elements[0].get("inputs.p1") == wk.tasks[0].elements[0].get(
        f"inputs.p1[{label}]"
    )


@pytest.fixture
def element_get_wk(null_config, tmp_path: Path) -> Workflow:
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1"), hf.SchemaInput(parameter="p1c")],
        outputs=[hf.SchemaOutput(parameter="p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + <<parameter:p1c>>)",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    p1_value = 100
    p1c_value = P1(a=10, sub_param=P1_sub(e=5))
    t1 = hf.Task(
        schema=s1,
        inputs=[
            hf.InputValue("p1", value=p1_value),
            hf.InputValue("p1c", value=p1c_value),
        ],
        sequences=[hf.ValueSequence("inputs.p1c.a", values=[20, 30], nesting_order=0)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        path=tmp_path,
    )
    return wk


def test_element_get_simple(element_get_wk: Workflow):
    assert element_get_wk.tasks.t1.elements[0].get("inputs.p1") == 100
    assert element_get_wk.tasks.t1.elements[1].get("inputs.p1") == 100


def test_element_get_obj(element_get_wk: Workflow):
    obj_0 = P1(a=20, sub_param=P1_sub(e=5))
    obj_1 = P1(a=30, sub_param=P1_sub(e=5))
    assert element_get_wk.tasks.t1.elements[0].get("inputs.p1c") == obj_0
    assert element_get_wk.tasks.t1.elements[1].get("inputs.p1c") == obj_1


def test_element_get_sub_obj(element_get_wk: Workflow):
    sub_obj = P1_sub(e=5)
    assert element_get_wk.tasks.t1.elements[0].get("inputs.p1c.sub_param") == sub_obj
    assert element_get_wk.tasks.t1.elements[1].get("inputs.p1c.sub_param") == sub_obj


def test_element_get_sub_obj_attr(element_get_wk: Workflow):
    assert element_get_wk.tasks.t1.elements[0].get("inputs.p1c.sub_param.e") == 5
    assert element_get_wk.tasks.t1.elements[1].get("inputs.p1c.sub_param.e") == 5


def test_element_get_sub_obj_property(element_get_wk: Workflow):
    assert element_get_wk.tasks.t1.elements[0].get("inputs.p1c.sub_param.twice_e") == 10
    assert element_get_wk.tasks.t1.elements[1].get("inputs.p1c.sub_param.twice_e") == 10


def test_element_get_obj_no_raise_missing_attr(element_get_wk: Workflow):
    assert element_get_wk.tasks.t1.elements[0].get("inputs.p1c.b") is None


def test_element_get_obj_raise_missing_attr(element_get_wk: Workflow):
    with pytest.raises(ValueError):
        element_get_wk.tasks.t1.elements[0].get("inputs.p1c.b", raise_on_missing=True)


def test_element_get_obj_raise_missing_nested_attr(element_get_wk: Workflow):
    with pytest.raises(ValueError):
        element_get_wk.tasks.t1.elements[0].get("inputs.p1c.a.b", raise_on_missing=True)


def test_element_get_raise_missing_root(element_get_wk: Workflow):
    with pytest.raises(ValueError):
        element_get_wk.tasks.t1.elements[0].get("blah", raise_on_missing=True)


def test_element_get_no_raise_missing_root(element_get_wk: Workflow):
    assert element_get_wk.tasks.t1.elements[0].get("blah") is None


def test_element_get_expected_default(element_get_wk: Workflow):
    assert element_get_wk.tasks.t1.elements[0].get("blah", default={}) == {}


def test_element_get_part_unset(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1")],
        outputs=[hf.SchemaOutput(parameter="p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p2>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    s2 = hf.TaskSchema(objective="t2", inputs=[hf.SchemaInput(parameter="p2")])

    t1 = hf.Task(
        schema=s1,
        inputs=[hf.InputValue("p1", value=1)],
    )
    t2 = hf.Task(schema=s2, inputs=[hf.InputValue("p2", path="a", value=2)])

    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        path=tmp_path,
    )

    # "inputs.p2.a" is set (local) but "inputs.p2" is unset (from task 1), so value of
    # "p2" should be `None`:
    assert wk.tasks.t2.elements[0].get() == {
        "resources": {"any": {}},
        "inputs": {"p2": None},
    }
    assert wk.tasks.t2.elements[0].get("inputs") == {"p2": None}
    assert wk.tasks.t2.elements[0].get("inputs.p2") == None

    # but value of "p2.a" should be accessible:
    assert wk.tasks.t2.elements[0].get("inputs.p2.a") == 2

    with pytest.raises(UnsetParameterDataError):
        wk.tasks.t2.elements[0].get(raise_on_unset=True)

    with pytest.raises(UnsetParameterDataError):
        wk.tasks.t2.elements[0].get("inputs", raise_on_unset=True)

    with pytest.raises(UnsetParameterDataError):
        wk.tasks.t2.elements[0].get("inputs.p2", raise_on_unset=True)


def test_element_get_unset_object(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1")],
        outputs=[hf.SchemaOutput(parameter="p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p1c>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    t1 = hf.Task(
        schema=s1,
        inputs=[hf.InputValue("p1", value=1)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        path=tmp_path,
    )
    assert wk.tasks.t1.elements[0].get("outputs.p1c") == None


def test_element_get_unset_sub_object(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1")],
        outputs=[hf.SchemaOutput(parameter="p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p1c.CLI_parse(e=10)>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    t1 = hf.Task(
        schema=s1,
        inputs=[hf.InputValue("p1", value=1)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        path=tmp_path,
    )
    assert wk.tasks.t1.elements[0].get("outputs.p1c.sub_param") == None


def test_element_get_unset_object_group(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1c")],
        outputs=[hf.SchemaOutput(parameter="p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c>> + 100)",
                        stdout="<<parameter:p1c.CLI_parse()>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"), group="my_group")],
    )

    t1 = hf.Task(
        schema=s1,
        inputs=[hf.InputValue("p1c", value=P1(a=10, sub_param=P1_sub(e=5)))],
        sequences=[hf.ValueSequence("inputs.p1c.a", values=[20, 30], nesting_order=0)],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(
        schema=s2,
        nesting_order={"inputs.p1c": 0},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        path=tmp_path,
    )
    assert wk.tasks.t2.elements[0].get("inputs.p1c") == [None, None]


def test_element_get_unset_sub_object_group(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1c")],
        outputs=[hf.SchemaOutput(parameter="p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c>> + 100)",
                        stdout="<<parameter:p1c.CLI_parse(e=10)>>",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"), group="my_group")],
    )

    t1 = hf.Task(
        schema=s1,
        inputs=[hf.InputValue("p1c", value=P1(a=10, sub_param=P1_sub(e=5)))],
        sequences=[hf.ValueSequence("inputs.p1c.a", values=[20, 30], nesting_order=0)],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(
        schema=s2,
        nesting_order={"inputs.p1c": 0},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        path=tmp_path,
    )
    assert wk.tasks.t2.elements[0].get("inputs.p1c.sub_param") == [None, None]


def test_iter(new_null_config, tmp_path: Path):
    wkt = hf.WorkflowTemplate(
        name="test",
        tasks=[
            hf.Task(
                schema=hf.task_schemas.test_t1_ps,
                sequences=[hf.ValueSequence(path="inputs.p1", values=[1, 2, 3])],
            ),
        ],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    for idx, elem_i in enumerate(wk.tasks[0].elements):
        assert elem_i.index == idx


def test_slice(new_null_config, tmp_path: Path):
    wkt = hf.WorkflowTemplate(
        name="test",
        tasks=[
            hf.Task(
                schema=hf.task_schemas.test_t1_ps,
                sequences=[hf.ValueSequence(path="inputs.p1", values=[1, 2, 3])],
            ),
        ],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    elems = wk.tasks[0].elements[0::2]
    assert len(elems) == 2
    assert elems[0].index == 0
    assert elems[1].index == 2


def test_element_get_with_list_index_sequence(null_config, tmp_path: Path):
    wkt_yaml = dedent(
        """\
        name: test_list_idx_sequence
        tasks:
          - schema: test_t1_ps
            inputs:
              p1: [0, 1]
            sequences:
              - path: inputs.p1.0
                values: [9]
    """
    )
    wkt = hf.WorkflowTemplate.from_YAML_string(wkt_yaml)
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert wk.tasks[0].elements[0].get("inputs.p1") == [9, 1]


def test_element_get_with_list_index_sequence_two_parts(null_config, tmp_path: Path):
    wkt_yaml = dedent(
        """\
        name: test_list_idx_sequence
        tasks:
          - schema: test_t1_ps
            inputs:
              p1: [
                [0, 1],
                [2, 3],
              ]
            sequences:
              - path: inputs.p1.0.1
                values: [9, 99]
    """
    )
    wkt = hf.WorkflowTemplate.from_YAML_string(wkt_yaml)
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert wk.tasks[0].elements[0].get("inputs.p1") == [
        [0, 9],
        [2, 3],
    ]
    assert wk.tasks[0].elements[1].get("inputs.p1") == [
        [0, 99],
        [2, 3],
    ]


def test_element_get_group_sequence(null_config, tmp_path: Path):
    wkt_yaml = dedent(
        """\
        name: test_list_idx_sequence
        template_components:
          task_schemas:
            - objective: test_group_schema
              inputs:
                - parameter: p1
                  group: my_group
        tasks:
          - schema: test_t1_ps
            inputs:
              p1:
                a: 1
            sequences:
              - path: inputs.p1.b
                values: [8, 9]
            groups:
              - name: my_group
          - schema: test_group_schema
    """
    )
    wkt = hf.WorkflowTemplate.from_YAML_string(wkt_yaml)
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert wk.tasks[1].elements[0].get("inputs") == {
        "p1": [{"a": 1, "b": 8}, {"a": 1, "b": 9}]
    }
    assert wk.tasks[1].elements[0].get("inputs.p1") == [
        {"a": 1, "b": 8},
        {"a": 1, "b": 9},
    ]
    assert wk.tasks[1].elements[0].get("inputs.p1.b") == [
        8,
        9,
    ]


def test_element_get_group_sequence_obj(new_null_config, tmp_path: Path):
    wkt_yaml = dedent(
        """\
        name: test_list_idx_sequence
        template_components:
          task_schemas:
            - objective: test_t1_ps_obj
              parameter_class_modules: ["hpcflow.sdk.core.test_utils"]
              inputs:
                - parameter: p1c
              outputs:
                - parameter: p2
              actions:
                - environments:
                    - scope:
                        type: any
                      environment: null_env
                  commands:
                    - command: Write-Output ((<<parameter:p1c>> + 100))
                      stdout: <<parameter:p2>>
            - objective: test_group_schema
              parameter_class_modules: ["hpcflow.sdk.core.test_utils"]
              inputs:
                - parameter: p1c
                  group: my_group

        tasks:
          - schema: test_t1_ps_obj
            inputs:
              p1c:
                a: 1
            sequences:
              - path: inputs.p1c.d
                values: [8, 9]
            groups:
              - name: my_group
          - schema: test_group_schema
    """
    )
    wkt = hf.WorkflowTemplate.from_YAML_string(wkt_yaml)
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert wk.tasks[1].elements[0].get("inputs") == {
        "p1c": [{"a": 1, "d": 8}, {"a": 1, "d": 9}]
    }
    assert wk.tasks[1].elements[0].get("inputs.p1c") == [
        P1(a=1, d=8),
        P1(a=1, d=9),
    ]
    assert wk.tasks[1].elements[0].get("inputs.p1c.d") == [
        8,
        9,
    ]


def test_element_resources_get_jobscript_hash_equal_empty() -> None:
    assert (
        hf.ElementResources().get_jobscript_hash()
        == hf.ElementResources().get_jobscript_hash()
    )


def test_element_resources_get_jobscript_hash_unequal_num_cores() -> None:
    assert (
        hf.ElementResources(num_cores=1).get_jobscript_hash()
        != hf.ElementResources(num_cores=2).get_jobscript_hash()
    )


def test_element_resources_get_jobscript_hash_equal_num_cores() -> None:
    assert (
        hf.ElementResources(num_cores=1).get_jobscript_hash()
        == hf.ElementResources(num_cores=1).get_jobscript_hash()
    )


def test_element_resources_get_jobscript_hash_unequal_scheduler_args_empty() -> None:
    assert (
        hf.ElementResources().get_jobscript_hash()
        != hf.ElementResources(
            scheduler_args={"options": {"--time": "01:00:00"}}
        ).get_jobscript_hash()
    )


def test_element_resources_get_jobscript_hash_equal_non_truthy_scheduler_args() -> None:
    assert (
        hf.ElementResources().get_jobscript_hash()
        == hf.ElementResources(scheduler_args={}).get_jobscript_hash()
    )


def test_element_resources_get_jobscript_hash_unequal_scheduler_args_diff_options() -> (
    None
):
    assert (
        hf.ElementResources(
            scheduler_args={"options": {"--time": "02:00:00"}}
        ).get_jobscript_hash()
        != hf.ElementResources(
            scheduler_args={"options": {"--partition": "MULTICORE"}}
        ).get_jobscript_hash()
    )


def test_element_resources_get_jobscript_hash_unequal_scheduler_args_same_options() -> (
    None
):
    assert (
        hf.ElementResources(
            scheduler_args={"options": {"--time": "02:00:00"}}
        ).get_jobscript_hash()
        != hf.ElementResources(
            scheduler_args={"options": {"--time": "01:00:00"}}
        ).get_jobscript_hash()
    )
