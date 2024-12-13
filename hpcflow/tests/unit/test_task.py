from __future__ import annotations
import copy
import os
import pytest
from typing import TYPE_CHECKING

from valida.conditions import Value  # type: ignore

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    MissingInputs,
    TaskTemplateInvalidNesting,
    TaskTemplateMultipleInputValues,
    TaskTemplateMultipleSchemaObjectives,
    TaskTemplateUnexpectedInput,
    UnknownEnvironmentPresetError,
    UnsetParameterDataError,
)
from hpcflow.sdk.core.parameters import NullDefault
from hpcflow.sdk.core.test_utils import (
    make_schemas,
    make_tasks,
    make_workflow,
    P1_parameter_cls as P1,
    P1_sub_parameter_cls as P1_sub_param,
    P1_sub_parameter_cls_2 as P1_sub_param_2,
)

if TYPE_CHECKING:
    from pathlib import Path
    from hpcflow.sdk.core.actions import Action, ActionEnvironment
    from hpcflow.sdk.core.command_files import FileSpec
    from hpcflow.sdk.core.parameters import Parameter
    from hpcflow.sdk.core.task_schema import TaskSchema
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
def workflow_w0(null_config, tmp_path: Path) -> Workflow:
    t1 = hf.Task(schema=[hf.TaskSchema(objective="t1", actions=[])])
    t2 = hf.Task(schema=[hf.TaskSchema(objective="t2", actions=[])])

    wkt = hf.WorkflowTemplate(name="workflow_w0", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


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
def workflow_w2(
    null_config,
    tmp_path: Path,
    param_p1: Parameter,
    param_p2: Parameter,
    param_p3: Parameter,
) -> Workflow:
    s1 = hf.TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p2])
    s2 = hf.TaskSchema("t2", actions=[], inputs=[param_p2, param_p3])

    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=1)],
    )
    t2 = hf.Task(
        schema=s2,
        sequences=[
            hf.ValueSequence("inputs.p3", values=[301, 302, 303], nesting_order=1)
        ],
        nesting_order={"inputs.p2": 0},
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    return hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def workflow_w3(
    null_config,
    tmp_path: Path,
    param_p1: Parameter,
    param_p2: Parameter,
    param_p3: Parameter,
    param_p4: Parameter,
) -> Workflow:
    s1 = hf.TaskSchema("t1", actions=[], inputs=[param_p1], outputs=[param_p3])
    s2 = hf.TaskSchema("t2", actions=[], inputs=[param_p2, param_p3], outputs=[param_p4])
    s3 = hf.TaskSchema("t3", actions=[], inputs=[param_p3, param_p4])

    t1 = hf.Task(schema=s1, inputs=[hf.InputValue(param_p1, 101)])
    t2 = hf.Task(
        schema=s2,
        sequences=[hf.ValueSequence("inputs.p2", values=[201, 202], nesting_order=1)],
    )
    t3 = hf.Task(schema=s3, nesting_order={"inputs.p3": 0, "inputs.p4": 1})

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2, t3])
    return hf.Workflow.from_template(wkt, name=wkt.name, overwrite=True)


@pytest.fixture
def file_spec_fs1() -> FileSpec:
    return hf.FileSpec(label="file1", name="file1.txt")


@pytest.fixture
def act_env_1() -> ActionEnvironment:
    return hf.ActionEnvironment("env_1")


@pytest.fixture
def act_3(
    act_env_1: ActionEnvironment, param_p2: Parameter, file_spec_fs1: FileSpec
) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        output_file_parsers=[
            hf.OutputFileParser(output=param_p2, output_files=[file_spec_fs1]),
        ],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s3(param_p1: Parameter, param_p2: Parameter, act_3) -> TaskSchema:
    return hf.TaskSchema("ts1", actions=[act_3], inputs=[param_p1], outputs=[param_p2])


@pytest.fixture
def workflow_w4(
    null_config, tmp_path: Path, schema_s3: TaskSchema, param_p1: Parameter
) -> Workflow:
    t1 = hf.Task(schema=schema_s3, inputs=[hf.InputValue(param_p1, 101)])
    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1])
    return hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.fixture
def act_1(act_env_1: ActionEnvironment) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def act_2(act_env_1: ActionEnvironment) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p2>>")],
        environments=[act_env_1],
    )


@pytest.fixture
def schema_s1(param_p1: Parameter, act_1) -> TaskSchema:
    return hf.TaskSchema("ts1", actions=[act_1], inputs=[param_p1])


@pytest.fixture
def schema_s2(param_p1: Parameter, act_1) -> TaskSchema:
    return hf.TaskSchema(
        "ts1", actions=[act_1], inputs=[hf.SchemaInput(param_p1, default_value=101)]
    )


@pytest.fixture
def schema_s4(param_p2: Parameter, act_2) -> TaskSchema:
    return hf.TaskSchema("ts2", actions=[act_2], inputs=[param_p2])


@pytest.fixture
def schema_s5(param_p2: Parameter, act_2) -> TaskSchema:
    return hf.TaskSchema(
        "ts2", actions=[act_2], inputs=[hf.SchemaInput(param_p2, default_value=2002)]
    )


def test_task_get_available_task_input_sources_expected_return_first_task_local_value(
    schema_s1: TaskSchema,
    param_p1: Parameter,
):
    t1 = hf.Task(schema=schema_s1, inputs=[hf.InputValue(param_p1, value=101)])

    available = t1.get_available_task_input_sources(
        element_set=t1.element_sets[0],
        source_tasks=[],
    )
    available_exp = {"p1": [hf.InputSource(source_type=hf.InputSourceType.LOCAL)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_first_task_default_value(
    schema_s2: TaskSchema,
):
    t1 = hf.Task(schema=schema_s2)
    available = t1.get_available_task_input_sources(element_set=t1.element_sets[0])
    available_exp = {"p1": [hf.InputSource(source_type=hf.InputSourceType.DEFAULT)]}

    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output(
    tmp_path: Path,
):
    t1, t2 = make_tasks(
        schemas_spec=[
            ({"p1": NullDefault.NULL}, ("p2",), "t1"),
            ({"p2": NullDefault.NULL}, (), "t2"),
        ],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1],
    )
    available_exp = {
        "p2": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            )
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default(
    tmp_path: Path,
):
    t1, t2 = make_tasks(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": 2001}, (), "t2"),
        ],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1],
    )
    available_exp = {
        "p2": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
            hf.InputSource(source_type=hf.InputSourceType.DEFAULT),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_local(
    tmp_path: Path,
):
    t1, t2 = make_tasks(
        schemas_spec=[
            ({"p1": NullDefault.NULL}, ("p2",), "t1"),
            ({"p2": NullDefault.NULL}, (), "t2"),
        ],
        local_inputs={0: ("p1",), 1: ("p2",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1],
    )
    available_exp = {
        "p2": [
            hf.InputSource(source_type=hf.InputSourceType.LOCAL),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_one_output_with_default_and_local(
    tmp_path: Path,
):
    t1, t2 = make_tasks(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": 2001}, (), "t2"),
        ],
        local_inputs={0: ("p1",), 1: ("p2",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1],
    )
    available_exp = {
        "p2": [
            hf.InputSource(source_type=hf.InputSourceType.LOCAL),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
            hf.InputSource(source_type=hf.InputSourceType.DEFAULT),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_one_param_two_outputs(
    tmp_path: Path,
):
    t1, t2, t3 = make_tasks(
        schemas_spec=[
            ({"p1": NullDefault.NULL}, ("p2", "p3"), "t1"),
            ({"p2": NullDefault.NULL}, ("p3", "p4"), "t2"),
            ({"p3": NullDefault.NULL}, (), "t3"),
        ],
        local_inputs={0: ("p1",), 1: ("p2",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1, t2]), path=tmp_path
    )
    available = t3.get_available_task_input_sources(
        element_set=t3.element_sets[0],
        source_tasks=[wk.tasks.t1, wk.tasks.t2],
    )
    available_exp = {
        "p3": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=1,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[1],
            ),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
        ]
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_expected_return_two_params_one_output(
    tmp_path: Path,
):
    t1, t2 = make_tasks(
        schemas_spec=[
            ({"p1": NullDefault.NULL}, ("p2", "p3"), "t1"),
            ({"p2": NullDefault.NULL, "p3": NullDefault.NULL}, (), "t2"),
        ],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1]), path=tmp_path
    )
    available = t2.get_available_task_input_sources(
        element_set=t2.element_sets[0],
        source_tasks=[wk.tasks.t1],
    )
    available_exp = {
        "p2": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            )
        ],
        "p3": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            )
        ],
    }
    assert available == available_exp


def test_task_get_available_task_input_sources_one_parameter_extravaganza(
    tmp_path: Path,
):
    """Test an input source is excluded if it is not locally defined (meaning it comes
    from another task)."""

    t1, t2, t3 = make_tasks(
        schemas_spec=[
            ({"p1": NullDefault.NULL}, ("p1",), "t1"),  # sources for t3: input + output
            ({"p1": NullDefault.NULL}, ("p1",), "t2"),  # sources fot t3: output only
            ({"p1": NullDefault.NULL}, ("p1",), "t3"),
        ],
        local_inputs={0: ("p1",)},
    )
    wk = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="w1", tasks=[t1, t2]), path=tmp_path
    )
    available = t3.get_available_task_input_sources(
        element_set=t3.element_sets[0],
        source_tasks=[wk.tasks.t1, wk.tasks.t2],
    )
    available_exp = {
        "p1": [
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=1,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[1],
            ),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=1,
                task_source_type=hf.TaskSourceType.INPUT,
                element_iters=[1],
            ),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.OUTPUT,
                element_iters=[0],
            ),
            hf.InputSource(
                source_type=hf.InputSourceType.TASK,
                task_ref=0,
                task_source_type=hf.TaskSourceType.INPUT,
                element_iters=[0],
            ),
        ],
    }
    assert available == available_exp


def test_task_input_sources_output_label(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command="Write-Host 101", stdout="<<int(parameter:p1)>>")
                ]
            )
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2", inputs=[hf.SchemaInput("p1", labels={"one": {}}, multiple=True)]
    )

    tasks = [
        hf.Task(schema=ts1, output_labels=[hf.OutputLabel(parameter="p1", label="one")]),
        hf.Task(schema=ts2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks, template_name="test_sources", path=tmp_path
    )

    assert wk.tasks.t2.template.element_sets[0].input_sources == {
        "p1[one]": [
            hf.InputSource.task(task_ref=0, task_source_type="output", element_iters=[0])
        ]
    }


def test_task_input_sources_output_label_filtered(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Host (<<parameter:p1>> + 101)",
                        stdout="<<int(parameter:p1)>>",
                    ),
                ],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2", inputs=[hf.SchemaInput("p1", labels={"one": {}}, multiple=True)]
    )

    tasks = [
        hf.Task(
            schema=ts1,
            sequences=[hf.ValueSequence(path="inputs.p1", values=[1, 2])],
            output_labels=[
                hf.OutputLabel(
                    parameter="p1",
                    label="one",
                    where=hf.Rule(path="inputs.p1", condition={"value.equal_to": 2}),
                ),
            ],
        ),
        hf.Task(schema=ts2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        template_name="test_sources",
        path=tmp_path,
    )

    assert wk.tasks.t2.template.element_sets[0].input_sources == {
        "p1[one]": [
            hf.InputSource.task(task_ref=0, task_source_type="output", element_iters=[1])
        ]
    }


def test_get_task_unique_names_two_tasks_no_repeats():
    s1 = hf.TaskSchema("t1", actions=[])
    s2 = hf.TaskSchema("t2", actions=[])

    t1 = hf.Task(schema=s1)
    t2 = hf.Task(schema=s2)

    assert hf.Task.get_task_unique_names([t1, t2]) == ["t1", "t2"]


def test_get_task_unique_names_two_tasks_with_repeat():
    s1 = hf.TaskSchema("t1", actions=[])

    t1 = hf.Task(schema=s1)
    t2 = hf.Task(schema=s1)

    assert hf.Task.get_task_unique_names([t1, t2]) == ["t1_1", "t1_2"]


def test_raise_on_multiple_schema_objectives():
    s1 = hf.TaskSchema("t1", actions=[])
    s2 = hf.TaskSchema("t2", actions=[])
    with pytest.raises(TaskTemplateMultipleSchemaObjectives):
        hf.Task(schema=[s1, s2])


def test_raise_on_unexpected_inputs(param_p1: Parameter, param_p2: Parameter):
    (s1,) = make_schemas(({"p1": None}, ()))

    with pytest.raises(TaskTemplateUnexpectedInput):
        hf.Task(
            schema=s1,
            inputs=[
                hf.InputValue(param_p1, value=101),
                hf.InputValue(param_p2, value=4),
            ],
        )


def test_raise_on_multiple_input_values(param_p1: Parameter):
    (s1,) = make_schemas(({"p1": None}, ()))

    with pytest.raises(TaskTemplateMultipleInputValues):
        hf.Task(
            schema=s1,
            inputs=[
                hf.InputValue(param_p1, value=101),
                hf.InputValue(param_p1, value=7),
            ],
        )


def test_raise_on_multiple_input_values_same_label(param_p1: Parameter):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1", labels={"0": {}})],
    )

    with pytest.raises(TaskTemplateMultipleInputValues):
        hf.Task(
            schema=s1,
            inputs=[
                hf.InputValue(param_p1, value=101, label="0"),
                hf.InputValue(param_p1, value=101, label="0"),
            ],
        )


def test_multiple_input_values_different_labels(param_p1: Parameter):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter="p1",
                labels={"0": {}, "1": {}},
                multiple=True,
            )
        ],
    )
    hf.Task(
        schema=s1,
        inputs=[
            hf.InputValue(param_p1, value=101, label="0"),
            hf.InputValue(param_p1, value=101, label="1"),
        ],
    )


def test_expected_return_defined_and_undefined_input_types(
    param_p1: Parameter, param_p2: Parameter
):
    (s1,) = make_schemas(({"p1": None, "p2": None}, ()))

    t1 = hf.Task(schema=s1, inputs=[hf.InputValue(param_p1, value=101)])
    element_set = t1.element_sets[0]
    assert element_set.defined_input_types == {
        param_p1.typ
    } and element_set.undefined_input_types == {param_p2.typ}


def test_expected_return_all_schema_input_types_single_schema(
    param_p1: Parameter, param_p2: Parameter
):
    (s1,) = make_schemas(({"p1": None, "p2": None}, ()))
    t1 = hf.Task(schema=s1)

    assert t1.all_schema_input_types == {param_p1.typ, param_p2.typ}


def test_expected_return_all_schema_input_types_multiple_schemas(
    param_p1: Parameter, param_p2: Parameter, param_p3: Parameter
):
    s1, s2 = make_schemas(
        ({"p1": None, "p2": None}, (), "t1"), ({"p1": None, "p3": None}, (), "t1")
    )

    t1 = hf.Task(schema=[s1, s2])

    assert t1.all_schema_input_types == {param_p1.typ, param_p2.typ, param_p3.typ}


def test_expected_name_single_schema():
    s1 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schema=[s1])
    assert t1.name == "t1"


def test_expected_name_single_schema_with_method():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    t1 = hf.Task(schema=s1)
    assert t1.name == "t1_m1"


def test_expected_name_single_schema_with_implementation():
    s1 = hf.TaskSchema("t1", implementation="i1", actions=[])
    t1 = hf.Task(schema=s1)
    assert t1.name == "t1_i1"


def test_expected_name_single_schema_with_method_and_implementation():
    s1 = hf.TaskSchema("t1", method="m1", implementation="i1", actions=[])
    t1 = hf.Task(schema=s1)
    assert t1.name == "t1_m1_i1"


def test_expected_name_multiple_schemas():
    s1 = hf.TaskSchema("t1", actions=[])
    s2 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schema=[s1, s2])
    assert t1.name == "t1"


def test_expected_name_two_schemas_first_with_method():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    s2 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schema=[s1, s2])
    assert t1.name == "t1_m1"


def test_expected_name_two_schemas_first_with_method_and_implementation():
    s1 = hf.TaskSchema("t1", method="m1", implementation="i1", actions=[])
    s2 = hf.TaskSchema("t1", actions=[])
    t1 = hf.Task(schema=[s1, s2])
    assert t1.name == "t1_m1_i1"


def test_expected_name_two_schemas_both_with_method():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    s2 = hf.TaskSchema("t1", method="m2", actions=[])
    t1 = hf.Task(schema=[s1, s2])
    assert t1.name == "t1_m1_and_m2"


def test_expected_name_two_schemas_first_with_method_second_with_implementation():
    s1 = hf.TaskSchema("t1", method="m1", actions=[])
    s2 = hf.TaskSchema("t1", implementation="i2", actions=[])
    t1 = hf.Task(schema=[s1, s2])
    assert t1.name == "t1_m1_and_i2"


def test_expected_name_two_schemas_first_with_implementation_second_with_method():
    s1 = hf.TaskSchema("t1", implementation="i1", actions=[])
    s2 = hf.TaskSchema("t1", method="m2", actions=[])
    t1 = hf.Task(schema=[s1, s2])
    assert t1.name == "t1_i1_and_m2"


def test_expected_name_two_schemas_both_with_method_and_implementation():
    s1 = hf.TaskSchema("t1", method="m1", implementation="i1", actions=[])
    s2 = hf.TaskSchema("t1", method="m2", implementation="i2", actions=[])
    t1 = hf.Task(schema=[s1, s2])
    assert t1.name == "t1_m1_i1_and_m2_i2"


def test_raise_on_negative_nesting_order():
    (s1,) = make_schemas(({"p1": None}, ()))
    with pytest.raises(TaskTemplateInvalidNesting):
        hf.Task(schema=s1, nesting_order={"inputs.p1": -1})


# TODO: test resolution of elements and with raise MissingInputs


def test_empty_task_init():
    """Check we can init a hf.Task with no input values."""
    (s1,) = make_schemas(({"p1": None}, ()))
    t1 = hf.Task(schema=s1)


def test_task_task_dependencies(tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    assert wk.tasks.t2.get_task_dependencies(as_objects=True) == [wk.tasks.t1]


def test_task_dependent_tasks(tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    assert wk.tasks.t1.get_dependent_tasks(as_objects=True) == [wk.tasks.t2]


def test_task_element_dependencies(tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    assert wk.tasks.t2.get_element_dependencies() == {0, 1}


def test_task_dependent_elements(tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    assert wk.tasks.t1.get_dependent_elements() == {2, 3}


def test_task_add_elements_without_propagation_expected_workflow_num_elements(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(inputs=[hf.InputValue(param_p1, 103)])
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 1


def test_task_add_elements_without_propagation_expected_task_num_elements(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.tasks.t1.num_elements
    wk.tasks.t1.add_elements(inputs=[hf.InputValue(param_p1, 103)])
    num_elems_new = wk.tasks.t1.num_elements
    assert num_elems_new - num_elems == 1


def test_task_add_elements_without_propagation_expected_new_data_index(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    data_index = [sorted(i.get_data_idx()) for i in wk.tasks.t1.elements[:]]
    wk.tasks.t1.add_elements(inputs=[hf.InputValue(param_p1, 103)])
    data_index_new = [sorted(i.get_data_idx()) for i in wk.tasks.t1.elements[:]]
    new_elems = data_index_new[len(data_index) :]
    assert new_elems == [["inputs.p1", "outputs.p2", "resources.any"]]


def test_task_add_elements_with_propagation_expected_workflow_num_elements(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 103)],
        propagate_to=[hf.ElementPropagation(task=wk.tasks.t2)],
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 2


def test_task_add_elements_with_propagation_expected_task_num_elements(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 103)],
        propagate_to=[hf.ElementPropagation(task=wk.tasks.t2)],
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 1 and num_elems_diff[1] == 1


def test_task_add_elements_with_propagation_expected_new_data_index(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 103)],
        propagate_to=[hf.ElementPropagation(task=wk.tasks.t2)],
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    assert new_elems_t1 == [
        [
            "inputs.p1",
            "outputs.p2",
            "resources.any",
        ]
    ] and new_elems_t2 == [["inputs.p2", "resources.any"]]


def test_task_add_elements_sequence_without_propagation_expected_workflow_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[hf.ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 2


def test_task_add_elements_sequence_without_propagation_expected_task_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.tasks.t1.num_elements
    wk.tasks.t1.add_elements(
        sequences=[hf.ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    num_elems_new = wk.tasks.t1.num_elements
    assert num_elems_new - num_elems == 2


def test_task_add_elements_sequence_without_propagation_expected_new_data_index(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    wk.tasks.t1.add_elements(
        sequences=[hf.ValueSequence("inputs.p1", values=[103, 104], nesting_order=1)]
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems = data_index_new[t1_num_elems:t1_num_elems_new]
    assert new_elems == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
    ]


def test_task_add_elements_sequence_with_propagation_expected_workflow_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(task=wk.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 6


def test_task_add_elements_sequence_with_propagation_expected_task_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(task=wk.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 3 and num_elems_diff[1] == 3


def test_task_add_elements_sequence_with_propagation_expected_new_data_index(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(task=wk.tasks.t2, nesting_order={"inputs.p2": 1}),
        ],
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    assert new_elems_t1 == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
    ] and new_elems_t2 == [
        ["inputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
        ["inputs.p2", "resources.any"],
    ]


def test_task_add_elements_sequence_with_propagation_into_sequence_expected_workflow_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None, "p3": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 1)], 1: [("inputs.p3", 3, 1)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(
                task=wk.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 12


def test_task_add_elements_sequence_with_propagation_into_sequence_expected_task_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None, "p3": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 1)], 1: [("inputs.p3", 3, 1)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(
                task=wk.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff[0] == 3 and num_elems_diff[1] == 9


def test_task_add_elements_sequence_with_propagation_into_sequence_expected_new_data_index(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None, "p3": None}, (), "t2"),
        ],
        local_sequences={0: [("inputs.p1", 2, 1)], 1: [("inputs.p3", 3, 1)]},
        nesting_orders={1: {"inputs.p2": 0}},
        path=tmp_path,
    )

    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[103, 104, 105], nesting_order=1)
        ],
        propagate_to=[
            hf.ElementPropagation(
                task=wk.tasks.t2, nesting_order={"inputs.p2": 1, "inputs.p3": 2}
            ),
        ],
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    assert new_elems_t1 == [
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
        ["inputs.p1", "outputs.p2", "resources.any"],
    ] and new_elems_t2 == [
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
        ["inputs.p2", "inputs.p3", "resources.any"],
    ]


def test_task_add_elements_multi_task_dependence_expected_workflow_num_elements(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 7


def test_task_add_elements_multi_task_dependence_expected_task_num_elements(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [1, 2, 4]


def test_task_add_elements_multi_task_dependence_expected_task_num_elements_custom_input_source(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        input_sources={
            2: {"p3": [hf.InputSource.task(0)]}
        },  # override default (t2 input)
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {
                "nesting_order": {"inputs.p3": 0, "inputs.p4": 1},
                "input_sources": {"p3": [hf.InputSource.task(0)]},  # override default
            },
        },
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [1, 2, 2]


def test_task_add_elements_multi_task_dependence_expected_new_data_index(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    t3_num_elems = wk.tasks.t3.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    t3_num_elems_new = wk.tasks.t3.num_elements
    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    new_elems_t3 = data_index_new[
        t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems : t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems_new
    ]

    assert (
        new_elems_t1 == [["inputs.p1", "outputs.p3", "resources.any"]]
        and new_elems_t2
        == [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 2
        and new_elems_t3 == [["inputs.p3", "inputs.p4", "resources.any"]] * 4
    )


def test_task_add_elements_multi_task_dependence_expected_new_data_index_custom_input_source(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        input_sources={
            2: {"p3": [hf.InputSource.task(0)]}
        },  # override default (t2 input)
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    t3_num_elems = wk.tasks.t3.num_elements
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {
                "nesting_order": {"inputs.p3": 0, "inputs.p4": 1},
                "input_sources": {"p3": [hf.InputSource.task(0)]},  # override default
            },
        },
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    t3_num_elems_new = wk.tasks.t3.num_elements
    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    new_elems_t3 = data_index_new[
        t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems : t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems_new
    ]

    assert (
        new_elems_t1 == [["inputs.p1", "outputs.p3", "resources.any"]]
        and new_elems_t2
        == [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 2
        and new_elems_t3 == [["inputs.p3", "inputs.p4", "resources.any"]] * 2
    )


def test_task_add_elements_sequence_multi_task_dependence_workflow_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 45


def test_task_add_elements_sequence_multi_task_dependence_workflow_num_elements_custom_input_source(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        input_sources={
            2: {"p3": [hf.InputSource.task(0)]}
        },  # override default (t2 input)
        path=tmp_path,
    )
    num_elems = wk.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {
                "nesting_order": {"inputs.p3": 0, "inputs.p4": 1},
                "input_sources": {"p3": [hf.InputSource.task(0)]},  # override default
            },
        },
    )
    num_elems_new = wk.num_elements
    assert num_elems_new - num_elems == 27


def test_task_add_elements_sequence_multi_task_dependence_expected_task_num_elements(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [3, 6, 36]


def test_task_add_elements_sequence_multi_task_dependence_expected_task_num_elements_custom_input_source(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        input_sources={
            2: {"p3": [hf.InputSource.task(0)]}
        },  # override default (t2 input)
        path=tmp_path,
    )
    num_elems = [task.num_elements for task in wk.tasks]
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {
                "nesting_order": {"inputs.p3": 0, "inputs.p4": 1},
                "input_sources": {"p3": [hf.InputSource.task(0)]},  # override default
            },
        },
    )
    num_elems_new = [task.num_elements for task in wk.tasks]
    num_elems_diff = [i - j for i, j in zip(num_elems_new, num_elems)]
    assert num_elems_diff == [3, 6, 18]


def test_task_add_elements_sequence_multi_task_dependence_expected_new_data_index(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    t3_num_elems = wk.tasks.t3.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {"nesting_order": {"inputs.p3": 0, "inputs.p4": 1}},
        },
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    t3_num_elems_new = wk.tasks.t3.num_elements

    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    new_elems_t3 = data_index_new[
        t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems : t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems_new
    ]
    assert (
        new_elems_t1 == [["inputs.p1", "outputs.p3", "resources.any"]] * 3
        and new_elems_t2
        == [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 6
        and new_elems_t3 == [["inputs.p3", "inputs.p4", "resources.any"]] * 36
    )


def test_task_add_elements_sequence_multi_task_dependence_expected_new_data_index_custom_input_source(
    tmp_path: Path,
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p3",), "t1"),
            ({"p2": None, "p3": None}, ("p4",), "t2"),
            ({"p3": None, "p4": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 1)]},
        nesting_orders={2: {"inputs.p3": 0, "inputs.p4": 1}},
        input_sources={
            2: {"p3": [hf.InputSource.task(0)]}
        },  # override default (t2 input)
        path=tmp_path,
    )
    t1_num_elems = wk.tasks.t1.num_elements
    t2_num_elems = wk.tasks.t2.num_elements
    t3_num_elems = wk.tasks.t3.num_elements
    wk.tasks.t1.add_elements(
        sequences=[
            hf.ValueSequence("inputs.p1", values=[102, 103, 104], nesting_order=1)
        ],
        propagate_to={
            "t2": {"nesting_order": {"inputs.p2": 0, "inputs.p3": 1}},
            "t3": {
                "nesting_order": {"inputs.p3": 0, "inputs.p4": 1},
                "input_sources": {"p3": [hf.InputSource.task(0)]},  # override default
            },
        },
    )
    t1_num_elems_new = wk.tasks.t1.num_elements
    t2_num_elems_new = wk.tasks.t2.num_elements
    t3_num_elems_new = wk.tasks.t3.num_elements

    data_index_new = [sorted(i.get_data_idx()) for i in wk.elements()]
    new_elems_t1 = data_index_new[t1_num_elems:t1_num_elems_new]
    new_elems_t2 = data_index_new[
        t1_num_elems_new + t2_num_elems : t1_num_elems_new + t2_num_elems_new
    ]
    new_elems_t3 = data_index_new[
        t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems : t1_num_elems_new
        + t2_num_elems_new
        + t3_num_elems_new
    ]
    assert (
        new_elems_t1 == [["inputs.p1", "outputs.p3", "resources.any"]] * 3
        and new_elems_t2
        == [["inputs.p2", "inputs.p3", "outputs.p4", "resources.any"]] * 6
        and new_elems_t3 == [["inputs.p3", "inputs.p4", "resources.any"]] * 18
    )


def test_task_add_elements_simple_dependence_three_tasks(
    tmp_path: Path, param_p1: Parameter
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2",), "t1"),
            ({"p2": None}, ("p3",), "t2"),
            ({"p3": None}, (), "t3"),
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    num_elems = [i.num_elements for i in wk.tasks]
    wk.tasks.t1.add_elements(
        inputs=[hf.InputValue(param_p1, 102)],
        propagate_to={"t2": {}, "t3": {}},
    )
    num_elems_new = [i.num_elements for i in wk.tasks]
    assert num_elems_new == [i + 1 for i in num_elems]


def test_no_change_to_tasks_metadata_on_add_task_failure(tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[({"p1": NullDefault.NULL}, (), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    tasks_meta = copy.deepcopy(wk._store.get_tasks())

    (s2,) = make_schemas(({"p1": NullDefault.NULL, "p3": NullDefault.NULL}, ()))
    t2 = hf.Task(schema=s2)
    with pytest.raises(MissingInputs) as exc_info:
        wk.add_task(t2)

    assert wk._store.get_tasks() == tasks_meta


def test_no_change_to_parameter_data_on_add_task_failure(
    tmp_path: Path, param_p2: Parameter, param_p3: Parameter
):
    wk = make_workflow(
        schemas_spec=[({"p1": NullDefault.NULL}, (), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    param_data: list = copy.deepcopy(wk.get_all_parameters())
    (s2,) = make_schemas(
        ({"p1": NullDefault.NULL, "p2": NullDefault.NULL, "p3": NullDefault.NULL}, ())
    )
    t2 = hf.Task(schema=s2, inputs=[hf.InputValue(param_p2, 201)])
    with pytest.raises(MissingInputs) as exc_info:
        wk.add_task(t2)

    assert wk.get_all_parameters() == param_data


def test_expected_additional_parameter_data_on_add_task(
    tmp_path: Path, param_p3: Parameter
):
    wk = make_workflow(
        schemas_spec=[({"p1": NullDefault.NULL}, (), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    param_data = copy.deepcopy(wk.get_all_parameter_data())

    (s2,) = make_schemas(({"p1": NullDefault.NULL, "p3": NullDefault.NULL}, ()))
    t2 = hf.Task(schema=s2, inputs=[hf.InputValue(param_p3, 301)])
    wk.add_task(t2)

    param_data_new = wk.get_all_parameter_data()

    new_keys = sorted(set(param_data_new).difference(param_data))
    new_data = [param_data_new[k] for k in new_keys]

    # one new key for resources, one for param_p3 value
    assert len(new_data) == 2
    assert new_data[1] == 301


def test_parameters_accepted_on_add_task(tmp_path: Path, param_p3: Parameter):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, (), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    (s2,) = make_schemas(({"p1": None, "p3": None}, ()))
    t2 = hf.Task(schema=s2, inputs=[hf.InputValue(param_p3, 301)])
    wk.add_task(t2)
    assert not wk._store._pending.add_parameters


def test_parameters_pending_during_add_task(tmp_path: Path, param_p3: Parameter):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, (), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
    )
    (s2,) = make_schemas(({"p1": None, "p3": None}, ()))
    t2 = hf.Task(schema=s2, inputs=[hf.InputValue(param_p3, 301)])
    with wk.batch_update():
        wk.add_task(t2)
        assert wk._store._pending.add_parameters


def test_add_task_after(workflow_w0: Workflow):
    new_task = hf.Task(schema=hf.TaskSchema(objective="after_t1", actions=[]))
    workflow_w0.add_task_after(new_task, workflow_w0.tasks.t1)
    assert [i.name for i in workflow_w0.tasks] == ["t1", "after_t1", "t2"]


def test_add_task_after_no_ref(workflow_w0: Workflow):
    new_task = hf.Task(schema=hf.TaskSchema(objective="at_end", actions=[]))
    workflow_w0.add_task_after(new_task)
    assert [i.name for i in workflow_w0.tasks] == ["t1", "t2", "at_end"]


def test_add_task_before(workflow_w0: Workflow):
    new_task = hf.Task(schema=hf.TaskSchema(objective="before_t2", actions=[]))
    workflow_w0.add_task_before(new_task, workflow_w0.tasks.t2)
    assert [i.name for i in workflow_w0.tasks] == ["t1", "before_t2", "t2"]


def test_add_task_before_no_ref(workflow_w0: Workflow):
    new_task = hf.Task(schema=hf.TaskSchema(objective="at_start", actions=[]))
    workflow_w0.add_task_before(new_task)
    assert [i.name for i in workflow_w0.tasks] == ["at_start", "t1", "t2"]


def test_parameter_two_modifying_actions_expected_data_indices(
    tmp_path: Path, act_env_1: ActionEnvironment, param_p1: Parameter
):
    act1 = hf.Action(
        commands=[hf.Command("doSomething <<parameter:p1>>", stdout="<<parameter:p1>>")],
        environments=[act_env_1],
    )
    act2 = hf.Action(
        commands=[hf.Command("doSomething <<parameter:p1>>", stdout="<<parameter:p1>>")],
        environments=[act_env_1],
    )

    s1 = hf.TaskSchema("t1", actions=[act1, act2], inputs=[param_p1], outputs=[param_p1])
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue(param_p1, 101)])

    wkt = hf.WorkflowTemplate(name="w3", tasks=[t1])
    wk = hf.Workflow.from_template(template=wkt, path=tmp_path)
    iter_0 = wk.tasks.t1.elements[0].iterations[0]
    act_runs = iter_0.action_runs

    p1_idx_schema_in = iter_0.data_idx["inputs.p1"]
    p1_idx_schema_out = iter_0.data_idx["outputs.p1"]

    p1_idx_0 = act_runs[0].data_idx["inputs.p1"]
    p1_idx_1 = act_runs[0].data_idx["outputs.p1"]
    p1_idx_2 = act_runs[1].data_idx["inputs.p1"]
    p1_idx_3 = act_runs[1].data_idx["outputs.p1"]

    assert (
        p1_idx_schema_in == p1_idx_0
        and p1_idx_1 == p1_idx_2
        and p1_idx_3 == p1_idx_schema_out
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_conditional_shell_schema_single_initialised_action(
    null_config, tmp_path: Path, store: str
):
    rules = {
        "posix": hf.ActionRule(
            rule=hf.Rule(path="resources.os_name", condition=Value.equal_to("posix"))
        ),
        "nt": hf.ActionRule(
            rule=hf.Rule(path="resources.os_name", condition=Value.equal_to("nt"))
        ),
    }
    s1 = hf.TaskSchema(
        objective="test_conditional_on_shell",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p2")],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p1>> + 100))",
                        stdout="<<parameter:p2>>",
                    )
                ],
                rules=[rules["posix"]],
            ),
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[
                    hf.Command(
                        command="Write-Output ((<<parameter:p1>> + 100))",
                        stdout="<<parameter:p2>>",
                    )
                ],
                rules=[rules["nt"]],
            ),
        ],
    )
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue("p1", 101)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    action_runs = wk.tasks[0].elements[0].iterations[0].action_runs
    assert len(action_runs) == 1
    assert wk.tasks[0].elements[0].iterations[0].EARs_initialised
    assert action_runs[0].action.rules[0] == rules[os.name]


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_element_iteration_EARs_initialised_on_make_workflow(
    null_config, tmp_path: Path, store: str
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p2")],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p1>> + 100))",
                        stdout="<<parameter:p2>>",
                    )
                ],
            ),
        ],
    )
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue("p1", 101)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    assert wk.tasks[0].elements[0].iterations[0].EARs_initialised
    assert len(wk.tasks[0].elements[0].iterations[0].action_runs) == 1


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_element_iteration_EARs_initialised_on_make_workflow_with_no_actions(
    null_config, tmp_path: Path, store: str
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        actions=[],
    )
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue("p1", 101)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    assert wk.tasks[0].elements[0].iterations[0].EARs_initialised
    assert not wk.tasks[0].elements[0].iterations[0].action_runs


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_element_iteration_EARs_not_initialised_on_make_workflow_due_to_unset(
    null_config, tmp_path: Path, store: str
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p2")],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p1>> + 100))",
                        stdout="<<parameter:p2>>",
                    )
                ],
            ),
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaInput("p3")],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p2>> + 100))",
                        stdout="<<parameter:p3>>",
                    )
                ],
                rules=[hf.ActionRule(path="inputs.p2", condition=Value.less_than(500))],
            ),
        ],
    )
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue("p1", 101)])
    t2 = hf.Task(schema=[s2])
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    # second task cannot initialise runs because it depends on the value of an output of
    # the first task:
    assert not wk.tasks[1].elements[0].iterations[0].EARs_initialised


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_element_iteration_EARs_initialised_on_make_workflow_with_no_valid_actions(
    null_config, tmp_path: Path, store: str
):
    rules = {
        "posix": hf.ActionRule(
            rule=hf.Rule(path="resources.os_name", condition=Value.equal_to("posix"))
        ),
        "nt": hf.ActionRule(
            rule=hf.Rule(path="resources.os_name", condition=Value.equal_to("nt"))
        ),
    }
    s1 = hf.TaskSchema(
        objective="test_conditional_on_shell",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p2")],
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment("null_env")],
                commands=[
                    hf.Command(
                        command="some command that uses <<parameter:p1>>",
                        stdout="<<parameter:p2>>",
                    )
                ],
                rules=[rules["posix"] if os.name == "nt" else rules["nt"]],
            ),
        ],
    )
    t1 = hf.Task(schema=[s1], inputs=[hf.InputValue("p1", 101)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    action_runs = wk.tasks[0].elements[0].iterations[0].action_runs
    assert len(action_runs) == 0
    assert wk.tasks[0].elements[0].iterations[0].EARs_initialised


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_unset_data_raise(
    null_config, tmp_path: Path, store: str
):
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
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=1)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx = wk.tasks.t1.elements[0].get_data_idx()
    with pytest.raises(UnsetParameterDataError):
        wk.tasks.t1._get_merged_parameter_data(
            data_index=data_idx,
            path="outputs.p2",
            raise_on_unset=True,
        )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_unset_data_no_raise(
    null_config, tmp_path: Path, store: str
):
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
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=1)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx = wk.tasks.t1.elements[0].get_data_idx()
    assert None == wk.tasks.t1._get_merged_parameter_data(
        data_index=data_idx,
        path="outputs.p2",
        raise_on_unset=False,
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_missing_data_raise(
    null_config, tmp_path: Path, store: str
):
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
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=1)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx = wk.tasks.t1.elements[0].get_data_idx()
    with pytest.raises(ValueError):
        wk.tasks.t1._get_merged_parameter_data(
            data_index=data_idx,
            path="inputs.p4",
            raise_on_missing=True,
        )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_missing_data_no_raise(
    null_config, tmp_path: Path, store: str
):
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
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=1)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx = wk.tasks.t1.elements[0].get_data_idx()
    assert None == wk.tasks.t1._get_merged_parameter_data(
        data_index=data_idx,
        path="inputs.p4",
        raise_on_missing=False,
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_group_unset_data_raise(
    null_config, tmp_path: Path, store: str
):
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
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"), group="my_group")],
    )
    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2], nesting_order=0)],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(schema=s2)
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx_t1 = wk.tasks.t1.elements[0].get_data_idx()
    data_idx_t2 = wk.tasks.t2.elements[0].get_data_idx()
    with pytest.raises(UnsetParameterDataError):
        wk.tasks.t1._get_merged_parameter_data(
            data_index=data_idx_t1,
            path="outputs.p2",
            raise_on_unset=True,
        )
    with pytest.raises(UnsetParameterDataError):
        wk.tasks.t2._get_merged_parameter_data(
            data_index=data_idx_t2,
            path="inputs.p2",
            raise_on_unset=True,
        )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_group_unset_data_no_raise(
    null_config, tmp_path: Path, store: str
):
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
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"), group="my_group")],
    )
    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2], nesting_order=0)],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(schema=s2)
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx_t1 = wk.tasks.t1.elements[0].get_data_idx()
    data_idx_t2 = wk.tasks.t2.elements[0].get_data_idx()
    assert None == wk.tasks.t1._get_merged_parameter_data(
        data_index=data_idx_t1,
        path="outputs.p2",
        raise_on_unset=False,
    )
    assert [None, None] == wk.tasks.t2._get_merged_parameter_data(
        data_index=data_idx_t2,
        path="inputs.p2",
        raise_on_unset=False,
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_group_missing_data_raise(
    null_config, tmp_path: Path, store: str
):
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
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"), group="my_group")],
    )
    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2], nesting_order=0)],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(schema=s2)
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx_t1 = wk.tasks.t1.elements[0].get_data_idx()
    data_idx_t2 = wk.tasks.t2.elements[0].get_data_idx()
    with pytest.raises(ValueError):
        wk.tasks.t1._get_merged_parameter_data(
            data_index=data_idx_t1,
            path="outputs.p4",
            raise_on_missing=True,
        )
    with pytest.raises(ValueError):
        wk.tasks.t2._get_merged_parameter_data(
            data_index=data_idx_t2,
            path="inputs.p4",
            raise_on_missing=True,
        )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_merged_parameter_data_group_missing_data_no_raise(
    null_config, tmp_path: Path, store: str
):
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
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"), group="my_group")],
    )
    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2], nesting_order=0)],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(schema=s2)
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
        store=store,
    )
    data_idx_t1 = wk.tasks.t1.elements[0].get_data_idx()
    data_idx_t2 = wk.tasks.t2.elements[0].get_data_idx()
    assert None == wk.tasks.t1._get_merged_parameter_data(
        data_index=data_idx_t1,
        path="outputs.p4",
        raise_on_missing=False,
    )
    assert None == wk.tasks.t2._get_merged_parameter_data(
        data_index=data_idx_t2,
        path="inputs.p4",
        raise_on_missing=False,
    )


@pytest.fixture
def path_to_PV_classes_workflow(null_config, tmp_path: Path) -> Workflow:
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(commands=[hf.Command("Write-Output (<<parameter:p1c>> + 100)")])
        ],
    )
    p1_value = P1(a=10, sub_param=P1_sub_param(e=5))
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        overwrite=True,
        path=tmp_path,
    )
    return wk


def test_path_to_PV_classes(path_to_PV_classes_workflow: Workflow):
    assert path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes("inputs.p1c") == {
        "inputs.p1c": P1,
    }


def test_path_to_PV_classes_sub_data(path_to_PV_classes_workflow: Workflow):
    assert path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes("inputs.p1c.a") == {
        "inputs.p1c": P1,
    }


def test_path_to_PV_classes_sub_parameter(path_to_PV_classes_workflow: Workflow):
    assert path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes(
        "inputs.p1c.sub_param"
    ) == {
        "inputs.p1c": P1,
        "inputs.p1c.sub_param": P1_sub_param,
    }


def test_path_to_PV_classes_multiple_sub_parameters(
    path_to_PV_classes_workflow: Workflow,
):
    paths = ["inputs.p1c.sub_param", "inputs.p1c.sub_param_2"]
    assert path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes(*paths) == {
        "inputs.p1c": P1,
        "inputs.p1c.sub_param": P1_sub_param,
        "inputs.p1c.sub_param_2": P1_sub_param_2,
    }


def test_path_to_PV_classes_multiple_sub_parameter_attr(
    path_to_PV_classes_workflow: Workflow,
):
    paths = ["inputs.p1c.sub_param.e"]
    assert path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes(*paths) == {
        "inputs.p1c": P1,
        "inputs.p1c.sub_param": P1_sub_param,
    }


def test_path_to_PV_classes_inputs_only_path_ignored(
    path_to_PV_classes_workflow: Workflow,
):
    paths_1 = ["inputs", "inputs.p1c"]
    paths_2 = ["inputs.p1c"]
    assert path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes(
        *paths_1
    ) == path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes(*paths_2)


def test_path_to_PV_classes_resources_path_ignored(path_to_PV_classes_workflow: Workflow):
    paths_1 = ["resources", "inputs.p1c"]
    paths_2 = ["inputs.p1c"]
    assert path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes(
        *paths_1
    ) == path_to_PV_classes_workflow.tasks.t1._paths_to_PV_classes(*paths_2)


def test_input_values_specified_by_dict(null_config):
    ts = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput("p1")])
    t1 = hf.Task(schema=ts, inputs=[hf.InputValue(parameter="p1", value=101)])
    t2 = hf.Task(schema=ts, inputs={"p1": 101})
    assert t1 == t2


def test_labelled_input_values_specified_by_dict(null_config):
    ts = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput("p1", labels={"one": {}})])
    t1 = hf.Task(
        schema=ts, inputs=[hf.InputValue(parameter="p1", label="one", value=101)]
    )
    t2 = hf.Task(schema=ts, inputs={"p1[one]": 101})
    assert t1 == t2


def test_raise_UnknownEnvironmentPresetError(null_config):
    ts = hf.TaskSchema(objective="t1")
    with pytest.raises(UnknownEnvironmentPresetError):
        hf.Task(schema=ts, env_preset="my_env_preset")


def test_raise_UnknownEnvironmentPresetError_sequence(null_config):
    ts = hf.TaskSchema(objective="t1")
    seq = hf.ValueSequence(path="env_preset", values=["my_env_preset"])
    with pytest.raises(UnknownEnvironmentPresetError):
        hf.Task(schema=ts, sequences=[seq])


def test_group_values_input_and_output_source_from_upstream(null_config, tmp_path: Path):
    """
    | task | inputs | outputs | group    | num_elements               |
    | ---- | ------ | ------- | -------- | ---------------------------|
    | t1   | p0     | p1      | -        | 3                          |
    | t2   | p1     | p2      | my_group | 3                          |
    | t3   | p1, p2 | -       | -        | 1 (grouped p1, grouped p2) |
    """
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p0")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo <<parameter:p0>> + 1",
                        stdout="<<parameter:p1>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo <<parameter:p1>> + 1",
                        stdout="<<parameter:p2>>",
                    )
                ]
            )
        ],
    )
    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[
            hf.SchemaInput("p1", group="my_group"),
            hf.SchemaInput("p2", group="my_group"),
        ],
    )
    t1 = hf.Task(
        schema=s1,
        inputs={"p0": 1},
        repeats=3,
    )
    t2 = hf.Task(schema=s2, groups=[hf.ElementGroup("my_group")])
    t3 = hf.Task(schema=s3)
    wk = hf.Workflow.from_template_data(
        template_name="test_group",
        tasks=[t1, t2, t3],
        path=tmp_path,
    )
    assert wk.tasks[0].num_elements == 3
    assert wk.tasks[1].num_elements == 3
    assert wk.tasks[2].num_elements == 1
    assert [i.value for i in wk.tasks[2].inputs.p1] == [[None, None, None]]
    assert [i.value for i in wk.tasks[2].inputs.p2] == [[None, None, None]]


def test_is_input_type_required_True(null_config):
    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[hf.Command("cat <<file:my_input_file>>")],
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=inp_file,
                        inputs=[hf.Parameter("p1")],
                        script="NOT-SET-FOR-THIS-TEST",
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    t1 = hf.Task(schema=s1, inputs={"p1": 100})
    assert t1.is_input_type_required(typ="p1", element_set=t1.element_sets[0])


def test_is_input_type_required_False(null_config):
    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[hf.Command("cat <<file:my_input_file>>")],
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=inp_file,
                        inputs=[hf.Parameter("p1")],
                        script="NOT-SET-FOR-THIS-TEST",
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    t1 = hf.Task(
        schema=s1, input_files=[hf.InputFile(file=inp_file, path="NOT-SET-FOR-THIS-TEST")]
    )
    assert not t1.is_input_type_required(typ="p1", element_set=t1.element_sets[0])
