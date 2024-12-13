from __future__ import annotations
from typing import TYPE_CHECKING
import numpy as np
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    InapplicableInputSourceElementIters,
    MissingInputs,
    NoCoincidentInputSources,
    UnavailableInputSource,
)
from hpcflow.sdk.core.test_utils import (
    P1_parameter_cls as P1,
    P1_sub_parameter_cls as P1_sub,
    make_schemas,
)

if TYPE_CHECKING:
    from pathlib import Path
    from hpcflow.sdk.core.parameters import Parameter
    from hpcflow.sdk.core.types import RuleArgs


def test_input_source_class_method_local() -> None:
    assert hf.InputSource.local() == hf.InputSource(hf.InputSourceType.LOCAL)


def test_input_source_class_method_default() -> None:
    assert hf.InputSource.default() == hf.InputSource(hf.InputSourceType.DEFAULT)


def test_input_source_class_method_task() -> None:
    task_ref = 0
    assert hf.InputSource.task(task_ref) == hf.InputSource(
        source_type=hf.InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_class_method_import() -> None:
    import_ref = (
        0  # TODO: interface to imports (and so how to reference) is not yet decided
    )
    assert hf.InputSource.import_(import_ref) == hf.InputSource(
        hf.InputSourceType.IMPORT, import_ref=import_ref
    )


def test_input_source_class_method_task_same_default_task_source_type() -> None:
    task_ref = 0
    assert (
        hf.InputSource(hf.InputSourceType.TASK, task_ref=task_ref).task_source_type
        == hf.InputSource.task(task_ref=task_ref).task_source_type
    )


def test_input_source_validate_source_type_string_local() -> None:
    assert hf.InputSource("local") == hf.InputSource(hf.InputSourceType.LOCAL)


def test_input_source_validate_source_type_string_default() -> None:
    assert hf.InputSource("default") == hf.InputSource(hf.InputSourceType.DEFAULT)


def test_input_source_validate_source_type_string_task() -> None:
    task_ref = 0
    assert hf.InputSource("task", task_ref=task_ref) == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_validate_source_type_string_import() -> None:
    import_ref = (
        0  # TODO: interface to imports (and so how to reference) is not yet decided
    )
    assert hf.InputSource("import", import_ref=import_ref) == hf.InputSource(
        hf.InputSourceType.IMPORT, import_ref=import_ref
    )


def test_input_source_validate_source_type_raise_on_unknown_string() -> None:
    with pytest.raises(ValueError):
        hf.InputSource("bad_source_type")


def test_input_source_validate_task_source_type_string_any() -> None:
    task_ref = 0
    assert hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type="any"
    ) == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type=hf.TaskSourceType.ANY
    )


def test_input_source_validate_task_source_type_string_input() -> None:
    task_ref = 0
    assert hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type="input"
    ) == hf.InputSource(
        hf.InputSourceType.TASK,
        task_ref=task_ref,
        task_source_type=hf.TaskSourceType.INPUT,
    )


def test_input_source_validate_task_source_type_string_output() -> None:
    task_ref = 0
    assert hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref, task_source_type="output"
    ) == hf.InputSource(
        hf.InputSourceType.TASK,
        task_ref=task_ref,
        task_source_type=hf.TaskSourceType.OUTPUT,
    )


def test_input_source_validate_task_source_type_raise_on_unknown_string() -> None:
    task_ref = 0
    with pytest.raises(ValueError):
        hf.InputSource(
            hf.InputSourceType.TASK,
            task_ref=task_ref,
            task_source_type="bad_task_source_type",
        )


def test_input_source_to_string_local() -> None:
    assert hf.InputSource.local().to_string() == "local"


def test_input_source_to_string_default() -> None:
    assert hf.InputSource.default().to_string() == "default"


def test_input_source_to_string_task_output() -> None:
    task_ref = 0
    assert (
        hf.InputSource.task(task_ref, task_source_type="output").to_string()
        == f"task.{task_ref}.output"
    )


def test_input_source_to_string_task_input() -> None:
    task_ref = 0
    assert (
        hf.InputSource.task(task_ref, task_source_type="input").to_string()
        == f"task.{task_ref}.input"
    )


def test_input_source_to_string_task_any() -> None:
    task_ref = 0
    assert (
        hf.InputSource.task(task_ref, task_source_type="any").to_string()
        == f"task.{task_ref}.any"
    )


def test_input_source_to_string_import() -> None:
    import_ref = 0
    assert hf.InputSource.import_(import_ref).to_string() == f"import.{import_ref}"


def test_input_source_from_string_local() -> None:
    assert hf.InputSource.from_string("local") == hf.InputSource(hf.InputSourceType.LOCAL)


def test_input_source_from_string_default() -> None:
    assert hf.InputSource.from_string("default") == hf.InputSource(
        hf.InputSourceType.DEFAULT
    )


def test_input_source_from_string_task() -> None:
    assert hf.InputSource.from_string("task.0.output") == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=0, task_source_type=hf.TaskSourceType.OUTPUT
    )


def test_input_source_from_string_task_same_default_task_source() -> None:
    task_ref = 0
    assert hf.InputSource.from_string(f"task.{task_ref}") == hf.InputSource(
        hf.InputSourceType.TASK, task_ref=task_ref
    )


def test_input_source_from_string_import() -> None:
    import_ref = 0
    assert hf.InputSource.from_string(f"import.{import_ref}") == hf.InputSource(
        hf.InputSourceType.IMPORT, import_ref=import_ref
    )


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
def null_config(tmp_path: Path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_specified_sourceable_elements_subset(
    null_config,
    param_p1: Parameter,
    param_p2: Parameter,
    param_p3: Parameter,
    tmp_path: Path,
):
    input_p1 = hf.SchemaInput(param_p1, default_value=1001)
    input_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    input_p3 = hf.SchemaInput(param_p3)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[input_p1], outputs=[input_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[input_p2, input_p3])

    t1 = hf.Task(
        schema=s1,
        sequences=[
            hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=0),
        ],
    )
    t2 = hf.Task(
        schema=s2,
        inputs=[hf.InputValue(input_p2, 201)],
        sourceable_elem_iters=[0],
        nesting_order={"inputs.p3": 1},
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert (
        wk.tasks[1].num_elements == 1
        and wk.tasks[1].elements[0].input_sources["inputs.p3"] == "element.0.OUTPUT"
    )


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_specified_sourceable_elements_all_available(
    null_config,
    param_p1: Parameter,
    param_p2: Parameter,
    param_p3: Parameter,
    tmp_path: Path,
):
    input_p1 = hf.SchemaInput(param_p1, default_value=1001)
    input_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    input_p3 = hf.SchemaInput(param_p3)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[input_p1], outputs=[input_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[input_p2, input_p3])

    t1 = hf.Task(
        schema=s1,
        sequences=[
            hf.ValueSequence("inputs.p1", values=[101, 102], nesting_order=0),
        ],
    )
    t2 = hf.Task(
        schema=s2,
        inputs=[hf.InputValue(input_p2, 201)],
        sourceable_elem_iters=[0, 1],
        nesting_order={"inputs.p3": 1},
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert (
        wk.tasks[1].num_elements == 2
        and wk.tasks[1].elements[0].input_sources["inputs.p3"] == "element.0.OUTPUT"
        and wk.tasks[1].elements[1].input_sources["inputs.p3"] == "element.1.OUTPUT"
    )


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_no_sourceable_elements_so_raise_missing(
    null_config,
    param_p1: Parameter,
    param_p2: Parameter,
    param_p3: Parameter,
    tmp_path: Path,
):
    input_p1 = hf.SchemaInput(param_p1, default_value=1001)
    input_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    input_p3 = hf.SchemaInput(param_p3)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[input_p1], outputs=[input_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[input_p2, input_p3])

    t1 = hf.Task(schema=s1, inputs=[hf.InputValue(input_p1, 101)])
    t2 = hf.Task(
        schema=s2,
        inputs=[hf.InputValue(input_p2, 201)],
        sourceable_elem_iters=[],
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])

    with pytest.raises(MissingInputs):
        _ = hf.Workflow.from_template(wkt, path=tmp_path)


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_no_sourceable_elements_so_default_used(
    null_config,
    param_p1: Parameter,
    param_p2: Parameter,
    param_p3: Parameter,
    tmp_path: Path,
):
    input_p1 = hf.SchemaInput(param_p1, default_value=1001)
    input_p2 = hf.SchemaInput(param_p2, default_value=np.array([2002, 2003]))
    input_p3 = hf.SchemaInput(param_p3, default_value=3001)

    s1 = hf.TaskSchema("ts1", actions=[], inputs=[input_p1], outputs=[input_p3])
    s2 = hf.TaskSchema("ts2", actions=[], inputs=[input_p2, input_p3])

    t1 = hf.Task(schema=s1, inputs=[hf.InputValue(input_p1, 101)])
    t2 = hf.Task(
        schema=s2,
        inputs=[hf.InputValue(input_p2, 201)],
        sourceable_elem_iters=[],
    )

    wkt = hf.WorkflowTemplate(name="w1", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert wk.tasks[1].elements[0].input_sources["inputs.p3"] == "default"


def test_equivalent_where_args() -> None:
    rule_args: RuleArgs = {"path": "inputs.p1", "condition": {"value.equal_to": 1}}
    i1 = hf.InputSource.task(task_ref=0, where=rule_args)
    i2 = hf.InputSource.task(task_ref=0, where=[rule_args])
    i3 = hf.InputSource.task(task_ref=0, where=hf.Rule(**rule_args))
    i4 = hf.InputSource.task(task_ref=0, where=[hf.Rule(**rule_args)])
    i5 = hf.InputSource.task(task_ref=0, where=hf.ElementFilter([hf.Rule(**rule_args)]))
    assert i1 == i2 == i3 == i4 == i5


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_source_where(null_config, tmp_path: Path, store: str):
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
            sequences=[
                hf.ValueSequence(path="inputs.p1", values=[1, 2], nesting_order=0)
            ],
        ),
        hf.Task(
            schema=s2,
            nesting_order={"inputs.p2": 0},
            input_sources={
                "p2": [
                    hf.InputSource.task(
                        task_ref=0,
                        where=hf.Rule(path="inputs.p1", condition={"value.equal_to": 2}),
                    )
                ]
            },
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
        store=store,
    )
    assert wk.tasks.t2.num_elements == 1
    assert (
        wk.tasks.t2.elements[0].get_data_idx("inputs.p2")["inputs.p2"]
        == wk.tasks.t1.elements[1].get_data_idx("outputs.p2")["outputs.p2"]
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_source_where_parameter_value_class_sub_parameter(
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
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
    )
    tasks = [
        hf.Task(
            schema=s1,
            sequences=[
                hf.ValueSequence(
                    path="inputs.p1", values=[P1(a=1), P1(a=2)], nesting_order=0
                )
            ],
        ),
        hf.Task(
            schema=s2,
            nesting_order={"inputs.p2": 0},
            input_sources={
                "p2": [
                    hf.InputSource.task(
                        task_ref=0,
                        where=hf.Rule(
                            path="inputs.p1.a", condition={"value.equal_to": 2}
                        ),
                    )
                ]
            },
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
        store=store,
    )
    assert wk.tasks.t2.num_elements == 1
    assert (
        wk.tasks.t2.elements[0].get_data_idx("inputs.p2")["inputs.p2"]
        == wk.tasks.t1.elements[1].get_data_idx("outputs.p2")["outputs.p2"]
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_input_source_where_parameter_value_class_sub_parameter_property(
    null_config, tmp_path: Path, store: str
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c>> + 100)",
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
            sequences=[
                hf.ValueSequence(
                    path="inputs.p1c", values=[P1(a=1), P1(a=2)], nesting_order=0
                )
            ],
        ),
        hf.Task(
            schema=s2,
            nesting_order={"inputs.p2": 0},
            input_sources={
                "p2": [
                    hf.InputSource.task(
                        task_ref=0,
                        where=hf.Rule(
                            path="inputs.p1c.twice_a", condition={"value.equal_to": 4}
                        ),
                    )
                ]
            },
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
        store=store,
    )
    assert wk.tasks.t2.num_elements == 1
    assert (
        wk.tasks.t2.elements[0].get_data_idx("inputs.p2")["inputs.p2"]
        == wk.tasks.t1.elements[1].get_data_idx("outputs.p2")["outputs.p2"]
    )


def test_sub_parameter_task_input_source_excluded_when_root_parameter_is_task_output_source(
    null_config, tmp_path: Path
):
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
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("p1c")),
            hf.SchemaInput(parameter=hf.Parameter("p2")),
        ],
    )
    t1 = hf.Task(
        schema=s1,
        inputs=[
            hf.InputValue("p1c", value=P1(a=10, sub_param=P1_sub(e=5))),
            hf.InputValue("p1c", path="a", value=20),
        ],
    )
    t2 = hf.Task(
        schema=s2,
        inputs=[hf.InputValue("p2", value=201)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        path=tmp_path,
    )
    # "p1c.a" source should not be included, because it would be a task-input source, which
    # should be overridden by the "p1c" task-output source:
    assert wk.tasks.t2.template.element_sets[0].input_sources == {
        "p1c": [
            hf.InputSource.task(task_ref=0, task_source_type="output", element_iters=[0])
        ],
        "p2": [hf.InputSource.local()],
    }


def test_sub_parameter_task_input_source_included_when_root_parameter_is_task_input_source(
    null_config, tmp_path: Path
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c>> + 100)",
                    )
                ],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("p1c")),
            hf.SchemaInput(parameter=hf.Parameter("p2")),
        ],
    )
    t1 = hf.Task(
        schema=s1,
        inputs=[
            hf.InputValue("p1c", value=P1(a=10, sub_param=P1_sub(e=5))),
            hf.InputValue("p1c", path="a", value=20),
        ],
    )
    t2 = hf.Task(
        schema=s2,
        inputs=[hf.InputValue("p2", value=201)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        path=tmp_path,
    )
    assert wk.tasks.t2.template.element_sets[0].input_sources == {
        "p1c": [
            hf.InputSource.task(task_ref=0, task_source_type="input", element_iters=[0])
        ],
        "p1c.a": [
            hf.InputSource.task(task_ref=0, task_source_type="input", element_iters=[0])
        ],
        "p2": [hf.InputSource.local()],
    }


def test_sub_parameter_task_input_source_allowed_when_root_parameter_is_task_output_source(
    null_config, tmp_path: Path
):
    """Check we can override the default behaviour and specify that the sub-parameter
    task-input source should be used despite the root-parameter being a task-output
    source."""
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
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("p1c")),
            hf.SchemaInput(parameter=hf.Parameter("p2")),
        ],
    )
    t1 = hf.Task(
        schema=s1,
        inputs=[
            hf.InputValue("p1c", value=P1(a=10, sub_param=P1_sub(e=5))),
            hf.InputValue("p1c", path="a", value=20),
        ],
    )
    t2 = hf.Task(
        schema=s2,
        inputs=[hf.InputValue("p2", value=201)],
        input_sources={
            "p1c.a": [hf.InputSource.task(task_ref=0, task_source_type="input")]
        },
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="w1",
        path=tmp_path,
    )
    assert wk.tasks.t2.template.element_sets[0].input_sources == {
        "p1c": [
            hf.InputSource.task(task_ref=0, task_source_type="output", element_iters=[0])
        ],
        "p1c.a": [
            hf.InputSource.task(task_ref=0, task_source_type="input", element_iters=[0])
        ],
        "p2": [hf.InputSource.local()],
    }


def test_raise_unavailable_input_source(null_config, tmp_path: Path):
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs={"p1": 1})
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={"p1": [hf.InputSource.local()]},
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    with pytest.raises(UnavailableInputSource):
        hf.Workflow.from_template(wkt, path=tmp_path)


def test_input_source_specify_element_iters(null_config, tmp_path: Path):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[{"a": 1}, {"a": 2}, {"a": 3}],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 2]
                )
            ]
        },
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert len(wk.tasks[1].elements) == 2
    assert [i.value["a"] for i in wk.tasks[1].inputs.p1] == [1, 3]


def test_input_source_raise_on_inapplicable_specified_element_iters(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[{"a": 1}, {"a": 2}, {"a": 3}],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 4]
                )
            ]
        },
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    with pytest.raises(InapplicableInputSourceElementIters):
        hf.Workflow.from_template(wkt, path=tmp_path)


def test_input_source_specify_element_iters_and_where(null_config, tmp_path: Path):
    """Test the where argument further filters the element_iters argument."""
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[{"a": 1}, {"a": 2}, {"a": 3}],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0,
                    task_source_type="input",
                    element_iters=[0, 2],
                    where=hf.Rule(path="inputs.p1.a", condition={"value.equal_to": 3}),
                )
            ]
        },
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert len(wk.tasks[1].elements) == 1
    assert [i.value["a"] for i in wk.tasks[1].inputs.p1] == [3]


def test_element_iters_order_with_allow_non_coincident_task_sources_False(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[11, 12, 13],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[2, 0, 1]
                )
            ],
        },
        allow_non_coincident_task_sources=False,
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 3
    assert [i.value for i in wk.tasks[1].inputs.p1] == [13, 11, 12]


def test_element_iters_order_with_allow_non_coincident_task_sources_True(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[11, 12, 13],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[2, 0, 1]
                )
            ],
        },
        allow_non_coincident_task_sources=True,
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 3
    assert [i.value for i in wk.tasks[1].inputs.p1] == [13, 11, 12]


def test_element_iters_order_with_allow_non_coincident_task_sources_True_multiple_sources(
    null_config, tmp_path: Path
):
    """Test no-reordering of specified element iterations of sources from the same task."""
    (s1,) = make_schemas(({"p1": None, "p2": None}, ("p3",), "t1"))

    t1 = hf.Task(
        schema=s1,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[11, 12, 13],
            ),
            hf.ValueSequence(
                path="inputs.p2",
                values=[21, 22, 23],
            ),
        ],
    )
    t2 = hf.Task(
        schema=s1,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 1]
                )
            ],
            "p2": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[1, 0]
                )
            ],
        },
        allow_non_coincident_task_sources=True,
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 2
    assert [i.value for i in wk.tasks[1].inputs.p1] == [11, 12]
    assert [i.value for i in wk.tasks[1].inputs.p2] == [22, 21]


def test_element_iters_order_with_allow_non_coincident_task_sources_False_multiple_sources(
    null_config, tmp_path: Path
):
    """Test reordering of specified element iterations of sources from the same task."""
    (s1,) = make_schemas(({"p1": None, "p2": None}, ("p3",), "t1"))

    t1 = hf.Task(
        schema=s1,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[11, 12, 13],
            ),
            hf.ValueSequence(
                path="inputs.p2",
                values=[21, 22, 23],
            ),
        ],
    )
    t2 = hf.Task(
        schema=s1,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 1]
                )
            ],
            "p2": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[1, 0]
                )
            ],
        },
        allow_non_coincident_task_sources=False,
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 2
    assert [i.value for i in wk.tasks[1].inputs.p1] == [11, 12]
    assert [i.value for i in wk.tasks[1].inputs.p2] == [21, 22]


def test_not_allow_non_coincident_task_sources(null_config, tmp_path: Path):
    """Test only one coincident element from the two input sources"""
    (s1,) = make_schemas(({"p1": None, "p2": None}, ("p3",), "t1"))
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": 1},
        sequences=[
            hf.ValueSequence(path="inputs.p2", values=[21, 22, 23]),
        ],
    )
    t2 = hf.Task(
        schema=s1,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 1]
                )
            ],
            "p2": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[1, 2]
                )
            ],
        },
        allow_non_coincident_task_sources=False,
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 1
    assert [i.value for i in wk.tasks[1].inputs.p2] == [22]


def test_allow_non_coincident_task_sources(null_config, tmp_path: Path):
    """Test can combine inputs from non-coincident element iterations of the same task."""
    (s1,) = make_schemas(({"p1": None, "p2": None}, ("p3",), "t1"))
    t1 = hf.Task(
        schema=s1,
        sequences=[
            hf.ValueSequence(
                path="inputs.p1",
                values=[11, 12, 13],
            ),
            hf.ValueSequence(
                path="inputs.p2",
                values=[21, 22, 23],
            ),
        ],
    )
    t2 = hf.Task(
        schema=s1,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 1]
                )
            ],
            "p2": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[1, 2]
                )
            ],
        },
        allow_non_coincident_task_sources=True,
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 2
    assert [i.value for i in wk.tasks[1].inputs.p1] == [11, 12]
    assert [i.value for i in wk.tasks[1].inputs.p2] == [22, 23]


def test_input_source_task_input_from_multiple_element_sets_with_param_sequence(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        element_sets=[
            hf.ElementSet(inputs={"p1": {"a": 1}}),
            hf.ElementSet(
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1",
                        values=[{"a": 2}, {"a": 3}],
                    ),
                ],
            ),
        ],
    )
    t2 = hf.Task(schema=hf.task_schemas.test_t1_ps)
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert len(wk.tasks[1].elements) == 3
    assert [i.value["a"] for i in wk.tasks[1].inputs.p1] == [1, 2, 3]


def test_raise_no_coincident_input_sources(null_config, tmp_path: Path):
    (s1,) = make_schemas(({"p1": None, "p2": None}, ("p3",), "t1"))
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": 100},
        sequences=[
            hf.ValueSequence.from_range(path="inputs.p2", start=0, stop=4),
        ],
    )
    t2 = hf.Task(
        schema=s1,
        allow_non_coincident_task_sources=False,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 1]
                )
            ],
            "p2": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[2, 3]
                )
            ],
        },
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    with pytest.raises(NoCoincidentInputSources):
        hf.Workflow.from_template(wkt, path=tmp_path)


def test_input_source_task_input_from_multiple_element_sets_with_sub_param_sequence(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        element_sets=[
            hf.ElementSet(inputs={"p1": {"a": 1}}),
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.a",
                        values=[2, 3],
                    ),
                ],
            ),
        ],
    )
    t2 = hf.Task(schema=hf.task_schemas.test_t1_ps)
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert len(wk.tasks[1].elements) == 3
    assert [i.value["a"] for i in wk.tasks[1].inputs.p1] == [1, 2, 3]


def test_input_source_task_input_from_multiple_element_sets_with_sub_param_sequence_manual_sources_root_param(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        element_sets=[
            hf.ElementSet(inputs={"p1": {"a": 1}}),
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.a",
                        values=[2, 3],
                    ),
                ],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={
            "p1": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[0, 1]
                )
            ]
        },
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    assert len(wk.tasks[1].elements) == 2
    assert [i.value["a"] for i in wk.tasks[1].inputs.p1] == [1, 2]


def test_input_source_inputs_from_multiple_element_sets_with_sub_parameter_sequences_complex(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        element_sets=[
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.a",
                        values=[2],
                    ),
                ],
            ),
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.c",
                        values=[2, 3],
                    ),
                ],
            ),
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.b",
                        values=[22, 33],
                    ),
                    hf.ValueSequence(
                        path="inputs.p1.a",
                        values=[4, 5],
                    ),
                ],
            ),
        ],
    )
    t2 = hf.Task(schema=hf.task_schemas.test_t1_ps)
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 5
    assert [i.value for i in wk.tasks[1].inputs.p1] == [
        {"a": 2},
        {"a": 1, "c": 2},
        {"a": 1, "c": 3},
        {"a": 4, "b": 22},
        {"a": 5, "b": 33},
    ]


def test_input_source_inputs_from_multiple_element_sets_with_sub_parameter_sequences_complex_reordered_iters(
    null_config, tmp_path: Path
):
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        element_sets=[
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.a",
                        values=[2],
                    ),
                ],
            ),
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.c",
                        values=[2, 3],
                    ),
                ],
            ),
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.b",
                        values=[22, 33],
                    ),
                    hf.ValueSequence(
                        path="inputs.p1.a",
                        values=[4, 5],
                    ),
                ],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={
            # reordered p1.c elem iters:
            "p1.c": [
                hf.InputSource.task(
                    task_ref=0, task_source_type="input", element_iters=[2, 1]
                )
            ]
        },
        allow_non_coincident_task_sources=True,  # to maintain custom ordering
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 5
    assert [i.value for i in wk.tasks[1].inputs.p1] == [
        {"a": 2},
        {"a": 1, "c": 3},
        {"a": 1, "c": 2},
        {"a": 4, "b": 22},
        {"a": 5, "b": 33},
    ]


def test_input_source_inputs_from_multiple_element_sets_with_sub_parameter_sequences_mixed_padding(
    null_config, tmp_path: Path
):

    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        element_sets=[
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
            ),
            hf.ElementSet(
                inputs={"p1": {"a": 1}},
                nesting_order={"inputs.p1.a": 0, "inputs.p1.b": 1},
                sequences=[
                    hf.ValueSequence(
                        path="inputs.p1.a",
                        values=[4, 5],
                    ),
                    hf.ValueSequence(
                        path="inputs.p1.b",
                        values=[22],
                    ),
                ],
            ),
        ],
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        # `p1.b` has a different nesting order to the root param `p1`, so it will not be
        # "padded" to have the same multiplicity as `p1`/`p1.a`. With a higher nesting
        # order, it will be "applied" to all other elements, meaning we'll gain a value
        # for `p1.b` for all elements (including from the first element set, which didn't
        # have a value for `p1.b`):
        nesting_order={
            "inputs.p1": 0,
            "inputs.p1.a": 0,
            "inputs.p1.b": 1,
        },
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    wk = hf.Workflow.from_template(wkt, path=tmp_path)

    assert len(wk.tasks[1].elements) == 4
    assert [i.value for i in wk.tasks[1].inputs.p1] == [
        {"a": 1, "b": 22},
        {"a": 1, "b": 22},
        {"a": 5, "b": 22},
        {"a": 5, "b": 22},
    ]
