from __future__ import annotations
import os
from textwrap import dedent
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
    from typing import Any
    from hpcflow.sdk.core.parameters import Parameter
    from hpcflow.sdk.core.types import RuleArgs


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("multiplicity", [1, 3])
@pytest.mark.parametrize(
    "mode",
    [
        "plain",
        "filtered",
        "grouped",
        "filtered_grouped",
        "labelled",
        "nested",
        "looped_grouped",
    ],
)
def test_output_member_input_sources(
    tmp_path: Path, store: str, multiplicity: int, mode: str
) -> None:
    output = hf.SchemaOutput("p2", multiplicity=multiplicity)
    upstream = hf.TaskSchema(
        "producer",
        inputs=[hf.SchemaInput("p1")],
        outputs=[output],
        actions=[
            hf.Action(
                commands=[hf.Command("echo <<parameter:p1>>", stdout="<<parameter:p2>>")]
            )
        ],
    )
    input_name = "p2[chosen]" if mode == "labelled" else "p2"
    downstream = hf.TaskSchema(
        "consumer",
        inputs=[
            hf.SchemaInput(
                "p2",
                multiple=mode == "labelled",
                labels={"chosen": {}} if mode == "labelled" else None,
                group="members" if "grouped" in mode else None,
            ),
            *([hf.SchemaInput("p3")] if mode == "nested" else []),
        ],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        f"echo <<parameter:{input_name}>>"
                        + (" <<parameter:p3>>" if mode == "nested" else "")
                    )
                ]
            )
        ],
    )
    source = hf.InputSource.task(
        task_ref=0,
        task_source_type="output",
        element_iters=[1] if "filtered" in mode else None,
    )
    wk = hf.Workflow.from_template_data(
        template_name="output_members",
        tasks=[
            hf.Task(
                upstream,
                element_sets=(
                    [
                        hf.ElementSet(inputs={"p1": 0}),
                        hf.ElementSet(
                            inputs={"p1": 1}, groups=[hf.ElementGroup("members")]
                        ),
                    ]
                    if mode == "filtered_grouped"
                    else [
                        hf.ElementSet(
                            sequences=[hf.ValueSequence("inputs.p1", [0, 1])],
                            groups=(
                                [hf.ElementGroup("members")] if "grouped" in mode else []
                            ),
                        )
                    ]
                ),
            ),
            hf.Task(
                downstream,
                input_sources={input_name: [source]},
                sequences=(
                    [hf.ValueSequence("inputs.p3", [100, 200])]
                    if mode == "nested"
                    else []
                ),
                nesting_order=(
                    {"inputs.p2": 0, "inputs.p3": 1} if mode == "nested" else {}
                ),
            ),
        ],
        path=tmp_path,
        store=store,
        loops=(
            [hf.Loop(tasks=[0, 1], num_iterations=2)] if mode == "looped_grouped" else []
        ),
    )
    expected: list[Any] = (
        [10, 11, 12, 20, 21, 22] if multiplicity == 3 else [[10, 11, 12], [20, 21, 22]]
    )
    if "filtered" in mode:
        expected = expected[multiplicity:]
    if "grouped" in mode:
        expected = [expected]
    if mode == "nested":
        expected = [value for value in expected for _ in range(2)]
    assert wk.tasks[1].num_elements == len(expected)
    member_ids = [
        element.get_data_idx()[f"inputs.{input_name}"] for element in wk.tasks[1].elements
    ]
    if "grouped" not in mode:
        for idx in member_ids:
            assert isinstance(idx, int)
            assert not wk.is_parameter_set(idx)
    wk.add_submission()
    for element, value in zip(wk.tasks[0].elements, ([10, 11, 12], [20, 21, 22])):
        output_id = element.get_data_idx()["outputs.p2"]
        wk.set_parameter_value(output_id, value, commit=True)

    for workflow in (wk, hf.Workflow(wk.path)):
        assert (
            workflow.tasks[0].template.all_schema_outputs[0].multiplicity == multiplicity
        )
        actual = [
            element.get(f"inputs.{input_name}") for element in workflow.tasks[1].elements
        ]
        assert actual == expected
        all_sources = workflow.get_all_parameter_sources()
        for element in workflow.tasks[1].elements:
            member_source = element.get_parameter_sources()["inputs." + input_name]
            assert member_source
            indices = element.get_data_idx()[f"inputs.{input_name}"]
            for idx in indices if isinstance(indices, list) else [indices]:
                assert workflow.is_parameter_set(idx)
                assert all_sources[idx] == workflow.get_parameter_source(idx)


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("value", [42, [1], [1, 2, 3], [7, 9]])
def test_output_multiplicity_from_yaml(
    tmp_path: Path, store: str, value: int | list[int]
) -> None:
    wk = hf.Workflow.from_YAML_string(
        YAML_str=dedent("""
            name: output_members_yaml
            template_components:
              task_schemas:
                - objective: producer
                  outputs:
                    - parameter: p2
                      multiplicity: 2
                  actions:
                    - commands:
                        - command: echo members
                          stdout: <<parameter:p2>>
                - objective: consumer
                  inputs:
                    - parameter: p2
                  actions:
                    - commands:
                        - command: echo <<parameter:p2>>
            tasks:
              - schema: producer
              - schema: consumer
            """),
        path=tmp_path,
        store=store,
    )
    assert wk.tasks[1].num_elements == 2
    wk.add_submission()
    output_id = wk.tasks[0].elements[0].get_data_idx()["outputs.p2"]
    wk.set_parameter_value(output_id, value, commit=True)
    if value != [7, 9]:
        with pytest.raises(ValueError, match="must contain 2 members"):
            _ = wk.tasks[1].elements[0].get("inputs.p2")
    else:
        assert [el.get("inputs.p2") for el in hf.Workflow(wk.path).tasks[1].elements] == [
            7,
            9,
        ]


@pytest.mark.parametrize("legacy_array", [False, True])
@pytest.mark.parametrize("as_tuple", [False, True])
def test_output_member_array_values(
    tmp_path: Path, legacy_array: bool, as_tuple: bool
) -> None:
    producer, consumer = make_schemas(
        ({}, ("p2",), "producer"), ({"p2": None}, (), "consumer")
    )
    producer.outputs[0].multiplicity = 2
    wk = hf.Workflow.from_template_data(
        template_name="output_array_members",
        tasks=[
            hf.Task(
                producer,
                disable_array_outputs=["p2"] if legacy_array else [],
            ),
            hf.Task(consumer),
        ],
        path=tmp_path,
        store="zarr",
    )
    wk.add_submission()
    output_id = wk.tasks[0].elements[0].get_data_idx()["outputs.p2"]
    array = np.array([[10, 11, 12], [20, 21, 22]])
    wk.set_parameter_value(
        output_id, tuple(array.tolist()) if as_tuple else array, commit=True
    )
    restored = hf.Workflow(wk.path)
    for idx, element in enumerate(restored.tasks[1].elements):
        np.testing.assert_array_equal(element.get("inputs.p2"), array[idx])


@pytest.mark.integration
@pytest.mark.skipif(os.name != "posix", reason="Uses POSIX shell arithmetic")
@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("loop_tasks", [None, [0], [0, 1]])
def test_output_members_execution(
    tmp_path: Path, store: str, loop_tasks: list[int] | None
) -> None:
    producer = hf.TaskSchema(
        "producer",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2", multiplicity=2)],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo <<parameter:p1>>,$((<<parameter:p1>> + 1))",
                        stdout='<<list[item_type=int, delim=","](parameter:p2)>>',
                    )
                ]
            )
        ],
    )
    consumer = hf.TaskSchema(
        "consumer",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $((<<parameter:p2>> * 10))",
                        stdout="<<int(parameter:p3)>>",
                    )
                ]
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="output_members_execution",
        tasks=[
            hf.Task(producer, sequences=[hf.ValueSequence("inputs.p1", [10, 20])]),
            hf.Task(consumer),
        ],
        path=tmp_path,
        store=store,
        loops=[hf.Loop(tasks=loop_tasks, num_iterations=2)] if loop_tasks else [],
    )
    assert wk.tasks[1].num_elements == 4
    for idx, element in enumerate(wk.tasks[1].elements):
        source_run = wk.tasks[0].elements[idx // 2].latest_iteration.action_runs[0]
        assert element.get_EAR_dependencies() == {source_run.id_}
    wk.submit(wait=True, add_to_known=False, status=False)
    restored = hf.Workflow(wk.path)
    assert [el.get("inputs.p2") for el in restored.tasks[1].elements] == [10, 11, 20, 21]
    assert [el.get("outputs.p3") for el in restored.tasks[1].elements] == [
        100,
        110,
        200,
        210,
    ]


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


@pytest.mark.skip(reason="Import not yet implemented.")
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


@pytest.mark.skip(reason="Need to add e.g. parameters of the workflow to the app data.")
def test_specified_sourceable_elements_subset(
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
def test_input_source_where(tmp_path: Path, store: str):
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
    tmp_path: Path, store: str
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
    tmp_path: Path, store: str
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
    tmp_path: Path,
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
    tmp_path: Path,
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
    tmp_path: Path,
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


def test_raise_unavailable_input_source(tmp_path: Path):
    t1 = hf.Task(schema=hf.task_schemas.test_t1_ps, inputs={"p1": 1})
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        input_sources={"p1": [hf.InputSource.local()]},
    )
    wkt = hf.WorkflowTemplate(name="test", tasks=[t1, t2])
    with pytest.raises(UnavailableInputSource):
        hf.Workflow.from_template(wkt, path=tmp_path)


def test_input_source_specify_element_iters(tmp_path: Path):
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


def test_input_source_raise_on_inapplicable_specified_element_iters(tmp_path: Path):
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


def test_input_source_specify_element_iters_and_where(tmp_path: Path):
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


def test_element_iters_order_with_allow_non_coincident_task_sources_False(tmp_path: Path):
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


def test_element_iters_order_with_allow_non_coincident_task_sources_True(tmp_path: Path):
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
    tmp_path: Path,
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
    tmp_path: Path,
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


def test_not_allow_non_coincident_task_sources(tmp_path: Path):
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


def test_allow_non_coincident_task_sources(tmp_path: Path):
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
    tmp_path: Path,
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


def test_raise_no_coincident_input_sources(tmp_path: Path):
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
    tmp_path: Path,
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
    tmp_path: Path,
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
    tmp_path: Path,
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
    tmp_path: Path,
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
    tmp_path: Path,
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


def test_input_source_task_ref_equivalence(tmp_path):
    yml = dedent("""\
    name: test
    template_components:
      task_schemas:
        - objective: t1
          inputs:
            - parameter: p1
    tasks:
      - schema: t1
        inputs:
          p1: 100 # all subsequent tasks will source from this input
    
      - schema: t1 # t1_2
        input_sources: # single source dict; by task insert ID
          p1: 
            source_type: task
            task_source_type: input
            task_ref: 0

      - schema: t1 # t1_3
        input_sources: # as a list of dicts; by task insert ID
          p1: 
            - source_type: task
              task_source_type: input
              task_ref: 0
    
      - schema: t1 # t1_4
        input_sources: # as a single source dict; by task unique name
          p1: 
            source_type: task
            task_source_type: input
            task_ref: t1_1

      - schema: t1 # t1_5
        input_sources: # as a list of dicts; by task unique name
          p1: 
            - source_type: task
              task_source_type: input
              task_ref: t1_1
    
      - schema: t1 # t1_6
        input_sources: # single source string; by task insert ID
          p1: task.0.input

      - schema: t1 # t1_7
        input_sources: # as a list of strings; by task insert ID
          p1: 
            - task.0.input

      - schema: t1 # t1_8
        input_sources: # single source string; by task unique name
          p1: task.t1_1.input

      - schema: t1 # t1_9
        input_sources: # as a list of strings; by task unique name
          p1: 
            - task.t1_1.input
        
    """)
    wk = hf.Workflow.from_YAML_string(YAML_str=yml, path=tmp_path)

    all_sources = (task.elements[0].input_sources["inputs.p1"] for task in wk.tasks[1:])
    all_task_refs = (src.task_ref for src in all_sources)
    assert all(task_ref == 0 for task_ref in all_task_refs)


def test_inp_src_task_output_precedence(tmp_path):
    # test a task output source takes precedence over a task input source, even if the
    # task input source is from a closer task.

    s1, s2 = make_schemas(
        ({"p0": None}, ("p1",), "t1"),
        ({"p1": None, "p2": None}, ("p3",), "t2"),
    )
    s3 = hf.TaskSchema(
        "t3",
        inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p3", group="my_group")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<sum(parameter:p3)>> + <<parameter:p1>> ))",
                        stdout="<<parameter:p4>>",
                    )
                ]
            ),
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_inp_src",
        tasks=[
            hf.Task(s1, inputs={"p0": 1}),
            hf.Task(
                s2,
                sequences=[hf.ValueSequence("inputs.p2", [0, 1, 2])],
                groups=[hf.ElementGroup(name="my_group")],
            ),
            hf.Task(s3),
        ],
        path=tmp_path,
    )

    task = wk.tasks.t3
    task_template = task.template
    element_set = task_template.element_sets[0]
    all_stats = task_template.get_input_statuses(element_set)
    inp_sources = task_template.get_available_task_input_sources(
        element_set=element_set,
        input_statuses=all_stats,
        source_tasks=list(task.upstream_tasks),
    )
    assert inp_sources["p1"] == [
        hf.InputSource.task(task_ref=0, task_source_type="output", element_iters=[0]),
        hf.InputSource.task(
            task_ref=1, task_source_type="input", element_iters=[1, 2, 3]
        ),
    ]
    # p1 source from t1 output should take precedence, rather than t2 input (t2 input has
    # multiple elements, so interferes with grouping on the other parameter, p3)


def test_task_type_sources_output_input_swapped_on_local_inputs_defined(tmp_path):

    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p3",), "t2"),
        ({"p2": None}, ("p4",), "t3"),
    )

    # t2's input sources for p2 do not include any local sources, so the task-output source
    # from t1 should be preferred.

    wk = hf.Workflow.from_template_data(
        template_name="test_inp_src",
        tasks=[
            hf.Task(s1, inputs={"p1": 100}),
            hf.Task(s2),
            hf.Task(s3),
        ],
        path=tmp_path,
    )

    task = wk.tasks.t3
    task_template = task.template
    element_set = task_template.element_sets[0]
    all_stats = task_template.get_input_statuses(element_set)
    inp_sources = task_template.get_available_task_input_sources(
        element_set=element_set,
        input_statuses=all_stats,
        source_tasks=list(task.upstream_tasks),
    )
    assert inp_sources["p2"] == [
        hf.InputSource.task(task_ref=0, task_source_type="output", element_iters=[0]),
        hf.InputSource.task(task_ref=1, task_source_type="input", element_iters=[1]),
        hf.InputSource.default(),
    ]


def test_task_type_sources_output_input_not_swapped_on_no_local_inputs_defined(tmp_path):
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p3",), "t2"),
        ({"p2": None}, ("p4",), "t3"),
    )

    # now include a local source in t2, which should switch p2's input source precedence in t3
    # such that the task-input source from t2 is preferred over the task-output source from
    # t1

    wk = hf.Workflow.from_template_data(
        template_name="test_inp_src",
        tasks=[
            hf.Task(s1, inputs={"p1": 100}),
            hf.Task(s2, inputs={"p2": 200}),
            hf.Task(s3),
        ],
        path=tmp_path,
    )

    task = wk.tasks.t3
    task_template = task.template
    element_set = task_template.element_sets[0]
    all_stats = task_template.get_input_statuses(element_set)
    inp_sources = task_template.get_available_task_input_sources(
        element_set=element_set,
        input_statuses=all_stats,
        source_tasks=list(task.upstream_tasks),
    )
    assert inp_sources["p2"] == [
        hf.InputSource.task(task_ref=1, task_source_type="input", element_iters=[1]),
        hf.InputSource.task(task_ref=0, task_source_type="output", element_iters=[0]),
        hf.InputSource.default(),
    ]
