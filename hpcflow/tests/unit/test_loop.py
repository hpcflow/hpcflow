from __future__ import annotations
from pathlib import Path
import pytest

from valida.conditions import Value  # type: ignore

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import LoopAlreadyExistsError, LoopTaskSubsetError
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.core.test_utils import P1_parameter_cls, make_schemas, make_workflow


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_loop_tasks_obj_insert_ID_equivalence(tmp_path: Path, store: str):
    wk_1 = make_workflow(
        schemas_spec=[({"p1": None}, ("p1",), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    lp_0 = hf.Loop(tasks=[wk_1.tasks.t1], num_iterations=2)
    lp_1 = hf.Loop(tasks=[0], num_iterations=2)
    assert lp_0.task_insert_IDs == lp_1.task_insert_IDs


def test_raise_on_add_loop_same_name(tmp_path: Path):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, ("p1",), "t1"), ({"p2": None}, ("p2",), "t2")],
        local_inputs={0: ("p1",), 1: ("p2",)},
        path=tmp_path,
        store="json",
    )
    lp_0 = hf.Loop(name="my_loop", tasks=[0], num_iterations=2)
    lp_1 = hf.Loop(name="my_loop", tasks=[1], num_iterations=2)

    wk.add_loop(lp_0)
    with pytest.raises(LoopAlreadyExistsError):
        wk.add_loop(lp_1)


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_single_task_single_element_single_parameter_three_iters(
    tmp_path: Path, store: str
):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, ("p1",), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[wk.tasks.t1], num_iterations=3))
    iter_0, iter_1, iter_2 = wk.tasks.t1.elements[0].iterations

    p1_idx_i0_out = iter_0.get_data_idx()["outputs.p1"]
    p1_idx_i1_in = iter_1.get_data_idx()["inputs.p1"]
    p1_idx_i1_out = iter_1.get_data_idx()["outputs.p1"]
    p1_idx_i2_in = iter_2.get_data_idx()["inputs.p1"]

    assert p1_idx_i0_out == p1_idx_i1_in and p1_idx_i1_out == p1_idx_i2_in


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_EARs_initialised_single_task_single_element_single_parameter_three_iters(
    tmp_path: Path, store: str
):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, ("p1",), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[wk.tasks.t1], num_iterations=3))
    iter_0, iter_1, iter_2 = wk.tasks.t1.elements[0].iterations
    assert iter_0.EARs_initialised and iter_1.EARs_initialised and iter_2.EARs_initialised


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_single_task_multi_element_single_parameter_three_iters(
    tmp_path: Path, store: str
):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, ("p1",), "t1")],
        local_sequences={0: [("inputs.p1", 2, 0)]},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[wk.tasks.t1], num_iterations=3))
    e0_iter_0, e0_iter_1, e0_iter_2 = wk.tasks.t1.elements[0].iterations
    e1_iter_0, e1_iter_1, e1_iter_2 = wk.tasks.t1.elements[1].iterations

    e0_p1_idx_i0_out = e0_iter_0.get_data_idx()["outputs.p1"]
    e0_p1_idx_i1_in = e0_iter_1.get_data_idx()["inputs.p1"]
    e0_p1_idx_i1_out = e0_iter_1.get_data_idx()["outputs.p1"]
    e0_p1_idx_i2_in = e0_iter_2.get_data_idx()["inputs.p1"]

    e1_p1_idx_i0_out = e1_iter_0.get_data_idx()["outputs.p1"]
    e1_p1_idx_i1_in = e1_iter_1.get_data_idx()["inputs.p1"]
    e1_p1_idx_i1_out = e1_iter_1.get_data_idx()["outputs.p1"]
    e1_p1_idx_i2_in = e1_iter_2.get_data_idx()["inputs.p1"]

    assert (
        e0_p1_idx_i0_out == e0_p1_idx_i1_in
        and e0_p1_idx_i1_out == e0_p1_idx_i2_in
        and e1_p1_idx_i0_out == e1_p1_idx_i1_in
        and e1_p1_idx_i1_out == e1_p1_idx_i2_in
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_multi_task_single_element_single_parameter_two_iters(
    tmp_path: Path, store: str
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p1",), "t1"),
            ({"p1": None}, ("p1",), "t2"),
            ({"p1": None}, ("p1",), "t3"),
        ],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[0, 1, 2], num_iterations=2))
    t1_iter_0, t1_iter_1 = wk.tasks.t1.elements[0].iterations
    t2_iter_0, t2_iter_1 = wk.tasks.t2.elements[0].iterations
    t3_iter_0, t3_iter_1 = wk.tasks.t3.elements[0].iterations

    in_key = "inputs.p1"
    out_key = "outputs.p1"

    t1_i0_p1_idx_out = t1_iter_0.get_data_idx()[out_key]
    t2_i0_p1_idx_in = t2_iter_0.get_data_idx()[in_key]
    t2_i0_p1_idx_out = t2_iter_0.get_data_idx()[out_key]
    t3_i0_p1_idx_in = t3_iter_0.get_data_idx()[in_key]
    t3_i0_p1_idx_out = t3_iter_0.get_data_idx()[out_key]

    t1_i1_p1_idx_in = t1_iter_1.get_data_idx()[in_key]
    t1_i1_p1_idx_out = t1_iter_1.get_data_idx()[out_key]
    t2_i1_p1_idx_in = t2_iter_1.get_data_idx()[in_key]
    t2_i1_p1_idx_out = t2_iter_1.get_data_idx()[out_key]
    t3_i1_p1_idx_in = t3_iter_1.get_data_idx()[in_key]

    assert (
        t1_i0_p1_idx_out == t2_i0_p1_idx_in
        and t2_i0_p1_idx_out == t3_i0_p1_idx_in
        and t3_i0_p1_idx_out == t1_i1_p1_idx_in
        and t1_i1_p1_idx_out == t2_i1_p1_idx_in
        and t2_i1_p1_idx_out == t3_i1_p1_idx_in
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_data_idx_single_task_single_element_single_parameter_three_iters_non_iterable_param(
    tmp_path: Path, store: str
):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, ("p1",), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(
        hf.Loop(tasks=[wk.tasks.t1], num_iterations=3, non_iterable_parameters=["p1"])
    )
    iter_0, iter_1, iter_2 = wk.tasks.t1.elements[0].iterations

    p1_idx_i0_out = iter_0.get_data_idx()["outputs.p1"]
    p1_idx_i1_in = iter_1.get_data_idx()["inputs.p1"]
    p1_idx_i1_out = iter_1.get_data_idx()["outputs.p1"]
    p1_idx_i2_in = iter_2.get_data_idx()["inputs.p1"]

    assert p1_idx_i0_out != p1_idx_i1_in and p1_idx_i1_out != p1_idx_i2_in


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_iterable_parameters(tmp_path: Path, store: str):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None, "p2": None}, ("p1", "p2"), "t1"),
            ({"p1": None}, ("p1",), "t2"),
            ({"p1": None, "p2": None}, ("p1", "p2"), "t3"),
        ],
        local_inputs={0: ("p1", "p2"), 1: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[0, 1, 2], num_iterations=2))
    assert dict(sorted(wk.loops[0].iterable_parameters.items(), key=lambda x: x[0])) == {
        "p1": {"input_task": 0, "output_tasks": [0, 1, 2]},
        "p2": {"input_task": 0, "output_tasks": [0, 2]},
    }


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_wk_loop_input_sources_including_local_single_element_two_iters(
    tmp_path: Path, store: str
):
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None, "p2": None}, ("p1", "p2"), "t1"),
            ({"p1": None}, ("p1",), "t2"),
            ({"p1": None, "p2": None}, ("p1", "p2"), "t3"),
        ],
        local_inputs={0: ("p1", "p2"), 1: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(tasks=[0, 1, 2], num_iterations=2))

    t2_iter_0 = wk.tasks.t2.elements[0].iterations[0]
    t3_iter_0 = wk.tasks.t3.elements[0].iterations[0]
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1]
    t2_iter_1 = wk.tasks.t2.elements[0].iterations[1]

    t3_p1_i0_out = t3_iter_0.get_data_idx()["outputs.p1"]
    t3_p2_i0_out = t3_iter_0.get_data_idx()["outputs.p2"]

    t1_p1_i1_in = t1_iter_1.get_data_idx()["inputs.p1"]
    t1_p2_i1_in = t1_iter_1.get_data_idx()["inputs.p2"]

    # local input defined in task 2 is not an input task of the iterative parameter p1,
    # so it is sourced in all iterations from the original local input:
    t2_p1_i0_in = t2_iter_0.get_data_idx()["inputs.p1"]
    t2_p1_i1_in = t2_iter_1.get_data_idx()["inputs.p1"]

    assert (
        t3_p1_i0_out == t1_p1_i1_in
        and t3_p2_i0_out == t1_p2_i1_in
        and t2_p1_i0_in == t2_p1_i1_in
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_get_iteration_task_pathway_single_task_single_element_three_iters(
    tmp_path: Path, store: str
):
    wk = make_workflow(
        schemas_spec=[({"p1": None}, ("p1",), "t1")],
        local_inputs={0: ("p1",)},
        path=tmp_path,
        store=store,
    )
    wk.add_loop(hf.Loop(name="loop_0", tasks=[wk.tasks.t1], num_iterations=3))

    assert wk.get_iteration_task_pathway() == [
        (0, {"loop_0": 0}),
        (0, {"loop_0": 1}),
        (0, {"loop_0": 2}),
    ]


def test_get_iteration_task_pathway_nested_loops_multi_iter(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="inner_loop", tasks=[2], num_iterations=2),
            hf.Loop(name="outer_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    assert wk.get_iteration_task_pathway() == [
        (0, {}),
        (1, {"outer_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 1}),
        (1, {"outer_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 0}),
        (2, {"outer_loop": 1, "inner_loop": 1}),
    ]


@pytest.mark.skip(
    reason="second set of asserts fail; need to re-source inputs on adding iterations."
)
def test_get_iteration_task_pathway_nested_loops_multi_iter_jagged(
    null_config, tmp_path: Path
):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="inner_loop", tasks=[2], num_iterations=2),
            hf.Loop(name="outer_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    wk.loops.inner_loop.add_iteration(parent_loop_indices={"outer_loop": 1})
    wk.loops.inner_loop.add_iteration(parent_loop_indices={"outer_loop": 1})
    assert wk.get_iteration_task_pathway() == [
        (0, {}),
        (1, {"outer_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 1}),
        (1, {"outer_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 0}),
        (2, {"outer_loop": 1, "inner_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 2}),
        (2, {"outer_loop": 1, "inner_loop": 3}),
        (3, {}),
    ]
    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)
    assert pathway[1][2][0]["inputs.p1"] == pathway[0][2][0]["outputs.p1"]
    assert pathway[2][2][0]["inputs.p1"] == pathway[1][2][0]["outputs.p1"]
    assert pathway[3][2][0]["inputs.p1"] == pathway[2][2][0]["outputs.p1"]
    assert pathway[4][2][0]["inputs.p1"] == pathway[3][2][0]["outputs.p1"]
    assert pathway[5][2][0]["inputs.p1"] == pathway[4][2][0]["outputs.p1"]
    assert pathway[6][2][0]["inputs.p1"] == pathway[5][2][0]["outputs.p1"]
    assert pathway[7][2][0]["inputs.p1"] == pathway[6][2][0]["outputs.p1"]
    assert pathway[8][2][0]["inputs.p1"] == pathway[7][2][0]["outputs.p1"]

    # FAILS currently:
    assert pathway[9][2][0]["inputs.p1"] == pathway[8][2][0]["outputs.p1"]


def test_get_iteration_task_pathway_nested_loops_multi_iter_add_outer_iter(
    null_config, tmp_path: Path
):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="inner_loop", tasks=[2], num_iterations=2),
            hf.Loop(name="outer_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    wk.loops.outer_loop.add_iteration()
    assert wk.get_iteration_task_pathway() == [
        (0, {}),
        (1, {"outer_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 0}),
        (2, {"outer_loop": 0, "inner_loop": 1}),
        (1, {"outer_loop": 1}),
        (2, {"outer_loop": 1, "inner_loop": 0}),
        (2, {"outer_loop": 1, "inner_loop": 1}),
        (1, {"outer_loop": 2}),
        (2, {"outer_loop": 2, "inner_loop": 0}),
        (2, {"outer_loop": 2, "inner_loop": 1}),
    ]


def test_get_iteration_task_pathway_unconnected_loops(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
        ],
        loops=[
            hf.Loop(name="loop_A", tasks=[0, 1], num_iterations=2),
            hf.Loop(name="loop_B", tasks=[2, 3], num_iterations=2),
        ],
    )
    assert wk.get_iteration_task_pathway() == [
        (0, {"loop_A": 0}),
        (1, {"loop_A": 0}),
        (0, {"loop_A": 1}),
        (1, {"loop_A": 1}),
        (2, {"loop_B": 0}),
        (3, {"loop_B": 0}),
        (2, {"loop_B": 1}),
        (3, {"loop_B": 1}),
    ]

    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)
    assert pathway[1][2][0]["inputs.p1"] == pathway[0][2][0]["outputs.p1"]
    assert pathway[2][2][0]["inputs.p1"] == pathway[1][2][0]["outputs.p1"]
    assert pathway[3][2][0]["inputs.p1"] == pathway[2][2][0]["outputs.p1"]
    assert pathway[5][2][0]["inputs.p1"] == pathway[4][2][0]["outputs.p1"]
    assert pathway[6][2][0]["inputs.p1"] == pathway[5][2][0]["outputs.p1"]
    assert pathway[7][2][0]["inputs.p1"] == pathway[6][2][0]["outputs.p1"]
    assert pathway[4][2][0]["inputs.p1"] == pathway[3][2][0]["outputs.p1"]


def test_wk_loop_input_sources_including_non_iteration_task_source(
    null_config, tmp_path: Path
):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 100))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2"), hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p2>> + <<parameter:p3>>))",
                        stdout="<<int(parameter:p4)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("p3"), hf.SchemaInput("p4")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p3>> + <<parameter:p4>>))",
                        stdout="<<int(parameter:p3)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts2, inputs={"p3": 301}),
            hf.Task(schema=ts3),
        ],
    )
    wk.add_loop(hf.Loop(tasks=[1, 2], num_iterations=2))
    t1 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t2_iter_0 = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t3_iter_0 = wk.tasks.t3.elements[0].iterations[0].get_data_idx()
    t2_iter_1 = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t3_iter_1 = wk.tasks.t3.elements[0].iterations[1].get_data_idx()

    assert t2_iter_0["inputs.p2"] == t2_iter_1["inputs.p2"] == t1["outputs.p2"]
    assert t3_iter_0["inputs.p3"] == t2_iter_0["inputs.p3"]
    assert t3_iter_0["inputs.p4"] == t2_iter_0["outputs.p4"]
    assert t3_iter_1["inputs.p3"] == t2_iter_1["inputs.p3"]
    assert t3_iter_1["inputs.p4"] == t2_iter_1["outputs.p4"]
    assert t2_iter_1["inputs.p3"] == t3_iter_0["outputs.p3"]


def test_wk_loop_input_sources_default(null_config, tmp_path: Path):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2", default_value=2)],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + <<parameter:p2>>))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
    )
    wk.add_loop(hf.Loop(tasks=[0], num_iterations=2))
    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()

    assert t1_iter_0["inputs.p2"] == t1_iter_1["inputs.p2"]


def test_wk_loop_input_sources_iterable_param_default(null_config, tmp_path: Path):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1", default_value=1)],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 10))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
    )
    wk.add_loop(hf.Loop(tasks=[0], num_iterations=3))
    # first iteration should be the default value, second and third iterations should
    # be from previous iteration outputs:
    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t1_iter_2 = wk.tasks.t1.elements[0].iterations[2].get_data_idx()

    assert t1_iter_0["inputs.p1"] != t1_iter_1["inputs.p1"]
    assert t1_iter_1["inputs.p1"] != t1_iter_2["inputs.p1"]
    assert t1_iter_1["inputs.p1"] == t1_iter_0["outputs.p1"]
    assert t1_iter_2["inputs.p1"] == t1_iter_1["outputs.p1"]


def test_wk_loop_input_sources_iterable_param_default_conditional_action(
    null_config, tmp_path: Path
):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput("p1", default_value=1),
            hf.SchemaInput("p2", default_value=None),
        ],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 10))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
                environments=[act_env],
            ),
            hf.Action(
                commands=[hf.Command("Write-Output ((<<parameter:p2>> + 10))")],
                environments=[act_env],
                rules=[
                    hf.ActionRule(path="inputs.p2", condition=Value.not_equal_to(None))
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
    )
    wk.add_loop(hf.Loop(tasks=[0], num_iterations=3))
    # first iteration should be the default value, second and third iterations should
    # be from previous iteration outputs:
    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t1_iter_2 = wk.tasks.t1.elements[0].iterations[2].get_data_idx()

    assert t1_iter_0["inputs.p1"] != t1_iter_1["inputs.p1"]
    assert t1_iter_1["inputs.p1"] != t1_iter_2["inputs.p1"]
    assert t1_iter_1["inputs.p1"] == t1_iter_0["outputs.p1"]
    assert t1_iter_2["inputs.p1"] == t1_iter_1["outputs.p1"]


def test_wk_loop_input_sources_including_non_iteration_task_source_with_groups(
    null_config, tmp_path: Path
):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1>> + 100))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2"), hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p2>> + <<parameter:p3>>))",
                        stdout="<<int(parameter:p4)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    ts3 = hf.TaskSchema(
        objective="t3",
        inputs=[
            hf.SchemaInput("p3", labels={"": {"group": "my_group"}}),
            hf.SchemaInput("p4", labels={"": {"group": "my_group"}}),
        ],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<sum(parameter:p3)>> + <<sum(parameter:p4)>>))",
                        stdout="<<int(parameter:p3)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(
                schema=ts2,
                sequences=[hf.ValueSequence(path="inputs.p3", values=[301, 302])],
                groups=[hf.ElementGroup(name="my_group")],
            ),
            hf.Task(schema=ts3),
        ],
    )
    wk.add_loop(hf.Loop(tasks=[1, 2], num_iterations=2))

    t2_elem_0_iter_0 = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_elem_1_iter_0 = wk.tasks.t2.elements[1].iterations[0].get_data_idx()
    t2_elem_0_iter_1 = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_elem_1_iter_1 = wk.tasks.t2.elements[1].iterations[1].get_data_idx()

    t3_iter_0 = wk.tasks.t3.elements[0].iterations[0].get_data_idx()
    t3_iter_1 = wk.tasks.t3.elements[0].iterations[1].get_data_idx()
    assert len(t3_iter_0["inputs.p3"]) == len(t3_iter_1["inputs.p3"]) == 2
    assert len(t3_iter_0["inputs.p4"]) == len(t3_iter_1["inputs.p4"]) == 2
    assert t3_iter_0["inputs.p3"] == [
        t2_elem_0_iter_0["inputs.p3"],
        t2_elem_1_iter_0["inputs.p3"],
    ]
    assert t3_iter_0["inputs.p4"] == [
        t2_elem_0_iter_0["outputs.p4"],
        t2_elem_1_iter_0["outputs.p4"],
    ]
    assert t3_iter_1["inputs.p3"] == [
        t2_elem_0_iter_1["inputs.p3"],
        t2_elem_1_iter_1["inputs.p3"],
    ]
    assert t3_iter_1["inputs.p4"] == [
        t2_elem_0_iter_1["outputs.p4"],
        t2_elem_1_iter_1["outputs.p4"],
    ]


def test_loop_local_sub_parameters(null_config, tmp_path: Path):
    act_env = hf.ActionEnvironment("null_env")
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1c")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p1c.a>> + 100))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p1c")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output ((<<parameter:p2>> + 100))",
                        stdout="<<parameter:p1c>>",
                    )
                ],
                environments=[act_env],
            ),
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(
                schema=ts1,
                inputs=[
                    hf.InputValue(parameter="p1c", value=P1_parameter_cls(a=101)),
                    hf.InputValue(parameter="p1c", path="d", value=9),
                ],
            ),
            hf.Task(schema=ts2),
        ],
    )
    wk.add_loop(hf.Loop(tasks=[0, 1], num_iterations=2))

    t1_iter_0 = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t2_iter_0 = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t1_iter_1 = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t2_iter_1 = wk.tasks.t2.elements[0].iterations[1].get_data_idx()

    assert t2_iter_0["inputs.p2"] == t1_iter_0["outputs.p2"]
    assert t1_iter_1["inputs.p1c"] == t2_iter_0["outputs.p1c"]
    assert t2_iter_1["inputs.p2"] == t1_iter_1["outputs.p2"]
    assert t1_iter_0["inputs.p1c.d"] == t1_iter_1["inputs.p1c.d"]


def test_nested_loop_iter_loop_idx(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
        loops=[
            hf.Loop(name="outer_loop", tasks=[0], num_iterations=1),
            hf.Loop(name="inner_loop", tasks=[0], num_iterations=1),
        ],
    )
    assert wk.tasks[0].elements[0].iterations[0].loop_idx == {
        "inner_loop": 0,
        "outer_loop": 0,
    }


def test_schema_input_with_group_sourced_from_prev_iteration(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<parameter:p1>> + 1 ))", stdout="<<parameter:p2>>"
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2", group="my_group")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<parameter:p2>> + 2 ))", stdout="<<parameter:p3>>"
                    )
                ]
            )
        ],
    )
    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<parameter:p3>> + 3 ))", stdout="<<parameter:p2>>"
                    )
                ]
            )
        ],
    )

    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2, 3])],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(schema=s2)
    t3 = hf.Task(
        schema=s3,
        repeats=3,
        groups=[hf.ElementGroup(name="my_group")],
    )

    l1 = hf.Loop(name="my_loop", tasks=[1, 2], num_iterations=2)

    wk = hf.Workflow.from_template_data(
        template_name="test_loops",
        path=tmp_path,
        tasks=[t1, t2, t3],
        loops=[l1],
    )

    assert wk.tasks.t2.elements[0].iterations[0].get_data_idx()["inputs.p2"] == [
        i.get_data_idx()["outputs.p2"] for i in wk.tasks.t1.elements
    ]
    assert [
        i.iterations[0].get_data_idx()["inputs.p3"] for i in wk.tasks.t3.elements
    ] == [wk.tasks.t2.elements[0].iterations[0].get_data_idx()["outputs.p3"]] * 3
    assert wk.tasks.t2.elements[0].iterations[1].get_data_idx()["inputs.p2"] == [
        i.iterations[0].get_data_idx()["outputs.p2"] for i in wk.tasks.t3.elements
    ]
    assert [
        i.iterations[1].get_data_idx()["inputs.p3"] for i in wk.tasks.t3.elements
    ] == [wk.tasks.t2.elements[0].iterations[1].get_data_idx()["outputs.p3"]] * 3


def test_loop_downstream_tasks(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p2>> + 100)",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts1),
            hf.Task(schema=ts2, inputs={"p2": 201}),
        ],
        loops=[
            hf.Loop(name="my_loop", tasks=[1, 2], num_iterations=2),
        ],
    )
    assert list(wk.loops.my_loop.downstream_tasks) == [wk.tasks[3]]
    assert list(wk.loops.my_loop.upstream_tasks) == [wk.tasks[0]]


def test_raise_loop_task_subset_error(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    with pytest.raises(LoopTaskSubsetError):
        hf.Workflow.from_template_data(
            template_name="test_loop",
            path=tmp_path,
            tasks=[
                hf.Task(schema=ts1, inputs={"p1": 101}),
                hf.Task(schema=ts1),
                hf.Task(schema=ts1),
            ],
            loops=[
                hf.Loop(name="my_loop", tasks=[2, 1], num_iterations=2),
            ],
        )


def test_add_iteration_updates_downstream_data_idx_loop_output_param(
    new_null_config, tmp_path: Path
):
    # loop output (but not iterable) parameter sourced in task downstream of loop:
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        (
            {"p2": None},
            (
                "p2",
                "p3",
            ),
            "t2",
        ),
        ({"p3": None}, ("p4",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]
    loops = [hf.Loop(tasks=[1], num_iterations=3)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    t1_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_i2_di = wk.tasks.t2.elements[0].iterations[2].get_data_idx()
    t3_di = wk.tasks.t3.elements[0].get_data_idx()

    # final task should get its input from the final iteration of the second task
    assert t2_i0_di["inputs.p2"] == t1_di["outputs.p2"]
    assert t2_i1_di["inputs.p2"] == t2_i0_di["outputs.p2"]
    assert t2_i2_di["inputs.p2"] == t2_i1_di["outputs.p2"]
    assert t3_di["inputs.p3"] == t2_i2_di["outputs.p3"]


def test_add_iteration_updates_downstream_data_idx_loop_output_param_multi_element(
    new_null_config, tmp_path: Path
):
    # loop output (but not iterable) parameter sourced in task downstream of loop - multi
    # element
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        (
            {"p2": None},
            (
                "p2",
                "p3",
            ),
            "t2",
        ),
        ({"p3": None}, ("p4",), "t3"),
    )
    tasks = [
        hf.Task(s1, sequences=[hf.ValueSequence("inputs.p1", values=[100, 101])]),
        hf.Task(s2),
        hf.Task(s3),
    ]
    loops = [hf.Loop(tasks=[1], num_iterations=3)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    assert wk.tasks.t1.num_elements == 2
    assert wk.tasks.t2.num_elements == 2
    assert wk.tasks.t3.num_elements == 2

    t1_e0_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_e0_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_e0_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_e0_i2_di = wk.tasks.t2.elements[0].iterations[2].get_data_idx()
    t3_e0_di = wk.tasks.t3.elements[0].get_data_idx()

    t1_e1_di = wk.tasks.t1.elements[1].get_data_idx()
    t2_e1_i0_di = wk.tasks.t2.elements[1].iterations[0].get_data_idx()
    t2_e1_i1_di = wk.tasks.t2.elements[1].iterations[1].get_data_idx()
    t2_e1_i2_di = wk.tasks.t2.elements[1].iterations[2].get_data_idx()
    t3_e1_di = wk.tasks.t3.elements[1].get_data_idx()

    assert t2_e0_i0_di["inputs.p2"] == t1_e0_di["outputs.p2"]
    assert t2_e0_i1_di["inputs.p2"] == t2_e0_i0_di["outputs.p2"]
    assert t2_e0_i2_di["inputs.p2"] == t2_e0_i1_di["outputs.p2"]
    assert t3_e0_di["inputs.p3"] == t2_e0_i2_di["outputs.p3"]

    assert t2_e1_i0_di["inputs.p2"] == t1_e1_di["outputs.p2"]
    assert t2_e1_i1_di["inputs.p2"] == t2_e1_i0_di["outputs.p2"]
    assert t2_e1_i2_di["inputs.p2"] == t2_e1_i1_di["outputs.p2"]
    assert t3_e1_di["inputs.p3"] == t2_e1_i2_di["outputs.p3"]


def test_add_iteration_updates_downstream_data_idx_loop_output_param_multi_element_to_group(
    new_null_config, tmp_path: Path
):
    # loop output (but not iterable) parameter sourced in task downstream of loop - multi
    # element group
    s1, s2 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        (
            {"p2": None},
            (
                "p2",
                "p3",
            ),
            "t2",
        ),
    )
    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("p3", group="all")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<sum(parameter:p3)>>))",
                        stdout="<<parameter:p4>>",
                    )
                ],
            )
        ],
    )
    tasks = [
        hf.Task(s1, sequences=[hf.ValueSequence("inputs.p1", values=[100, 101])]),
        hf.Task(s2, groups=[hf.ElementGroup(name="all")]),
        hf.Task(s3),
    ]
    loops = [hf.Loop(tasks=[1], num_iterations=3)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )
    assert wk.tasks.t1.num_elements == 2
    assert wk.tasks.t2.num_elements == 2
    assert wk.tasks.t3.num_elements == 1

    t1_e0_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_e0_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_e0_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_e0_i2_di = wk.tasks.t2.elements[0].iterations[2].get_data_idx()

    t1_e1_di = wk.tasks.t1.elements[1].get_data_idx()
    t2_e1_i0_di = wk.tasks.t2.elements[1].iterations[0].get_data_idx()
    t2_e1_i1_di = wk.tasks.t2.elements[1].iterations[1].get_data_idx()
    t2_e1_i2_di = wk.tasks.t2.elements[1].iterations[2].get_data_idx()

    t3_e0_di = wk.tasks.t3.elements[0].get_data_idx()

    assert t2_e0_i0_di["inputs.p2"] == t1_e0_di["outputs.p2"]
    assert t2_e0_i1_di["inputs.p2"] == t2_e0_i0_di["outputs.p2"]
    assert t2_e0_i2_di["inputs.p2"] == t2_e0_i1_di["outputs.p2"]

    assert t2_e1_i0_di["inputs.p2"] == t1_e1_di["outputs.p2"]
    assert t2_e1_i1_di["inputs.p2"] == t2_e1_i0_di["outputs.p2"]
    assert t2_e1_i2_di["inputs.p2"] == t2_e1_i1_di["outputs.p2"]

    assert t3_e0_di["inputs.p3"] == [t2_e0_i2_di["outputs.p3"], t2_e1_i2_di["outputs.p3"]]


def test_add_iteration_updates_downstream_data_idx_loop_iterable_param(
    new_null_config, tmp_path: Path
):
    # loop iterable parameter sourced in task downstream of loop:
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p3",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]
    loops = [hf.Loop(tasks=[1], num_iterations=3)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )
    t1_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_i2_di = wk.tasks.t2.elements[0].iterations[2].get_data_idx()
    t3_di = wk.tasks.t3.elements[0].get_data_idx()

    # final task should get its input from the final iteration of the second task
    assert t2_i0_di["inputs.p2"] == t1_di["outputs.p2"]
    assert t2_i1_di["inputs.p2"] == t2_i0_di["outputs.p2"]
    assert t2_i2_di["inputs.p2"] == t2_i1_di["outputs.p2"]
    assert t3_di["inputs.p2"] == t2_i2_di["outputs.p2"]


def test_add_iteration_updates_downstream_data_idx_loop_iterable_param_multi_element(
    new_null_config, tmp_path: Path
):
    # loop iterable parameter sourced in task downstream of loop - multi element:
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p3",), "t3"),
    )
    tasks = [
        hf.Task(s1, sequences=[hf.ValueSequence("inputs.p1", values=[100, 101])]),
        hf.Task(s2),
        hf.Task(s3),
    ]
    loops = [hf.Loop(tasks=[1], num_iterations=3)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )
    t1_e0_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_e0_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_e0_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_e0_i2_di = wk.tasks.t2.elements[0].iterations[2].get_data_idx()
    t3_e0_di = wk.tasks.t3.elements[0].get_data_idx()

    t1_e1_di = wk.tasks.t1.elements[1].get_data_idx()
    t2_e1_i0_di = wk.tasks.t2.elements[1].iterations[0].get_data_idx()
    t2_e1_i1_di = wk.tasks.t2.elements[1].iterations[1].get_data_idx()
    t2_e1_i2_di = wk.tasks.t2.elements[1].iterations[2].get_data_idx()
    t3_e1_di = wk.tasks.t3.elements[1].get_data_idx()

    # final task should get its input from the final iteration of the second task
    assert t2_e0_i0_di["inputs.p2"] == t1_e0_di["outputs.p2"]
    assert t2_e0_i1_di["inputs.p2"] == t2_e0_i0_di["outputs.p2"]
    assert t2_e0_i2_di["inputs.p2"] == t2_e0_i1_di["outputs.p2"]
    assert t3_e0_di["inputs.p2"] == t2_e0_i2_di["outputs.p2"]

    assert t2_e1_i0_di["inputs.p2"] == t1_e1_di["outputs.p2"]
    assert t2_e1_i1_di["inputs.p2"] == t2_e1_i0_di["outputs.p2"]
    assert t2_e1_i2_di["inputs.p2"] == t2_e1_i1_di["outputs.p2"]
    assert t3_e1_di["inputs.p2"] == t2_e1_i2_di["outputs.p2"]


def test_add_iteration_updates_downstream_data_idx_loop_iterable_param_multi_element_to_group(
    new_null_config, tmp_path: Path
):
    # loop iterable parameter sourced in task downstream of loop - multi element:
    s1, s2 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
    )

    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("p2", group="all")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<sum(parameter:p2)>>))",
                        stdout="<<parameter:p3>>",
                    )
                ],
            )
        ],
    )
    tasks = [
        hf.Task(s1, sequences=[hf.ValueSequence("inputs.p1", values=[100, 101])]),
        hf.Task(s2, groups=[hf.ElementGroup(name="all")]),
        hf.Task(s3),
    ]
    loops = [hf.Loop(tasks=[1], num_iterations=3)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )
    assert wk.tasks.t1.num_elements == 2
    assert wk.tasks.t2.num_elements == 2
    assert wk.tasks.t3.num_elements == 1

    t1_e0_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_e0_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_e0_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t2_e0_i2_di = wk.tasks.t2.elements[0].iterations[2].get_data_idx()

    t1_e1_di = wk.tasks.t1.elements[1].get_data_idx()
    t2_e1_i0_di = wk.tasks.t2.elements[1].iterations[0].get_data_idx()
    t2_e1_i1_di = wk.tasks.t2.elements[1].iterations[1].get_data_idx()
    t2_e1_i2_di = wk.tasks.t2.elements[1].iterations[2].get_data_idx()

    t3_e0_di = wk.tasks.t3.elements[0].get_data_idx()

    assert t2_e0_i0_di["inputs.p2"] == t1_e0_di["outputs.p2"]
    assert t2_e0_i1_di["inputs.p2"] == t2_e0_i0_di["outputs.p2"]
    assert t2_e0_i2_di["inputs.p2"] == t2_e0_i1_di["outputs.p2"]

    assert t2_e1_i0_di["inputs.p2"] == t1_e1_di["outputs.p2"]
    assert t2_e1_i1_di["inputs.p2"] == t2_e1_i0_di["outputs.p2"]
    assert t2_e1_i2_di["inputs.p2"] == t2_e1_i1_di["outputs.p2"]

    assert t3_e0_di["inputs.p2"] == [t2_e0_i2_di["outputs.p2"], t2_e1_i2_di["outputs.p2"]]


def test_add_iteration_correct_downstream_data_idx_iterable_param_downstream_adjacent_loop(
    null_config, tmp_path: Path
):

    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]

    # downstream loop added after upstream loop:
    loops = [
        hf.Loop(tasks=[1], num_iterations=2),
        hf.Loop(tasks=[2], num_iterations=2),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    t1_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t3_i0_di = wk.tasks.t3.elements[0].iterations[0].get_data_idx()
    t3_i1_di = wk.tasks.t3.elements[0].iterations[1].get_data_idx()

    # final task should get its input from the final iteration of the second task
    assert t2_i0_di["inputs.p2"] == t1_di["outputs.p2"]
    assert t2_i1_di["inputs.p2"] == t2_i0_di["outputs.p2"]
    assert t3_i0_di["inputs.p2"] == t2_i1_di["outputs.p2"]
    assert t3_i1_di["inputs.p2"] == t3_i0_di["outputs.p2"]

    t1_iter_di = wk.tasks.t1.elements[0].iterations[0].data_idx
    t2_i0_iter_di = wk.tasks.t2.elements[0].iterations[0].data_idx
    t2_i1_iter_di = wk.tasks.t2.elements[0].iterations[1].data_idx
    t3_i0_iter_di = wk.tasks.t3.elements[0].iterations[0].data_idx
    t3_i1_iter_di = wk.tasks.t3.elements[0].iterations[1].data_idx

    assert t2_i0_iter_di["inputs.p2"] == t1_iter_di["outputs.p2"]
    assert t2_i1_iter_di["inputs.p2"] == t2_i0_iter_di["outputs.p2"]
    assert t3_i0_iter_di["inputs.p2"] == t2_i1_iter_di["outputs.p2"]
    assert t3_i1_iter_di["inputs.p2"] == t3_i0_iter_di["outputs.p2"]


def test_add_iteration_correct_downstream_data_idx_iterable_param_downstream_adjacent_loop_added_before(
    null_config, tmp_path: Path
):
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]

    # upstream loop added after downstream loop:
    loops = [
        hf.Loop(tasks=[2], num_iterations=2),
        hf.Loop(tasks=[1], num_iterations=2),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    t1_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t3_i0_di = wk.tasks.t3.elements[0].iterations[0].get_data_idx()
    t3_i1_di = wk.tasks.t3.elements[0].iterations[1].get_data_idx()

    # final task should get its input from the final iteration of the second task
    assert t2_i0_di["inputs.p2"] == t1_di["outputs.p2"]
    assert t2_i1_di["inputs.p2"] == t2_i0_di["outputs.p2"]
    assert t3_i0_di["inputs.p2"] == t2_i1_di["outputs.p2"]
    assert t3_i1_di["inputs.p2"] == t3_i0_di["outputs.p2"]

    t1_iter_di = wk.tasks.t1.elements[0].iterations[0].data_idx
    t2_i0_iter_di = wk.tasks.t2.elements[0].iterations[0].data_idx
    t2_i1_iter_di = wk.tasks.t2.elements[0].iterations[1].data_idx
    t3_i0_iter_di = wk.tasks.t3.elements[0].iterations[0].data_idx
    t3_i1_iter_di = wk.tasks.t3.elements[0].iterations[1].data_idx

    assert t2_i0_iter_di["inputs.p2"] == t1_iter_di["outputs.p2"]
    assert t2_i1_iter_di["inputs.p2"] == t2_i0_iter_di["outputs.p2"]
    assert t3_i0_iter_di["inputs.p2"] == t2_i1_iter_di["outputs.p2"]
    assert t3_i1_iter_di["inputs.p2"] == t3_i0_iter_di["outputs.p2"]


def test_add_iteration_correct_downstream_data_idx_iterable_param_downstream_multi_task_adjacent_loop_added_before(
    null_config, tmp_path: Path
):
    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
        ({"p2": None}, ("p2",), "t4"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
        hf.Task(s4),
    ]

    # upstream loop added after downstream loop:
    loops = [
        hf.Loop(tasks=[2, 3], num_iterations=2),
        hf.Loop(tasks=[1], num_iterations=2),
    ]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    t1_di = wk.tasks.t1.elements[0].get_data_idx()
    t2_i0_di = wk.tasks.t2.elements[0].iterations[0].get_data_idx()
    t2_i1_di = wk.tasks.t2.elements[0].iterations[1].get_data_idx()
    t3_i0_di = wk.tasks.t3.elements[0].iterations[0].get_data_idx()
    t3_i1_di = wk.tasks.t3.elements[0].iterations[1].get_data_idx()
    t4_i0_di = wk.tasks.t4.elements[0].iterations[0].get_data_idx()
    t4_i1_di = wk.tasks.t4.elements[0].iterations[1].get_data_idx()

    assert t2_i0_di["inputs.p2"] == t1_di["outputs.p2"]
    assert t2_i1_di["inputs.p2"] == t2_i0_di["outputs.p2"]

    assert t3_i0_di["inputs.p2"] == t2_i1_di["outputs.p2"]
    assert t3_i1_di["inputs.p2"] == t4_i0_di["outputs.p2"]

    assert t4_i0_di["inputs.p2"] == t3_i0_di["outputs.p2"]
    assert t4_i1_di["inputs.p2"] == t3_i1_di["outputs.p2"]

    t1_iter_di = wk.tasks.t1.elements[0].iterations[0].data_idx
    t2_i0_iter_di = wk.tasks.t2.elements[0].iterations[0].data_idx
    t2_i1_iter_di = wk.tasks.t2.elements[0].iterations[1].data_idx
    t3_i0_iter_di = wk.tasks.t3.elements[0].iterations[0].data_idx
    t3_i1_iter_di = wk.tasks.t3.elements[0].iterations[1].data_idx
    t4_i0_iter_di = wk.tasks.t4.elements[0].iterations[0].data_idx
    t4_i1_iter_di = wk.tasks.t4.elements[0].iterations[1].data_idx

    assert t2_i0_iter_di["inputs.p2"] == t1_iter_di["outputs.p2"]
    assert t2_i1_iter_di["inputs.p2"] == t2_i0_iter_di["outputs.p2"]
    assert t3_i0_iter_di["inputs.p2"] == t2_i1_iter_di["outputs.p2"]
    assert t3_i1_iter_di["inputs.p2"] == t4_i0_iter_di["outputs.p2"]
    assert t4_i0_iter_di["inputs.p2"] == t3_i0_iter_di["outputs.p2"]
    assert t4_i1_iter_di["inputs.p2"] == t3_i1_iter_di["outputs.p2"]


def test_nested_loops_with_downstream_updates_iteration_pathway(
    null_config, tmp_path: Path
):
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p1",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[1], num_iterations=2),
        hf.Loop(name="outer", tasks=[0, 1, 2], num_iterations=2),
    ]

    # when adding the inner loop iterations, the data index of the downstream task t3
    # must be updated to use the newly-added output. This should happen once before the
    # outer loop is added, and once again when adding the inner loop iteration as part of
    # adding the outer loop's second iteration!

    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update_nested",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)

    # task insert IDs:
    assert [i[0] for i in pathway] == [0, 1, 1, 2, 0, 1, 1, 2]

    # loop indices:
    assert [i[1] for i in pathway] == [
        {"outer": 0},
        {"outer": 0, "inner": 0},
        {"outer": 0, "inner": 1},
        {"outer": 0},
        {"outer": 1},
        {"outer": 1, "inner": 0},
        {"outer": 1, "inner": 1},
        {"outer": 1},
    ]

    # flow of parameter p1/p2 (element zero):
    assert pathway[0][2][0]["outputs.p2"] == pathway[1][2][0]["inputs.p2"]
    assert pathway[1][2][0]["outputs.p2"] == pathway[2][2][0]["inputs.p2"]
    assert pathway[2][2][0]["outputs.p2"] == pathway[3][2][0]["inputs.p2"]
    assert pathway[3][2][0]["outputs.p1"] == pathway[4][2][0]["inputs.p1"]
    assert pathway[4][2][0]["outputs.p2"] == pathway[5][2][0]["inputs.p2"]
    assert pathway[5][2][0]["outputs.p2"] == pathway[6][2][0]["inputs.p2"]
    assert pathway[6][2][0]["outputs.p2"] == pathway[7][2][0]["inputs.p2"]


def test_multi_task_loop_with_downstream_updates_iteration_pathway(
    null_config, tmp_path: Path
):
    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
        ({"p2": None}, ("p3",), "t4"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
        hf.Task(s4),
    ]

    loops = [
        hf.Loop(tasks=[1, 2], num_iterations=2),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)

    # task insert IDs:
    assert [i[0] for i in pathway] == [0, 1, 2, 1, 2, 3]

    # loop indices:
    assert [i[1] for i in pathway] == [
        {},
        {"loop_0": 0},
        {"loop_0": 0},
        {"loop_0": 1},
        {"loop_0": 1},
        {},
    ]

    # flow of parameter p2 (element zero):
    assert pathway[0][2][0]["outputs.p2"] == pathway[1][2][0]["inputs.p2"]
    assert pathway[1][2][0]["outputs.p2"] == pathway[2][2][0]["inputs.p2"]
    assert pathway[2][2][0]["outputs.p2"] == pathway[3][2][0]["inputs.p2"]
    assert pathway[3][2][0]["outputs.p2"] == pathway[4][2][0]["inputs.p2"]
    assert pathway[4][2][0]["outputs.p2"] == pathway[5][2][0]["inputs.p2"]


def test_multi_nested_loops_with_downstream_updates_iteration_pathway(
    null_config, tmp_path: Path
):

    s1, s2, s3, s4, s5, s6 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
        ({"p2": None}, ("p2",), "t4"),
        ({"p2": None}, ("p1",), "t5"),
        ({"p1": None}, ("p3",), "t6"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
        hf.Task(s4),
        hf.Task(s5),
        hf.Task(s6),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[1], num_iterations=2),
        hf.Loop(name="middle", tasks=[1, 2], num_iterations=2),
        hf.Loop(name="outer", tasks=[0, 1, 2, 3, 4], num_iterations=2),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update_nested",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)

    # task insert IDs:
    assert [i[0] for i in pathway] == [
        0,
        1,
        1,
        2,
        1,
        1,
        2,
        3,
        4,
        0,
        1,
        1,
        2,
        1,
        1,
        2,
        3,
        4,
        5,
    ]

    # loop indices:
    assert [i[1] for i in pathway] == [
        {"outer": 0},
        {"outer": 0, "middle": 0, "inner": 0},
        {"outer": 0, "middle": 0, "inner": 1},
        {"outer": 0, "middle": 0},
        {"outer": 0, "middle": 1, "inner": 0},
        {"outer": 0, "middle": 1, "inner": 1},
        {"outer": 0, "middle": 1},
        {"outer": 0},
        {"outer": 0},
        {"outer": 1},
        {"outer": 1, "middle": 0, "inner": 0},
        {"outer": 1, "middle": 0, "inner": 1},
        {"outer": 1, "middle": 0},
        {"outer": 1, "middle": 1, "inner": 0},
        {"outer": 1, "middle": 1, "inner": 1},
        {"outer": 1, "middle": 1},
        {"outer": 1},
        {"outer": 1},
        {},
    ]

    # flow of parameter p1/p2 (element zero):
    assert pathway[0][2][0]["outputs.p2"] == pathway[1][2][0]["inputs.p2"]
    assert pathway[1][2][0]["outputs.p2"] == pathway[2][2][0]["inputs.p2"]
    assert pathway[2][2][0]["outputs.p2"] == pathway[3][2][0]["inputs.p2"]
    assert pathway[3][2][0]["outputs.p2"] == pathway[4][2][0]["inputs.p2"]
    assert pathway[4][2][0]["outputs.p2"] == pathway[5][2][0]["inputs.p2"]
    assert pathway[5][2][0]["outputs.p2"] == pathway[6][2][0]["inputs.p2"]
    assert pathway[6][2][0]["outputs.p2"] == pathway[7][2][0]["inputs.p2"]
    assert pathway[7][2][0]["outputs.p2"] == pathway[8][2][0]["inputs.p2"]
    assert pathway[8][2][0]["outputs.p1"] == pathway[9][2][0]["inputs.p1"]
    assert pathway[9][2][0]["outputs.p2"] == pathway[10][2][0]["inputs.p2"]
    assert pathway[10][2][0]["outputs.p2"] == pathway[11][2][0]["inputs.p2"]
    assert pathway[11][2][0]["outputs.p2"] == pathway[12][2][0]["inputs.p2"]
    assert pathway[12][2][0]["outputs.p2"] == pathway[13][2][0]["inputs.p2"]
    assert pathway[13][2][0]["outputs.p2"] == pathway[14][2][0]["inputs.p2"]
    assert pathway[14][2][0]["outputs.p2"] == pathway[15][2][0]["inputs.p2"]
    assert pathway[15][2][0]["outputs.p2"] == pathway[16][2][0]["inputs.p2"]
    assert pathway[16][2][0]["outputs.p2"] == pathway[17][2][0]["inputs.p2"]
    assert pathway[17][2][0]["outputs.p1"] == pathway[18][2][0]["inputs.p1"]


def test_add_iteration_updates_downstream_data_idx_loop_output_param_including_task_input_sources(
    new_null_config, tmp_path: Path
):
    # task `t3` input `p1` has `InputSource.task(task_ref=1, task_source_type="input")`,
    # so `t3` elements needs to have data indices updated, since task `t2` (i.e.
    # `task_ref=1`) will have had its data indices updated:
    s1, s2, s3 = make_schemas(
        ({"p1": None}, ("p1",), "t1"),
        ({"p1": None}, ("p2",), "t2"),
        ({"p1": None, "p2": None}, ("p3",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]
    loops = [hf.Loop(tasks=[0], num_iterations=2)]

    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update_task_input_source",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    t1_i0_di = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_i1_di = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t2_di = wk.tasks.t2.elements[0].get_data_idx()
    t3_di = wk.tasks.t3.elements[0].get_data_idx()

    assert t1_i0_di["outputs.p1"] == t1_i1_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t2_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t3_di["inputs.p1"]
    assert t2_di["outputs.p2"] == t3_di["inputs.p2"]


def test_add_iteration_updates_downstream_data_idx_loop_output_param_including_task_input_sources_twice(
    new_null_config, tmp_path: Path
):
    # tasks `t3/t4` inputs `p1` have `InputSource.task(task_ref=1/2, task_source_type="input")`,
    # so `t3/t4` elements needs to have data indices updated, since task `t2/t3` (i.e.
    # `task_ref=1/2`) will have had their data indices updated:

    s1, s2, s3, s4 = make_schemas(
        ({"p1": None}, ("p1",), "t1"),
        ({"p1": None}, ("p2",), "t2"),
        ({"p1": None, "p2": None}, ("p3",), "t3"),
        ({"p1": None, "p3": None}, ("p4",), "t4"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
        hf.Task(s4),
    ]
    loops = [hf.Loop(tasks=[0], num_iterations=2)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update_task_input_source",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )
    t1_i0_di = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_i1_di = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t2_di = wk.tasks.t2.elements[0].get_data_idx()
    t3_di = wk.tasks.t3.elements[0].get_data_idx()
    t4_di = wk.tasks.t4.elements[0].get_data_idx()

    assert t1_i0_di["outputs.p1"] == t1_i1_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t2_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t3_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t4_di["inputs.p1"]
    assert t2_di["outputs.p2"] == t3_di["inputs.p2"]


def test_add_iteration_updates_downstream_data_idx_loop_output_param_including_task_input_sources_thrice(
    new_null_config, tmp_path: Path
):
    # tasks `t3/t4/t5` inputs `p1` have `InputSource.task(task_ref=1/2/3, task_source_type="input")`,
    # so `t3/t4/t5` elements needs to have data indices updated, since task `t2/t3/t4` (i.e.
    # `task_ref=1/2/3`) will have had their data indices updated:

    s1, s2, s3, s4, s5 = make_schemas(
        ({"p1": None}, ("p1",), "t1"),
        ({"p1": None}, ("p2",), "t2"),
        ({"p1": None, "p2": None}, ("p3",), "t3"),
        ({"p1": None, "p3": None}, ("p4",), "t4"),
        ({"p1": None, "p4": None}, ("p5",), "t5"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}),
        hf.Task(s2),
        hf.Task(s3),
        hf.Task(s4),
        hf.Task(s5),
    ]
    loops = [hf.Loop(tasks=[0], num_iterations=2)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update_task_input_source",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )
    t1_i0_di = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_i1_di = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t2_di = wk.tasks.t2.elements[0].get_data_idx()
    t3_di = wk.tasks.t3.elements[0].get_data_idx()
    t4_di = wk.tasks.t4.elements[0].get_data_idx()
    t5_di = wk.tasks.t5.elements[0].get_data_idx()

    assert t1_i0_di["outputs.p1"] == t1_i1_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t2_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t3_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t4_di["inputs.p1"]
    assert t1_i1_di["outputs.p1"] == t5_di["inputs.p1"]
    assert t2_di["outputs.p2"] == t3_di["inputs.p2"]


def test_add_iteration_updates_downstream_data_idx_loop_output_param_including_task_input_sources_thrice_multi_element(
    new_null_config, tmp_path: Path
):
    # tasks `t3/t4/t5` inputs `p1` have `InputSource.task(task_ref=1/2/3, task_source_type="input")`,
    # so `t3/t4/t5` elements needs to have data indices updated, since task `t2/t3/t4` (i.e.
    # `task_ref=1/2/3`) will have had their data indices updated:

    s1, s2, s3, s4, s5 = make_schemas(
        ({"p1": None}, ("p1",), "t1"),
        ({"p1": None}, ("p2",), "t2"),
        ({"p1": None, "p2": None}, ("p3",), "t3"),
        ({"p1": None, "p3": None}, ("p4",), "t4"),
        ({"p1": None, "p4": None}, ("p5",), "t5"),
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 100}, repeats=2),
        hf.Task(s2),
        hf.Task(s3),
        hf.Task(s4),
        hf.Task(s5),
    ]
    loops = [hf.Loop(tasks=[0], num_iterations=2)]
    wk = hf.Workflow.from_template_data(
        template_name="loop_param_update_task_input_source",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )
    t1_e0_i0_di = wk.tasks.t1.elements[0].iterations[0].get_data_idx()
    t1_e0_i1_di = wk.tasks.t1.elements[0].iterations[1].get_data_idx()
    t2_e0_di = wk.tasks.t2.elements[0].get_data_idx()
    t3_e0_di = wk.tasks.t3.elements[0].get_data_idx()
    t4_e0_di = wk.tasks.t4.elements[0].get_data_idx()
    t5_e0_di = wk.tasks.t5.elements[0].get_data_idx()

    t1_e1_i0_di = wk.tasks.t1.elements[1].iterations[0].get_data_idx()
    t1_e1_i1_di = wk.tasks.t1.elements[1].iterations[1].get_data_idx()
    t2_e1_di = wk.tasks.t2.elements[1].get_data_idx()
    t3_e1_di = wk.tasks.t3.elements[1].get_data_idx()
    t4_e1_di = wk.tasks.t4.elements[1].get_data_idx()
    t5_e1_di = wk.tasks.t5.elements[1].get_data_idx()

    assert t1_e0_i0_di["outputs.p1"] == t1_e0_i1_di["inputs.p1"]
    assert t1_e0_i1_di["outputs.p1"] == t2_e0_di["inputs.p1"]
    assert t1_e0_i1_di["outputs.p1"] == t3_e0_di["inputs.p1"]
    assert t1_e0_i1_di["outputs.p1"] == t4_e0_di["inputs.p1"]
    assert t1_e0_i1_di["outputs.p1"] == t5_e0_di["inputs.p1"]
    assert t2_e0_di["outputs.p2"] == t3_e0_di["inputs.p2"]

    assert t1_e1_i0_di["outputs.p1"] == t1_e1_i1_di["inputs.p1"]
    assert t1_e1_i1_di["outputs.p1"] == t2_e1_di["inputs.p1"]
    assert t1_e1_i1_di["outputs.p1"] == t3_e1_di["inputs.p1"]
    assert t1_e1_i1_di["outputs.p1"] == t4_e1_di["inputs.p1"]
    assert t1_e1_i1_di["outputs.p1"] == t5_e1_di["inputs.p1"]
    assert t2_e1_di["outputs.p2"] == t3_e1_di["inputs.p2"]


def test_adjacent_loops_iteration_pathway(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    ts2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p2>> + 100)",
                        stdout="<<int(parameter:p2)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
            hf.Task(schema=ts1),
            hf.Task(schema=ts2, inputs={"p2": 201}),
        ],
        loops=[
            hf.Loop(name="loop_A", tasks=[0, 1], num_iterations=2),
            hf.Loop(name="loop_B", tasks=[2], num_iterations=2),
        ],
    )
    assert wk.get_iteration_task_pathway() == [
        (0, {"loop_A": 0}),
        (1, {"loop_A": 0}),
        (0, {"loop_A": 1}),
        (1, {"loop_A": 1}),
        (2, {"loop_B": 0}),
        (2, {"loop_B": 1}),
    ]


def test_get_child_loops_ordered_by_depth(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(schema=ts1, inputs={"p1": 101}),
        ],
        loops=[
            hf.Loop(name="inner", tasks=[0], num_iterations=1),
            hf.Loop(name="middle", tasks=[0], num_iterations=1),
            hf.Loop(name="outer", tasks=[0], num_iterations=1),
        ],
    )
    assert wk.loops.inner.get_child_loops() == []
    assert wk.loops.middle.get_child_loops() == [wk.loops.inner]
    assert wk.loops.outer.get_child_loops() == [wk.loops.middle, wk.loops.inner]


def test_multi_nested_loops(null_config, tmp_path: Path):
    ts1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    )
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[hf.Task(schema=ts1, inputs={"p1": 101})],
        loops=[
            hf.Loop(name="inner", tasks=[0], num_iterations=2),
            hf.Loop(name="middle_1", tasks=[0], num_iterations=3),
            hf.Loop(name="middle_2", tasks=[0], num_iterations=2),
            hf.Loop(name="outer", tasks=[0], num_iterations=2),
        ],
    )
    pathway = wk.get_iteration_task_pathway(ret_iter_IDs=True)
    assert len(pathway) == 2 * 3 * 2 * 2
    assert wk.get_iteration_task_pathway(ret_iter_IDs=True) == [
        (0, {"inner": 0, "middle_1": 0, "middle_2": 0, "outer": 0}, (0,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 0, "outer": 0}, (1,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 0, "outer": 0}, (2,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 0, "outer": 0}, (3,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 0, "outer": 0}, (4,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 0, "outer": 0}, (5,)),
        (0, {"inner": 0, "middle_1": 0, "middle_2": 1, "outer": 0}, (6,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 1, "outer": 0}, (7,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 1, "outer": 0}, (8,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 1, "outer": 0}, (9,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 1, "outer": 0}, (10,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 1, "outer": 0}, (11,)),
        (0, {"inner": 0, "middle_1": 0, "middle_2": 0, "outer": 1}, (12,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 0, "outer": 1}, (13,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 0, "outer": 1}, (14,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 0, "outer": 1}, (15,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 0, "outer": 1}, (16,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 0, "outer": 1}, (17,)),
        (0, {"inner": 0, "middle_1": 0, "middle_2": 1, "outer": 1}, (18,)),
        (0, {"inner": 1, "middle_1": 0, "middle_2": 1, "outer": 1}, (19,)),
        (0, {"inner": 0, "middle_1": 1, "middle_2": 1, "outer": 1}, (20,)),
        (0, {"inner": 1, "middle_1": 1, "middle_2": 1, "outer": 1}, (21,)),
        (0, {"inner": 0, "middle_1": 2, "middle_2": 1, "outer": 1}, (22,)),
        (0, {"inner": 1, "middle_1": 2, "middle_2": 1, "outer": 1}, (23,)),
    ]


def test_nested_loop_input_from_parent_loop_task(null_config, tmp_path: Path):
    """Test that an input in a nested-loop task is correctly sourced from latest
    iteration of the parent loop."""
    wk = make_workflow(
        schemas_spec=[
            ({"p1": None}, ("p2", "p3")),
            ({"p2": None}, ("p4",)),
            ({"p4": None, "p3": None}, ("p2", "p1")),  # testing p3 source
        ],
        path=tmp_path,
        local_inputs={0: {"p1": 101}},
        loops=[
            hf.Loop(name="inner", tasks=[1, 2], num_iterations=3),
            hf.Loop(name="outer", tasks=[0, 1, 2], num_iterations=2),
        ],
    )
    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)
    assert len(pathway) == 14
    p3_out_idx = [i[2][0]["outputs.p3"] for i in pathway if i[0] == 0]
    p3_inp_idx = [i[2][0]["inputs.p3"] for i in pathway if i[0] == 2]
    assert len(p3_out_idx) == 2  # 2 outer iterations
    assert len(p3_inp_idx) == 6  # 2 * 3 iterations
    assert p3_inp_idx == [p3_out_idx[0]] * 3 + [p3_out_idx[1]] * 3


def test_doubly_nested_loop_input_from_parent_loop_task(null_config, tmp_path: Path):
    """Test that an input in a doubly-nested-loop task is correctly sourced from latest
    iteration of the parent loop."""
    # test source of p6 in final task:
    wk = make_workflow(
        schemas_spec=[
            ({"p5": None}, ("p6", "p1")),
            ({"p1": None}, ("p2", "p3")),
            ({"p2": None}, ("p4",)),
            ({"p4": None, "p3": None, "p6": None}, ("p2", "p1", "p5")),
        ],
        path=tmp_path,
        local_inputs={0: {"p5": 101}},
        loops=[
            hf.Loop(name="inner", tasks=[2, 3], num_iterations=3),
            hf.Loop(name="middle", tasks=[1, 2, 3], num_iterations=3),
            hf.Loop(name="outer", tasks=[0, 1, 2, 3], num_iterations=3),
        ],
        overwrite=True,
    )
    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)
    assert len(pathway) == 66

    p6_out_idx = [i[2][0]["outputs.p6"] for i in pathway if i[0] == 0]
    p6_inp_idx = [i[2][0]["inputs.p6"] for i in pathway if i[0] == 3]
    assert len(p6_out_idx) == 3  # 2 outer iterations
    assert len(p6_inp_idx) == 27  # 3 * 3 * 3 iterations
    assert p6_inp_idx == [p6_out_idx[0]] * 9 + [p6_out_idx[1]] * 9 + [p6_out_idx[2]] * 9


def test_loop_non_input_task_input_from_element_group(null_config, tmp_path: Path):
    """Test correct sourcing of an element group input within a loop, for a task that is
    not that loop's "input task" with respect to that parameter."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2"), hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p1>> + 1))",
                        stdout="<<parameter:p2>>",
                        stderr="<<parameter:p3>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2", group="my_group")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<sum(parameter:p2)>> + 1))",
                        stdout="<<parameter:p4>>",
                    )
                ]
            )
        ],
    )
    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("p3", group="my_group"), hf.SchemaInput("p4")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<sum(parameter:p3)>> + <<parameter:p4>>))",
                        stdout="<<parameter:p2>>",
                    )
                ]
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_loop",
        path=tmp_path,
        tasks=[
            hf.Task(
                schema=s1,
                sequences=[hf.ValueSequence("inputs.p1", values=[1, 2, 3])],
                groups=[hf.ElementGroup("my_group")],
            ),
            hf.Task(schema=s2),
            hf.Task(schema=s3),  # test source of p3 (should be group from t1)
        ],
        loops=[hf.Loop(name="inner", tasks=[1, 2], num_iterations=2)],
    )
    pathway = wk.get_iteration_task_pathway(ret_data_idx=True)
    assert len(pathway) == 5

    expected = [i["outputs.p3"] for i in pathway[0][2]]
    for i in pathway:
        if i[0] == 2:  # task 3
            assert i[2][0]["inputs.p3"] == expected


@pytest.mark.integration
def test_multi_task_loop_termination(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p1>> + 1))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p2>> + 1))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ]
            )
        ],
    )
    tasks = [
        hf.Task(schema=s1, inputs={"p1": 0}),
        hf.Task(schema=s2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        loops=[
            hf.Loop(
                tasks=[0, 1],
                num_iterations=3,
                termination=hf.Rule(
                    path="outputs.p1",
                    condition={"value.greater_than": 3},  # should stop after 2nd iter
                ),
            )
        ],
        path=tmp_path,
        template_name="test_loops",
    )
    wk.submit(wait=True, add_to_known=False)
    for task in wk.tasks:
        for element in task.elements:
            for iter_i in element.iterations:
                skips = (i.skip for i in iter_i.action_runs)
                if iter_i.loop_idx[wk.loops[0].name] > 1:
                    assert all(skips)
                    assert iter_i.loop_skipped
                else:
                    assert not any(skips)


@pytest.mark.integration
def test_multi_task_loop_termination_task(null_config, tmp_path: Path):
    """Specify non-default task at which to check for termination."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p1>> + 1))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p2>> + 1))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ]
            )
        ],
    )
    tasks = [
        hf.Task(schema=s1, inputs={"p1": 0}),
        hf.Task(schema=s2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        resources={"any": {"write_app_logs": True}},
        loops=[
            hf.Loop(
                tasks=[0, 1],
                num_iterations=3,
                termination_task=0,  # default would be final task (1)
                termination=hf.Rule(
                    path="inputs.p1",
                    condition={
                        "value.greater_than": 3
                    },  # should stop after first task of final iteration
                ),
            )
        ],
        path=tmp_path,
        template_name="test_loops",
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs_t0 = [j for i in wk.tasks[0].elements[0].iterations for j in i.action_runs]
    runs_t1 = [j for i in wk.tasks[1].elements[0].iterations for j in i.action_runs]

    assert [i.skip for i in runs_t0] == [0, 0, 0]
    assert [i.skip for i in runs_t1] == [0, 0, SkipReason.LOOP_TERMINATION.value]


@pytest.mark.integration
@pytest.mark.skip(reason="need to fix loop termination for multiple elements")
def test_multi_task_loop_termination_multi_element(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p1>> + 1))",
                        stdout="<<int(parameter:p2)>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $((<<parameter:p2>> + 1))",
                        stdout="<<int(parameter:p1)>>",
                    )
                ]
            )
        ],
    )
    tasks = [
        hf.Task(schema=s1, sequences=[hf.ValueSequence(path="inputs.p1", values=[0, 1])]),
        hf.Task(schema=s2),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        loops=[
            hf.Loop(
                tasks=[0, 1],
                num_iterations=3,
                termination=hf.Rule(
                    path="outputs.p1",
                    condition={
                        "value.greater_than": 3
                    },  # should stop after 2nd iter (element 0), 1st iter (element 1)
                ),
            )
        ],
        path=tmp_path,
        template_name="test_loops",
    )
    wk.submit(wait=True, add_to_known=False)
    expected_num_iters = [2, 1]
    for task in wk.tasks:
        for element in task.elements:
            for iter_i in element.iterations:
                skips = (i.skip for i in iter_i.action_runs)
                if (
                    iter_i.loop_idx[wk.loops[0].name]
                    > expected_num_iters[element.index] - 1
                ):
                    assert all(skips)
                    assert iter_i.loop_skipped
                else:
                    assert not any(skips)


def test_loop_termination_task_default(null_config):
    loop = hf.Loop(
        tasks=[0, 1],
        num_iterations=3,
    )
    assert loop.termination_task_insert_ID == 1


def test_loop_termination_task_non_default_specified(null_config):
    loop = hf.Loop(
        tasks=[0, 1],
        num_iterations=3,
        termination_task=0,
    )
    assert loop.termination_task_insert_ID == 0


def test_loop_termination_task_default_specified(null_config):
    loop = hf.Loop(
        tasks=[0, 1],
        num_iterations=3,
        termination_task=1,
    )
    assert loop.termination_task_insert_ID == 1


def test_loop_termination_task_raise_on_bad_task(null_config):
    with pytest.raises(ValueError):
        hf.Loop(
            tasks=[0, 1],
            num_iterations=3,
            termination_task=2,
        )


@pytest.mark.parametrize("num_iters", [1, 2])
def test_inner_loop_num_added_iterations_on_reload(null_config, tmp_path, num_iters):
    # this tests that the pending num_added_iterations are saved correctly when adding
    # loop iterations
    s1, s2 = make_schemas(
        ({"p2": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
    )
    tasks = [
        hf.Task(s1, inputs={"p2": 100}),
        hf.Task(s2),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[0], num_iterations=num_iters),
        hf.Loop(name="outer", tasks=[0, 1], num_iterations=2),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_loop_num_added_iters_reload",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    wk = wk.reload()
    assert wk.loops.inner.num_added_iterations == {
        (0,): num_iters,
        (1,): num_iters,
    }


@pytest.mark.parametrize("num_outer_iters", [1, 2])
def test_outer_loop_num_added_iterations_on_reload(
    null_config, tmp_path, num_outer_iters
):
    # this tests that the pending num_added_iterations are saved correctly when adding
    # loop iterations

    s1, s2 = make_schemas(
        ({"p2": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
    )
    tasks = [
        hf.Task(s1, inputs={"p2": 100}),
        hf.Task(s2),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[0], num_iterations=2),
        hf.Loop(name="outer", tasks=[0, 1], num_iterations=num_outer_iters),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_loop_num_added_iters_reload",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    wk = wk.reload()
    if num_outer_iters == 1:
        assert wk.loops.inner.num_added_iterations == {(0,): 2}
    elif num_outer_iters == 2:
        assert wk.loops.inner.num_added_iterations == {(0,): 2, (1,): 2}


def test_multi_nested_loop_num_added_iterations_on_reload(null_config, tmp_path: Path):
    s1, s2, s3 = make_schemas(
        ({"p2": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p2": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[0], num_iterations=2),
        hf.Loop(name="middle", tasks=[0, 1], num_iterations=3),
        hf.Loop(name="outer", tasks=[0, 1, 2], num_iterations=4),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_loop_num_added_iters_reload",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    wk = wk.reload()
    for loop in wk.loops:
        print(loop.num_added_iterations)

    assert wk.loops.inner.num_added_iterations == {
        (0, 0): 2,
        (1, 0): 2,
        (2, 0): 2,
        (0, 1): 2,
        (1, 1): 2,
        (2, 1): 2,
        (0, 2): 2,
        (1, 2): 2,
        (2, 2): 2,
        (0, 3): 2,
        (1, 3): 2,
        (2, 3): 2,
    }
    assert wk.loops.middle.num_added_iterations == {(0,): 3, (1,): 3, (2,): 3, (3,): 3}
    assert wk.loops.outer.num_added_iterations == {(): 4}


def test_multi_nested_loop_num_added_iterations_on_reload_single_iter_inner(
    null_config, tmp_path: Path
):
    s1, s2, s3 = make_schemas(
        ({"p2": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p2": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[0], num_iterations=1),
        hf.Loop(name="middle", tasks=[0, 1], num_iterations=3),
        hf.Loop(name="outer", tasks=[0, 1, 2], num_iterations=4),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_loop_num_added_iters_reload",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    wk = wk.reload()
    for loop in wk.loops:
        print(loop.num_added_iterations)

    assert wk.loops.inner.num_added_iterations == {
        (0, 0): 1,
        (1, 0): 1,
        (2, 0): 1,
        (0, 1): 1,
        (1, 1): 1,
        (2, 1): 1,
        (0, 2): 1,
        (1, 2): 1,
        (2, 2): 1,
        (0, 3): 1,
        (1, 3): 1,
        (2, 3): 1,
    }
    assert wk.loops.middle.num_added_iterations == {(0,): 3, (1,): 3, (2,): 3, (3,): 3}
    assert wk.loops.outer.num_added_iterations == {(): 4}


def test_multi_nested_loop_num_added_iterations_on_reload_single_iter_middle(
    null_config, tmp_path: Path
):
    s1, s2, s3 = make_schemas(
        ({"p2": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p2": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[0], num_iterations=2),
        hf.Loop(name="middle", tasks=[0, 1], num_iterations=1),
        hf.Loop(name="outer", tasks=[0, 1, 2], num_iterations=4),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_loop_num_added_iters_reload",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    wk = wk.reload()
    for loop in wk.loops:
        print(loop.num_added_iterations)

    assert wk.loops.inner.num_added_iterations == {
        (0, 0): 2,
        (0, 1): 2,
        (0, 2): 2,
        (0, 3): 2,
    }
    assert wk.loops.middle.num_added_iterations == {(0,): 1, (1,): 1, (2,): 1, (3,): 1}
    assert wk.loops.outer.num_added_iterations == {(): 4}


def test_multi_nested_loop_num_added_iterations_on_reload_single_iter_outer(
    null_config, tmp_path: Path
):
    s1, s2, s3 = make_schemas(
        ({"p2": None}, ("p2",), "t1"),
        ({"p2": None}, ("p2",), "t2"),
        ({"p2": None}, ("p2",), "t3"),
    )
    tasks = [
        hf.Task(s1, inputs={"p2": 100}),
        hf.Task(s2),
        hf.Task(s3),
    ]

    loops = [
        hf.Loop(name="inner", tasks=[0], num_iterations=2),
        hf.Loop(name="middle", tasks=[0, 1], num_iterations=3),
        hf.Loop(name="outer", tasks=[0, 1, 2], num_iterations=1),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_loop_num_added_iters_reload",
        tasks=tasks,
        loops=loops,
        path=tmp_path,
    )

    wk = wk.reload()
    for loop in wk.loops:
        print(loop.num_added_iterations)

    assert wk.loops.inner.num_added_iterations == {
        (0, 0): 2,
        (1, 0): 2,
        (2, 0): 2,
    }
    assert wk.loops.middle.num_added_iterations == {(0,): 3}
    assert wk.loops.outer.num_added_iterations == {(): 1}


# TODO: test loop termination across jobscripts
