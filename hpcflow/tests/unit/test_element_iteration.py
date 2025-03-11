from __future__ import annotations
from pathlib import Path
import pytest
from hpcflow.app import app as hf


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_decode(null_config, tmp_path: Path, store: str):
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
    wk = hf.Workflow.from_template_data(
        tasks=[hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=101)])],
        loops=[hf.Loop(tasks=[0], num_iterations=1)],
        path=tmp_path,
        template_name="wk0",
        store=store,
    )
    iter_i = wk.tasks[0].elements[0].iterations[0]
    assert iter_i.id_ == 0
    assert iter_i.index == 0
    assert iter_i.EARs_initialised == True
    assert sorted(iter_i.data_idx) == sorted(
        {"inputs.p1": 2, "resources.any": 1, "outputs.p2": 3}
    )
    assert iter_i.loop_idx == {"loop_0": 0}
    assert sorted(iter_i.schema_parameters) == sorted(
        ["resources.any", "inputs.p1", "outputs.p2"]
    )


@pytest.mark.integration
def test_loop_skipped_true_single_action_elements(null_config, tmp_path):
    ts = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $(( <<parameter:p1>> + 100 ))",
                        stdout="<<int(parameter:p1)>>",
                    ),
                ]
            ),
        ],
    )
    loop_term = hf.Rule(path="outputs.p1", condition={"value.equal_to": 300})
    wk = hf.Workflow.from_template_data(
        template_name="test_loop_skipped",
        path=tmp_path,
        tasks=[hf.Task(schema=ts, inputs={"p1": 100})],
        loops=[
            hf.Loop(name="my_loop", tasks=[0], termination=loop_term, num_iterations=3)
        ],
    )
    # loop should terminate after the second iteration; third iteration should
    # be loop-skipped
    wk.submit(wait=True, add_to_known=False, status=False)
    iters = wk.get_all_element_iterations()

    assert not iters[0].loop_skipped
    assert not iters[1].loop_skipped
    assert iters[2].loop_skipped

    # check latest iteration is the latest non-loop-skipped iteration:
    assert wk.tasks[0].elements[0].latest_iteration_non_skipped.id_ == iters[1].id_

    # check element inputs are from latest non-loop-skipped iteration:
    assert wk.tasks[0].elements[0].inputs.p1.value == 200
    assert wk.tasks[0].elements[0].get("inputs.p1") == 200

    # check element outputs are from latest non-loop-skipped iteration:
    assert wk.tasks[0].elements[0].outputs.p1.value == 300
    assert wk.tasks[0].elements[0].get("outputs.p1") == 300
