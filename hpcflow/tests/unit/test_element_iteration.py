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
