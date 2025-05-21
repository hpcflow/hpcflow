from __future__ import annotations
from pathlib import Path
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import MissingElementGroup


def test_group_simple(null_config, tmp_path: Path):
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

    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2, 3])],
        groups=[hf.ElementGroup(name="my_group")],
    )
    t2 = hf.Task(schema=s2)
    wk = hf.Workflow.from_template_data(
        template_name="test_groups",
        path=tmp_path,
        tasks=[t1, t2],
    )
    assert [task.num_elements for task in wk.tasks] == [3, 1]
    assert len(wk.tasks.t2.elements[0].get_data_idx("inputs.p2")["inputs.p2"]) == 3


def test_group_raise_no_elements(null_config, tmp_path: Path):
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

    t1 = hf.Task(
        schema=s1,
        sequences=[hf.ValueSequence("inputs.p1", values=[1, 2, 3])],
    )
    t2 = hf.Task(schema=s2)
    with pytest.raises(MissingElementGroup):
        hf.Workflow.from_template_data(
            template_name="test_groups",
            path=tmp_path,
            tasks=[t1, t2],
        )


def test_group_on_input_only_task(null_config, tmp_path: Path):

    s1 = hf.TaskSchema(objective="t1", inputs=[hf.SchemaInput("p1")])
    s2 = hf.TaskSchema(objective="t2", inputs=[hf.SchemaInput("p1", group="all")])

    t1 = hf.Task(
        schema=s1,
        inputs={"p1": 100},
        repeats=2,
        groups=[hf.ElementGroup(name="all")],  # define a group on a task with no actions
    )
    t2 = hf.Task(schema=s2)

    wk = hf.Workflow.from_template_data(
        template_name="test_input_group",
        path=tmp_path,
        tasks=[t1, t2],
    )
    assert wk.tasks.t1.num_elements == 2
    assert wk.tasks.t2.num_elements == 1
