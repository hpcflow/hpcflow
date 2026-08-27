import os

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.skip_reason import SkipReason


@pytest.mark.integration
def test_task_condition_implicit_dependence(tmp_path):
    """Test task condition where there is already a parameter-dependence between the
    tasks."""
    s1 = hf.TaskSchema(  # parse p1 as a boolean and map to p2
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command("echo <<parameter:p1>>", stdout="<<bool(parameter:p2)>>")
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p2")],
        outputs=[hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command('echo "<<parameter:p2>>"', stdout="<<parameter:p3>>")
                ]
            )
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        path=tmp_path,
        # config={"log_file_level": "debug"},
        # resources={"any": {"write_app_logs": True}},
        tasks=[
            hf.Task(
                schema=s1, sequences=[hf.ValueSequence("inputs.p1", values=[True, False])]
            ),
            hf.Task(
                schema=s2,
                condition="task.t1.outputs.p2",
            ),
        ],
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_all_EARs()
    assert wk.num_EARs == 4

    # task t1
    assert runs[0].get("outputs.p2") is True
    assert runs[1].get("outputs.p2") is False

    # task t2
    assert runs[2].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[3].skip_reason is SkipReason.TASK_CONDITION_NOT_MET
    assert runs[2].get("outputs.p3") == "True"


@pytest.mark.integration
def test_task_condition_no_dependence(tmp_path):
    """Test task condition where we need to add a parameter-dependence between the
    tasks."""
    s1 = hf.TaskSchema(  # parse p1 as a boolean and map to p2
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command("echo <<parameter:p1>>", stdout="<<bool(parameter:p2)>>")
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command('echo "<<parameter:p3>>"', stdout="<<parameter:p4>>")
                ]
            )
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        path=tmp_path,
        # config={"log_file_level": "debug"},
        # resources={"any": {"write_app_logs": True}},
        tasks=[
            hf.Task(
                schema=s1, sequences=[hf.ValueSequence("inputs.p1", values=[True, False])]
            ),
            hf.Task(
                schema=s2,
                inputs={"p3": 300},
                condition="task.t1.outputs.p2",
            ),
        ],
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_all_EARs()
    assert wk.num_EARs == 4

    # task t1
    assert runs[0].get("outputs.p2") is True
    assert runs[1].get("outputs.p2") is False

    # task t2
    assert runs[2].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[3].skip_reason is SkipReason.TASK_CONDITION_NOT_MET
    assert runs[2].get("outputs.p4") == "300"


@pytest.mark.integration
def test_task_condition_no_dependence_forking(tmp_path):
    """Task task condition where we need to add a parameter-dependence between the
    tasks, and we fork from a single element to multiple elements."""

    s1 = hf.TaskSchema(  # parse p1 as a boolean and map to p2
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command("echo <<parameter:p1>>", stdout="<<bool(parameter:p2)>>")
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p3")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command('echo "<<parameter:p3>>"', stdout="<<parameter:p4>>")
                ]
            )
        ],
    )
    p1_val = True
    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        path=tmp_path,
        # config={"log_file_level": "debug"},
        # resources={"any": {"write_app_logs": True}},
        tasks=[
            hf.Task(schema=s1, inputs={"p1": p1_val}),  # single element
            hf.Task(
                schema=s2,
                sequences=[hf.ValueSequence("inputs.p3", values=[301, 302])],
                condition="task.t1.outputs.p2",
            ),
        ],
    )

    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_all_EARs()
    assert wk.num_EARs == 3

    assert runs[0].get_dependent_EARs() == {1, 2}
    assert runs[1].get_EAR_dependencies() == {0}
    assert runs[2].get_EAR_dependencies() == {0}

    assert runs[0].get("outputs.p2") is p1_val
    if p1_val:
        assert runs[1].skip_reason is SkipReason.NOT_SKIPPED
        assert runs[2].skip_reason is SkipReason.NOT_SKIPPED
    else:
        assert runs[1].skip_reason is SkipReason.TASK_CONDITION_NOT_MET
        assert runs[2].skip_reason is SkipReason.TASK_CONDITION_NOT_MET


@pytest.mark.integration
def test_task_condition_implicit_dependence_grouping(tmp_path):
    """Task task condition where there is already a parameter-dependence between the
    tasks, and we group from multiple elements to a single element."""

    s1 = hf.TaskSchema(  # parse p1 as a boolean and map to p2
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command("echo <<parameter:p1>>", stdout="<<bool(parameter:p2)>>")
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
                    hf.Command('echo "<<parameter:p2>>"', stdout="<<parameter:p3>>")
                ]
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        path=tmp_path,
        # config={"log_file_level": "debug"},
        # resources={"any": {"write_app_logs": True}},
        tasks=[
            hf.Task(
                schema=s1,
                sequences=[hf.ValueSequence("inputs.p1", values=[True, False])],
                groups=[hf.ElementGroup(name="my_group")],
            ),
            hf.Task(
                schema=s2,
                condition="task.t1.outputs.p2:all(my_group)",
            ),
        ],
    )

    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_all_EARs()
    assert wk.num_EARs == 3

    # task t1:
    assert runs[0].get_dependent_EARs() == {2}
    assert runs[1].get_dependent_EARs() == {2}

    # task t2:
    assert runs[2].get_EAR_dependencies() == {0, 1}

    assert runs[0].get("outputs.p2") is True
    assert runs[1].get("outputs.p2") is False

    assert runs[2].skip_reason is SkipReason.TASK_CONDITION_NOT_MET


@pytest.mark.integration
@pytest.mark.parametrize(
    "p1_values, group_func",
    [
        ([True, False], "all"),
        ([True, True], "all"),
        ([False, False], "all"),
        ([True, False], "any"),
    ],
)
def test_task_condition_no_dependence_grouping(tmp_path, p1_values, group_func):
    """Task task condition where we need to add a parameter-dependence between the
    tasks, and we group from multiple elements to a single element."""

    s1 = hf.TaskSchema(  # parse p1 as a boolean and map to p2
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command("echo <<parameter:p1>>", stdout="<<bool(parameter:p2)>>")
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("p3", group="my_group")],  # no dependence on t1
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command('echo "<<parameter:p3>>"', stdout="<<parameter:p4>>")
                ]
            )
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        # config={"log_file_level": "debug"},
        # resources={"any": {"write_app_logs": True}},
        path=tmp_path,
        tasks=[
            hf.Task(
                schema=s1,
                sequences=[hf.ValueSequence("inputs.p1", values=p1_values)],
                groups=[hf.ElementGroup(name="my_group")],
            ),
            hf.Task(
                schema=s2,
                inputs={"p3": 301},
                condition=f"task.t1.outputs.p2:{group_func}(my_group)",
            ),
        ],
    )

    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_all_EARs()
    assert wk.num_EARs == 3

    # task t1:
    assert runs[0].get_dependent_EARs() == {2}
    assert runs[1].get_dependent_EARs() == {2}

    # task t2:
    assert runs[2].get_EAR_dependencies() == {0, 1}

    assert runs[0].get("outputs.p2") is p1_values[0]
    assert runs[1].get("outputs.p2") is p1_values[1]

    if group_func == "all":
        if all(p1_values):
            assert runs[2].skip_reason is SkipReason.NOT_SKIPPED
        else:
            assert runs[2].skip_reason is SkipReason.TASK_CONDITION_NOT_MET
    elif group_func == "any":
        if any(p1_values):
            assert runs[2].skip_reason is SkipReason.NOT_SKIPPED
        else:
            assert runs[2].skip_reason is SkipReason.TASK_CONDITION_NOT_MET


@pytest.mark.integration
def test_task_condition_implicit_dependence_loop(tmp_path):
    """Test task condition where there is already a parameter-dependence between the
    tasks."""
    if os.name == "nt":
        cmd_2 = "echo $((<<parameter:p1>> + 1) -lt 3 -or (<<parameter:p1>> + 1) -gt 4)"
    else:
        cmd_2 = "echo $(((<<parameter:p1>> + 1) < 3 || (<<parameter:p1>> + 1) > 4))"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p2"), hf.SchemaInput("is_accept")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $((<<parameter:p1>> + 1))", stdout="<<int(parameter:p2)>>"
                    ),
                    hf.Command(cmd_2, stdout="<<bool(parameter:is_accept)>>"),
                ]
            ),
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
                        "echo $((<<parameter:p2>>))", stdout="<<int(parameter:p1)>>"
                    )
                ]
            )
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        # path=tmp_path,
        config={"log_file_level": "debug"},
        resources={"any": {"write_app_logs": True}},
        tasks=[
            hf.Task(schema=s1, sequences=[hf.ValueSequence("inputs.p1", values=[0, 1])]),
            hf.Task(
                schema=s2,
                condition="task.t1.outputs.is_accept",
            ),
        ],
        loops=[
            hf.Loop(name="loop_0", tasks=[0, 1], num_iterations=3),
        ],
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_all_EARs()
    assert wk.num_EARs == 12

    # task t1
    # iter 0:
    assert runs[0].success and runs[0].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[0].get("outputs.is_accept")
    assert runs[1].success and runs[1].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[1].get("outputs.is_accept")
    # iter 1:
    assert runs[4].success and runs[4].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[4].get("outputs.is_accept")
    assert runs[5].success and runs[5].skip_reason is SkipReason.NOT_SKIPPED
    assert not runs[5].get("outputs.is_accept")
    # iter 2:
    assert runs[8].success and runs[8].skip_reason is SkipReason.NOT_SKIPPED
    assert not runs[8].get("outputs.is_accept")
    # fails because previous iteration of task t2 was not run:
    assert not runs[9].success and runs[9].skip_reason is SkipReason.NOT_SKIPPED
    assert not runs[8].get("outputs.is_accept")

    # task t2
    # iter 0:
    assert runs[2].success and runs[2].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[3].success and runs[3].skip_reason is SkipReason.NOT_SKIPPED
    # iter 1:
    assert runs[6].success and runs[6].skip_reason is SkipReason.NOT_SKIPPED
    assert (
        not runs[7].success and runs[7].skip_reason is SkipReason.TASK_CONDITION_NOT_MET
    )
    # iter 2:
    assert (
        not runs[10].success and runs[10].skip_reason is SkipReason.TASK_CONDITION_NOT_MET
    )
    assert (
        not runs[11].success and runs[11].skip_reason is SkipReason.TASK_CONDITION_NOT_MET
    )


@pytest.mark.integration
def test_task_condition_delayed_acceptance_subset_simulation(tmp_path):
    """Test a workflow that is structurally identical to the delayed acceptance
    subset-simulation variant in MatFlow."""

    s0 = hf.TaskSchema(
        objective="t0",
        inputs=[hf.SchemaInput("p0", group="my_group")],
        outputs=[hf.SchemaInput("p0"), hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                script="<<script:subset_sim_delayed_acceptance_collate.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
        ],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1"), hf.SchemaInput("x_0")],
        outputs=[hf.SchemaOutput("x")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $((<<parameter:p1>> + <<parameter:x_0>> + 1))",
                        stdout="<<int(parameter:x)>>",
                    )
                ]
            )
        ],
    )
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput("x")],
        outputs=[hf.SchemaOutput("g")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $((<<parameter:x>> + 1))", stdout="<<int(parameter:g)>>"
                    )
                ]
            )
        ],
    )

    if os.name == "nt":
        cmd_accept = "echo $(<<parameter:g[coarse]>> -eq 14 -or <<parameter:g[coarse]>> -eq 27 -or <<parameter:g[coarse]>> -eq 43 -or <<parameter:g[coarse]>> -eq 81)"
    else:
        cmd_accept = "echo $((<<parameter:g[coarse]>> == 14 || <<parameter:g[coarse]>> == 27 || <<parameter:g[coarse]>> == 43 || <<parameter:g[coarse]>> == 81))"
    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput("g", labels={"coarse": {}})],
        outputs=[hf.SchemaOutput("is_accept")],
        actions=[
            hf.Action(
                commands=[hf.Command(cmd_accept, stdout="<<bool(parameter:is_accept)>>")]
            )
        ],
    )
    s4 = hf.TaskSchema(
        objective="t4",
        inputs=[hf.SchemaInput("x")],
        outputs=[hf.SchemaOutput("g")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $((<<parameter:x>> + 2))", stdout="<<int(parameter:g)>>"
                    )
                ]
            )
        ],
    )
    s5 = hf.TaskSchema(
        objective="t5",
        inputs=[
            hf.SchemaInput("p0"),
            hf.SchemaInput("g", multiple=True, labels={"coarse": {}}),
            hf.SchemaInput(
                "g",
                multiple=True,
                labels={"fine": {"allow_failed_dependencies": 1.0}},
                default_value=None,
            ),
        ],
        outputs=[hf.SchemaOutput("x_0"), hf.SchemaOutput("p0")],
        actions=[
            hf.Action(
                script="<<script:subset_sim_delayed_acceptance.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            ),
        ],
    )

    wk = hf.Workflow.from_template_data(
        template_name="test_subset_sim_delayed_acceptance_structure",
        path=tmp_path,
        # config={"log_file_level": "debug"},
        # resources={"any": {"write_app_logs": True}},
        tasks=[
            hf.Task(schema=s0, inputs={"p0": 0}),
            hf.Task(schema=s1, sequences=[hf.ValueSequence("inputs.x_0", values=[0, 1])]),
            hf.Task(
                schema=s2, output_labels=[hf.OutputLabel(parameter="g", label="coarse")]
            ),
            hf.Task(schema=s3),
            hf.Task(
                schema=s4,
                output_labels=[hf.OutputLabel(parameter="g", label="fine")],
                condition="tasks.t3.outputs.is_accept",
                resources={"any": {"skip_downstream_on_failure": False}},
            ),
            hf.Task(
                schema=s5,
                groups=[hf.ElementGroup(name="my_group")],
            ),
        ],
        loops=[
            hf.Loop(name="markov_chain", tasks=[1, 2, 3, 4, 5], num_iterations=3),
            hf.Loop(name="subset", tasks=[0, 1, 2, 3, 4, 5], num_iterations=2),
        ],
    )
    wk.submit(wait=True, status=False, add_to_known=False)

    for elem in wk.tasks.t3.elements:
        for iter_i in elem.iterations:
            loop_idx = iter_i.loop_idx
            for run_j in iter_i.action_runs:
                if (
                    elem.index == 0
                    and loop_idx["subset"] == 0
                    and loop_idx["markov_chain"] == 1
                    or elem.index == 1
                    and loop_idx["subset"] == 0
                    and loop_idx["markov_chain"] == 2
                    or elem.index == 0
                    and loop_idx["subset"] == 1
                    and loop_idx["markov_chain"] == 0
                    or elem.index == 1
                    and loop_idx["subset"] == 1
                    and loop_idx["markov_chain"] == 2
                ):
                    assert run_j.get("outputs.is_accept")
                else:
                    assert not run_j.get("outputs.is_accept")

    runs = wk.get_all_EARs()
    non_success_runs = [run for run in runs if not run.success]
    for run in non_success_runs:
        assert run.skip_reason is SkipReason.TASK_CONDITION_NOT_MET

    for elem in wk.tasks.t5.elements:
        for iter_i in elem.iterations:
            for act_run in iter_i.action_runs:
                assert act_run.get("outputs.x_0") is not None
