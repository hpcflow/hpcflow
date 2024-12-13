from __future__ import annotations
from pathlib import Path
from hpcflow.app import app as hf


def test_compose_commands_no_shell_var(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test_compose_commands",
        actions=[hf.Action(commands=[hf.Command(command="Start-Sleep 10")])],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_compose_commands",
        path=tmp_path,
        tasks=[hf.Task(schema=ts)],
    )
    sub = wk.add_submission()
    assert sub is not None
    js = sub.jobscripts[0]
    run = wk.tasks[0].elements[0].iterations[0].action_runs[0]
    _, shell_vars = run.compose_commands(jobscript=js, JS_action_idx=0)
    assert shell_vars == {0: []}


def test_compose_commands_single_shell_var(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test_compose_commands",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    ),
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_compose_commands",
        path=tmp_path,
        tasks=[hf.Task(schema=ts, inputs={"p1": 101})],
    )
    sub = wk.add_submission()
    assert sub is not None
    js = sub.jobscripts[0]
    run = wk.tasks[0].elements[0].iterations[0].action_runs[0]
    _, shell_vars = run.compose_commands(jobscript=js, JS_action_idx=0)
    assert shell_vars == {0: [("outputs.p1", "parameter_p1", "stdout")]}


def test_compose_commands_multi_single_shell_var(null_config, tmp_path: Path):
    ts = hf.TaskSchema(
        objective="test_compose_commands",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command="Start-Sleep 10"),
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<int(parameter:p1)>>",
                    ),
                ],
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_compose_commands",
        path=tmp_path,
        tasks=[hf.Task(schema=ts, inputs={"p1": 101})],
    )
    sub = wk.add_submission()
    assert sub is not None
    js = sub.jobscripts[0]
    run = wk.tasks[0].elements[0].iterations[0].action_runs[0]
    _, shell_vars = run.compose_commands(jobscript=js, JS_action_idx=0)
    assert shell_vars == {0: [], 1: [("outputs.p1", "parameter_p1", "stdout")]}
