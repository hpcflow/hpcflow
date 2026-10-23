from __future__ import annotations

import os

import pytest

from pathlib import Path
from hpcflow.app import app as hf
from hpcflow.sdk.core.actions import SkipReason
from hpcflow.sdk.core.test_utils import make_workflow_to_run_command


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_execute_directory_is_prepared_on_demand(tmp_path: Path, store: str):
    source = tmp_path / "input.txt"
    source.write_text("original", encoding="utf-8")
    file_spec = hf.FileSpec(label="input", name="input.txt")
    schema = hf.TaskSchema(
        objective="lazy_directory",
        actions=[
            hf.Action(
                commands=[hf.Command("echo <<file:input>>")],
                input_file_generators=[
                    hf.InputFileGenerator(input_file=file_spec, inputs=[])
                ],
                requires_dir=True,
            )
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="lazy_directory",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schema=schema,
                input_files=[hf.InputFile(file_spec, path=source)],
            )
        ],
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    directory = run.get_directory()
    assert directory is not None
    assert not directory.exists()

    assert run.prepare_directory() == directory
    staged = directory / source.name
    assert staged.read_text() == "original"
    staged.write_text("modified", encoding="utf-8")
    run.prepare_directory()
    assert staged.read_text() == "modified"


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("final_requires_dir", [False, True])
@pytest.mark.parametrize("exit_code", [0, 1])
def test_final_action_execute_directory_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bulk: bool,
    final_requires_dir: bool,
    exit_code: int,
):
    schema = hf.TaskSchema(
        objective="directory_cleanup",
        actions=[
            hf.Action(commands=[hf.Command("echo first")], requires_dir=True),
            hf.Action(
                commands=[hf.Command("echo final")],
                requires_dir=final_requires_dir,
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="directory_cleanup",
        path=tmp_path,
        tasks=[hf.Task(schema=schema)],
    )
    wk.add_submission()
    runs = wk.get_all_EARs()
    directory = runs[0].prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(runs[0].id_, directory, None)
    (directory / "intermediate.txt").write_text("keep until final action")
    wk.set_EAR_end((0, 0, 0), runs[0], 0)
    assert directory.is_dir()

    final_dir = runs[1].prepare_directory()
    if bulk:
        wk.set_multi_run_starts([runs[1].id_], [final_dir], 1234)
        wk.set_multi_run_ends({(0, 0, 1): [(runs[1], exit_code, final_dir)]})
    else:
        wk.set_EAR_start(runs[1].id_, final_dir, None)
        wk.set_EAR_end((0, 0, 1), runs[1], exit_code)
    assert directory.exists() is (exit_code != 0)
    first = wk.get_EARs_from_IDs([runs[0].id_])[0]
    assert first.dir_diff is not None
    assert "intermediate.txt" in first.dir_diff.files_created


def test_execute_directory_retained_after_missing_saved_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    schema = hf.TaskSchema(
        objective="missing_file",
        actions=[
            hf.Action(
                commands=[hf.Command("echo ready")],
                requires_dir=True,
                output_files=[hf.FileSpec(label="missing", name="missing.txt")],
            )
        ],
        save_files=["missing"],
    )
    wk = hf.Workflow.from_template_data(
        template_name="missing_file", path=tmp_path, tasks=[hf.Task(schema=schema)]
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    directory = run.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(run.id_, directory, None)
    wk.set_EAR_end((0, 0, 0), run, 0)
    assert directory.is_dir()
    assert not wk.get_EARs_from_IDs([run.id_])[0].success


def test_skipped_run_does_not_create_execute_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    wk = make_workflow_to_run_command("echo skipped", path=tmp_path, requires_dir=True)
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    directory = run.get_directory()
    assert directory is not None
    wk.set_EAR_skip({run.id_: SkipReason.UPSTREAM_FAILURE})
    monkeypatch.chdir(tmp_path)
    hf.JobscriptExecutor(wk, 0, 0).execute()
    assert not directory.exists()


def test_execute_directory_cleanup_is_per_element_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    schema = hf.TaskSchema(
        objective="separate_directories",
        actions=[hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)],
    )
    wk = hf.Workflow.from_template_data(
        template_name="separate_directories",
        path=tmp_path,
        tasks=[hf.Task(schema=schema, repeats=2)],
        loops=[hf.Loop(tasks=[0], num_iterations=2)],
    )
    wk.add_submission()
    runs = wk.get_all_EARs()
    directories = [run.prepare_directory() for run in runs]
    assert len(set(directories)) == 4
    for index, (run, directory) in enumerate(zip(runs, directories)):
        assert directory is not None
        monkeypatch.chdir(directory)
        wk.set_EAR_start(run.id_, directory, None)
        wk.set_EAR_end((0, 0, 0), run, 0)
        assert not directory.exists()
        assert all(
            path is not None and path.is_dir() for path in directories[index + 1 :]
        )


def test_compose_commands_no_shell_var(tmp_path: Path):
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
    _, shell_vars = run.compose_commands(environments=sub.environments, shell=js.shell)
    assert shell_vars == {0: []}


def test_compose_commands_single_shell_var(tmp_path: Path):
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
    _, shell_vars = run.compose_commands(environments=sub.environments, shell=js.shell)
    assert shell_vars == {0: [("outputs.p1", "parameter_p1", "stdout")]}


def test_compose_commands_multi_single_shell_var(tmp_path: Path):
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
    _, shell_vars = run.compose_commands(environments=sub.environments, shell=js.shell)
    assert shell_vars == {0: [], 1: [("outputs.p1", "parameter_p1", "stdout")]}


@pytest.mark.integration
def test_run_dir_diff_new_file(tmp_path):
    if os.name == "nt":
        command = "New-Item -Path 'new_file.txt' -ItemType File"
    else:
        command = "touch new_file.txt"
    wk = make_workflow_to_run_command(
        command=command,
        requires_dir=True,
        path=tmp_path,
        name="w2",
        overwrite=True,
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    assert wk.get_all_EARs()[0].dir_diff.files_created == ["new_file.txt"]


@pytest.mark.integration
def test_run_skip_reason_upstream_failure(tmp_path):
    ts = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $(( <<parameter:p1>> + 100 ))",
                        stdout="<<parameter:p2>>",
                    ),
                    hf.Command(command="exit 1"),
                ]
            ),
            hf.Action(
                commands=[
                    hf.Command(
                        command="echo $(( <<parameter:p2>> + 100 ))",
                        stdout="<<parameter:p2>>",
                    ),
                ]
            ),  # should be skipped due to failure of action 0
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_skip_reason",
        path=tmp_path,
        tasks=[hf.Task(schema=ts, inputs={"p1": 100})],
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert not runs[0].success
    assert not runs[1].success
    assert runs[0].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[1].skip_reason is SkipReason.UPSTREAM_FAILURE


@pytest.mark.integration
def test_run_skip_reason_loop_termination(tmp_path):
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
        template_name="test_skip_reason",
        path=tmp_path,
        tasks=[hf.Task(schema=ts, inputs={"p1": 100})],
        loops=[
            hf.Loop(name="my_loop", tasks=[0], termination=loop_term, num_iterations=3)
        ],
    )
    # loop should terminate after the second iteration
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()

    assert runs[0].get("outputs.p1") == 200
    assert runs[1].get("outputs.p1") == 300
    assert not runs[2].get("outputs.p1")

    assert runs[0].success
    assert runs[1].success
    assert not runs[2].success

    assert runs[0].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[1].skip_reason is SkipReason.NOT_SKIPPED
    assert runs[2].skip_reason is SkipReason.LOOP_TERMINATION


def test_get_data_in_values_input_files(tmp_path: Path):
    with (file_name := Path("my_file.txt")).open("wt") as fh:
        fh.write("hello!\n")
    file_spec = hf.FileSpec("my_file", str(file_name))
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter="p1"),
        ],
        actions=[
            hf.Action(
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=file_spec,
                        inputs=[hf.Parameter("p1")],
                    )
                ],
            )
        ],
    )
    t1 = hf.Task(schema=[s1], input_files=[hf.InputFile(file_spec, path="my_file.txt")])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="test_get_data_in_values_input_files",
    )
    run = wk.tasks[0].elements[0].iterations[0].action_runs[0]
    assert run.get_data_in_values(("input_files.my_file",)) == {
        "my_file": wk.input_files_path.joinpath("0/my_file.txt").as_posix()
    }


def test_get_data_in_values_user_provided_input_file(tmp_path: Path):

    # pass an input file so the IFG doesn't need to run
    with (file_name := Path("my_file.txt")).open("wt") as fh:
        fh.write("hello!\n")

    file_spec = hf.FileSpec("my_file", str(file_name))
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter="p1"),
        ],
        actions=[
            hf.Action(
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=file_spec,
                        inputs=[hf.Parameter("p1")],
                    )
                ],
            )
        ],
    )
    t1 = hf.Task(schema=[s1], input_files=[hf.InputFile(file_spec, path="my_file.txt")])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="test_get_data_in_values_input_files",
    )
    run = wk.tasks[0].elements[0].iterations[0].action_runs[0]
    assert run.get_data_in_values(("input_files.my_file",)) == {
        "my_file": wk.input_files_path.joinpath("0/my_file.txt").as_posix()
    }


def test_get_data_in_values_input_file_to_script(tmp_path: Path):
    # pass an input file so the IFG doesn't need to run
    with (file_name := Path("my_file.txt")).open("wt") as fh:
        fh.write("hello!\n")

    file_spec = hf.FileSpec("my_file", str(file_name))
    act = hf.Action(
        input_file_generators=[
            hf.InputFileGenerator(
                input_file=file_spec,
                inputs=[hf.Parameter("p1")],
            )
        ],
        script_data_in={"input_files.my_file": "direct"},
    )
    s1 = hf.TaskSchema(
        objective="ts1",
        inputs=[hf.SchemaInput("p1")],
        actions=[act],
    )
    t1 = hf.Task(schema=[s1], input_files=[hf.InputFile(file_spec, path="my_file.txt")])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        path=tmp_path,
        template_name="test_get_data_in_values_input_files",
    )
    act_runs = wk.tasks[0].elements[0].iterations[0].action_runs
    assert len(act_runs) == 1
    assert act_runs[0].get_data_in_values(("input_files.my_file",)) == {
        "my_file": wk.input_files_path.joinpath("0/my_file.txt").as_posix()
    }
