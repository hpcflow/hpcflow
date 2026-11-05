from __future__ import annotations

import os
import errno

import pytest

from pathlib import Path
from hpcflow.app import app as hf
from hpcflow.sdk.core.actions import SkipReason
from hpcflow.sdk.core.test_utils import make_workflow_to_run_command


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("scratch", [False, True])
def test_execute_directory_is_prepared_on_demand(
    tmp_path: Path, store: str, scratch: bool
):
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
        resources={"any": {"scratch": str(tmp_path / "scratch") if scratch else None}},
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
    if scratch:
        assert directory.is_relative_to(tmp_path / "scratch")
        assert not (tmp_path / "scratch").exists()
    else:
        assert directory == wk.execution_path

    assert run.prepare_directory() == directory
    staged = directory / source.name
    assert staged.read_text() == "original"
    staged.write_text("modified", encoding="utf-8")
    run.prepare_directory()
    assert staged.read_text() == "modified"


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_scratch_temporary_file_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: str
) -> None:
    file_spec = hf.FileSpec(label="input", name="input.txt")
    schema = hf.TaskSchema(
        objective="scratch_file_reference",
        actions=[
            hf.Action(
                commands=[hf.Command("echo final")],
                input_file_generators=[
                    hf.InputFileGenerator(input_file=file_spec, inputs=[])
                ],
                requires_dir=True,
            ),
        ],
    )
    scratch = tmp_path / "scratch"
    wk = hf.Workflow.from_template_data(
        template_name="scratch_file_reference",
        path=tmp_path,
        store=store,
        resources={"any": {"scratch": str(scratch)}},
        tasks=[hf.Task(schema=schema)],
    )
    wk.add_submission()
    first, final = wk.get_all_EARs()
    directory = first.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(first.id_, directory, None)
    (directory / "input.txt").write_text("generated", encoding="utf-8")
    wk.set_EAR_end((0, 0, 0), first, 0)
    wk = hf.Workflow(wk.path)
    first, final = wk.get_all_EARs()
    assert Path(final.get("input_files.input")) == directory / "input.txt"
    assert final.prepare_directory() == directory
    assert (directory / "input.txt").read_text() == "generated"
    wk.set_EAR_start(final.id_, directory, None)
    wk.set_EAR_end((0, 0, 1), final, 0)
    assert not directory.exists()
    assert scratch.exists()
    assert not directory.parent.exists()
    assert not wk.task_artifacts_path.exists()
    final = wk.get_EARs_from_IDs(final.id_)
    assert final.get("input_files.input") is None


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("final_requires_dir", [False, True])
@pytest.mark.parametrize("exit_code", [0, 1])
@pytest.mark.parametrize("scratch", [False, True])
@pytest.mark.parametrize("move_scratch", [False, True])
def test_final_action_execute_directory_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bulk: bool,
    final_requires_dir: bool,
    exit_code: int,
    scratch: bool,
    move_scratch: bool,
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
        resources={"any": {"scratch": str(tmp_path / "scratch") if scratch else None}},
        tasks=[hf.Task(schema=schema)],
        config={"move_scratch_to_workflow": move_scratch},
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
    relocated = scratch and move_scratch and exit_code != 0
    assert directory.exists() is (exit_code != 0 and not relocated)
    if scratch:
        assert (tmp_path / "scratch").is_dir()
        assert directory.parent.exists() is (exit_code != 0 and not relocated)
    if relocated:
        retained = wk.get_run_directories(run_ids=[runs[0].id_])[0]
        assert retained is not None and retained.is_relative_to(wk.execution_path)
        assert (retained / "intermediate.txt").read_text() == "keep until final action"
    first = wk.get_EARs_from_IDs([runs[0].id_])[0]
    assert first.dir_diff is not None
    assert "intermediate.txt" in first.dir_diff.files_created


def _mock_cross_device_rename(
    monkeypatch: pytest.MonkeyPatch, directory: Path
) -> list[Path]:
    original_rename = Path.rename
    attempts: list[Path] = []

    def rename(source: Path, destination: str | os.PathLike[str]) -> Path:
        if source == directory:
            attempts.append(source)
            raise OSError(errno.EXDEV, "Cross-device link")
        return original_rename(source, destination)

    monkeypatch.setattr(Path, "rename", rename)
    return attempts


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("exit_code", [0, 1])
def test_retained_scratch_moves_contents_and_file_references(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: str, exit_code: int
) -> None:
    file_spec = hf.FileSpec(label="input", name="input.txt")
    schema = hf.TaskSchema(
        objective="retained_scratch",
        actions=[
            hf.Action(
                commands=[hf.Command("echo final")],
                input_file_generators=[
                    hf.InputFileGenerator(input_file=file_spec, inputs=[])
                ],
                requires_dir=True,
            ),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="retained_scratch",
        path=tmp_path,
        store=store,
        resources={"any": {"scratch": str(tmp_path / "scratch")}},
        tasks=[hf.Task(schema=schema)],
        config={"delete_execute_dir": False},
    )
    wk.add_submission()
    first, final = wk.get_all_EARs()
    directory = first.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(first.id_, directory, None)
    (directory / "input.txt").write_text("generated", encoding="utf-8")
    (directory / "nested").mkdir()
    (directory / "nested" / ".undeclared").write_bytes(b"\x00\x01\xff")
    rename_attempts = _mock_cross_device_rename(monkeypatch, directory)
    wk.set_EAR_end((0, 0, 0), first, 0)
    assert directory.exists()
    final = wk.get_EARs_from_IDs(final.id_)
    assert Path(final.get("input_files.input")) == directory / "input.txt"
    wk.set_EAR_start(final.id_, directory, None)
    wk.set_EAR_end((0, 0, 1), final, exit_code)

    assert rename_attempts == [directory]
    wk = hf.Workflow(wk.path)
    first, final = wk.get_all_EARs()
    relocated = first.get_directory()
    assert relocated is not None and relocated.is_relative_to(wk.execution_path)
    assert final.get_directory() == relocated
    assert Path(final.get("input_files.input")) == relocated / "input.txt"
    assert (relocated / "input.txt").read_text() == "generated"
    assert (relocated / "nested" / ".undeclared").read_bytes() == b"\x00\x01\xff"
    assert not directory.exists()
    assert list((tmp_path / "scratch").iterdir()) == []
    assert first.success
    assert final.success is (exit_code == 0)


@pytest.mark.parametrize("bulk", [False, True])
@pytest.mark.parametrize("skip_downstream", [False, True])
def test_failed_scratch_waits_for_remaining_actions(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    bulk: bool,
    skip_downstream: bool,
) -> None:
    schema = hf.TaskSchema(
        objective="failed_scratch",
        actions=[
            hf.Action(commands=[hf.Command("echo first")], requires_dir=True),
            hf.Action(commands=[hf.Command("echo final")], requires_dir=True),
        ],
    )
    wk = hf.Workflow.from_template_data(
        template_name="failed_scratch",
        path=tmp_path,
        resources={
            "any": {
                "scratch": str(tmp_path / "scratch"),
                "skip_downstream_on_failure": skip_downstream,
            }
        },
        tasks=[hf.Task(schema=schema)],
    )
    wk.add_submission()
    first, final = wk.get_all_EARs()
    directory = first.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(first.id_, directory, None)
    (directory / "failure.log").write_text("diagnostic", encoding="utf-8")
    if skip_downstream:
        wk.set_EAR_skip({final.id_: SkipReason.UPSTREAM_FAILURE})
    if bulk:
        wk.set_multi_run_ends({(0, 0, 0): [(first, 1, directory)]})
    else:
        wk.set_EAR_end((0, 0, 0), first, 1)
    assert directory.exists() is (not skip_downstream)
    if not skip_downstream:
        final = wk.get_EARs_from_IDs(final.id_)
        assert final.prepare_directory() == directory
        wk.set_EAR_start(final.id_, directory, None)
        if bulk:
            wk.set_multi_run_ends({(0, 0, 1): [(final, 0, directory)]})
        else:
            wk.set_EAR_end((0, 0, 1), final, 0)
    wk = hf.Workflow(wk.path)
    relocated = wk.get_run_directories()[0]
    assert relocated is not None and relocated.is_relative_to(wk.execution_path)
    assert (relocated / "failure.log").read_text() == "diagnostic"
    assert not directory.exists()
    assert list((tmp_path / "scratch").iterdir()) == []


@pytest.mark.parametrize("error", ["conflict", "copy"])
def test_scratch_move_reports_errors_without_removing_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, error: str
) -> None:
    wk = hf.Workflow.from_template_data(
        template_name="scratch_move_error",
        path=tmp_path,
        resources={"any": {"scratch": str(tmp_path / "scratch")}},
        tasks=[
            hf.Task(
                schema=hf.TaskSchema(
                    objective="scratch_move_error",
                    actions=[
                        hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)
                    ],
                )
            )
        ],
        config={"delete_execute_dir": False},
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    directory = run.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(run.id_, directory, None)
    (directory / "diagnostic.txt").write_text("keep", encoding="utf-8")
    rename_attempts: list[Path] = []
    if error == "conflict":
        wk.execution_path.mkdir(parents=True)
        (wk.execution_path / "existing.txt").write_text("existing", encoding="utf-8")
        expected: type[OSError] = FileExistsError
    else:
        import shutil

        def deny_copy(*args, **kwargs):
            raise PermissionError(errno.EACCES, "Copy denied")

        rename_attempts = _mock_cross_device_rename(monkeypatch, directory)
        monkeypatch.setattr(shutil, "copytree", deny_copy)
        expected = PermissionError
    with pytest.raises(expected):
        wk.set_EAR_end((0, 0, 0), run, 0)
    assert (directory / "diagnostic.txt").read_text() == "keep"
    assert hf.Workflow(wk.path).get_run_directories() == [directory]
    if error == "conflict":
        assert (wk.execution_path / "existing.txt").read_text() == "existing"
    else:
        assert rename_attempts == [directory]


@pytest.mark.parametrize("application_value", [None, False, True])
@pytest.mark.parametrize("workflow_value", [None, False, True])
@pytest.mark.parametrize("exit_code", [0, 1])
def test_scratch_move_config_fallback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    modifiable_config,
    application_value: bool | None,
    workflow_value: bool | None,
    exit_code: int,
) -> None:
    key = "move_scratch_to_workflow"
    if application_value is None:
        hf.config.unset(key)
    else:
        hf.config.set(key, application_value)
    hf.config.save()
    config_directory = hf.config.config_directory
    hf.reload_config(config_dir=config_directory, warn=False)
    expected_application = True if application_value is None else application_value
    assert hf.config.get(key) is expected_application
    config_path = hf.config.config_file_path
    original_config = config_path.read_bytes()

    wk = hf.Workflow.from_template_data(
        template_name="scratch_config_fallback",
        path=tmp_path,
        resources={"any": {"scratch": str(tmp_path / "scratch")}},
        tasks=[
            hf.Task(
                schema=hf.TaskSchema(
                    objective="scratch_config_fallback",
                    actions=[
                        hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)
                    ],
                )
            )
        ],
        config={
            "delete_execute_dir": exit_code != 0,
            **({key: workflow_value} if workflow_value is not None else {}),
        },
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    directory = run.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(run.id_, directory, None)
    (directory / "diagnostic.txt").write_text("keep", encoding="utf-8")
    wk.set_EAR_end((0, 0, 0), run, exit_code)

    expected = workflow_value if workflow_value is not None else expected_application
    move = expected is not False
    retained = hf.Workflow(wk.path).get_run_directories()[0]
    assert retained is not None
    assert retained.is_relative_to(wk.execution_path) is move
    assert directory.exists() is (not move)
    assert (retained / "diagnostic.txt").read_text() == "keep"
    assert config_path.read_bytes() == original_config
    assert hf.config.get(key) is expected_application


@pytest.mark.parametrize("bulk", [False, True])
def test_retained_scratch_move_is_per_element_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, bulk: bool
) -> None:
    wk = hf.Workflow.from_template_data(
        template_name="scratch_iteration_move",
        path=tmp_path,
        resources={"any": {"scratch": str(tmp_path / "scratch")}},
        tasks=[
            hf.Task(
                schema=hf.TaskSchema(
                    objective="scratch_iteration_move",
                    actions=[
                        hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)
                    ],
                ),
                repeats=2,
            )
        ],
        config={"delete_execute_dir": False},
    )
    wk.add_submission()
    runs = wk.get_all_EARs()
    directories = [run.prepare_directory() for run in runs]
    for idx, (run, directory) in enumerate(zip(runs, directories)):
        assert directory is not None
        monkeypatch.chdir(directory)
        wk.set_EAR_start(run.id_, directory, None)
        (directory / "diagnostic.txt").write_text(str(idx), encoding="utf-8")
    if bulk:
        wk.set_multi_run_ends(
            {(0, 0, 0): [(run, 0, path) for run, path in zip(runs, directories)]}
        )
    else:
        first_dir = directories[0]
        assert first_dir is not None
        monkeypatch.chdir(first_dir)
        wk.set_EAR_end((0, 0, 0), runs[0], 0)
        assert directories[1] is not None and directories[1].exists()
        monkeypatch.chdir(directories[1])
        wk.set_EAR_end((0, 0, 0), runs[1], 0)
    wk = hf.Workflow(wk.path)
    for idx, directory in enumerate(wk.get_run_directories()):
        assert directory is not None and directory.is_relative_to(wk.execution_path)
        assert (directory / "diagnostic.txt").read_text() == str(idx)
    assert list((tmp_path / "scratch").iterdir()) == []


@pytest.mark.parametrize("scratch", [False, True])
def test_execute_directory_retained_after_missing_saved_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch: bool
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
        template_name="missing_file",
        path=tmp_path,
        tasks=[hf.Task(schema=schema)],
        resources={"any": {"scratch": str(tmp_path / "scratch") if scratch else None}},
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    directory = run.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(run.id_, directory, None)
    wk.set_EAR_end((0, 0, 0), run, 0)
    retained = wk.get_run_directories()[0]
    assert retained is not None and retained.is_dir()
    if scratch:
        assert retained.is_relative_to(wk.execution_path)
        assert not directory.exists()
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


@pytest.mark.parametrize("scratch", [False, True])
def test_execute_directory_cleanup_is_per_element_iteration(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, scratch: bool
):
    schema = hf.TaskSchema(
        objective="separate_directories",
        actions=[hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)],
    )
    wk = hf.Workflow.from_template_data(
        template_name="separate_directories",
        path=tmp_path,
        resources={"any": {"scratch": str(tmp_path / "scratch") if scratch else None}},
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
    if scratch:
        assert list((tmp_path / "scratch").iterdir()) == []


@pytest.mark.parametrize("exit_code", [0, 1])
def test_scratch_pruning_preserves_other_workflows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, exit_code: int
) -> None:
    scratch = tmp_path / "scratch"
    other_workflow = scratch / "other_workflow"
    other_workflow.mkdir(parents=True)
    sentinel = other_workflow / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    schema = hf.TaskSchema(
        objective="scratch_cleanup",
        actions=[hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)],
    )
    wk = hf.Workflow.from_template_data(
        template_name="scratch_cleanup",
        path=tmp_path,
        resources={"any": {"scratch": str(scratch)}},
        tasks=[hf.Task(schema=schema)],
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    directory = run.prepare_directory()
    assert directory is not None
    monkeypatch.chdir(directory)
    wk.set_EAR_start(run.id_, directory, None)
    wk.set_EAR_end((0, 0, 0), run, exit_code)
    assert not directory.parent.exists()
    assert sentinel.read_text() == "keep"


@pytest.mark.parametrize("missing", [False, True])
def test_prune_empty_scratch_dirs(tmp_path: Path, missing: bool) -> None:
    root = tmp_path / "workflow"
    leaf = root / "execute" / "t_0" / "e_0"
    leaf.mkdir(parents=True)
    if missing:
        leaf.rmdir()
    hf.Workflow._prune_empty_scratch_dirs(leaf, root)
    assert not root.exists()
    assert tmp_path.is_dir()


def test_prune_scratch_dirs_stops_at_nonempty_directory(tmp_path: Path) -> None:
    root = tmp_path / "workflow"
    leaf = root / "execute" / "t_0"
    leaf.mkdir(parents=True)
    sentinel = root / "execute" / "keep.txt"
    sentinel.write_text("keep", encoding="utf-8")
    hf.Workflow._prune_empty_scratch_dirs(leaf, root)
    assert not leaf.exists()
    assert sentinel.read_text() == "keep"
    assert root.is_dir()


def test_prepare_directory_retries_after_concurrent_prune(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    wk = make_workflow_to_run_command("echo ready", path=tmp_path, requires_dir=True)
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    original_mkdir = Path.mkdir
    calls = 0

    def flaky_mkdir(self: Path, *args, **kwargs) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise FileNotFoundError(errno.ENOENT, "pruned concurrently", str(self))
        original_mkdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", flaky_mkdir)
    directory = run.prepare_directory()
    assert directory is not None and directory.is_dir()
    assert calls >= 2


def test_prune_scratch_dirs_propagates_unexpected_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def deny_rmdir(self: Path) -> None:
        raise PermissionError(errno.EACCES, "Permission denied", str(self))

    root = tmp_path / "workflow"
    root.mkdir()
    monkeypatch.setattr(Path, "rmdir", deny_rmdir)
    with pytest.raises(PermissionError):
        hf.Workflow._prune_empty_scratch_dirs(root, root)


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
