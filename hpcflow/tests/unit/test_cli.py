from pathlib import Path
from unittest.mock import Mock
import pytest

from click.testing import CliRunner
import click.exceptions

from hpcflow import __version__
from hpcflow.app import app as hf
from hpcflow.sdk.cli import (
    ErrorPropagatingClickContext,
    _make_API_CLI,
    _make_workflow_CLI,
    _make_zip_CLI,
)
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.cli_common import BoolOrString
from hpcflow.sdk.demo.cli import get_demo_workflow_CLI


@pytest.mark.parametrize("command_name", ["go", "demo-go", "submit"])
@pytest.mark.parametrize("ret_id", [False, True])
@pytest.mark.parametrize("print_idx", [False, True])
def test_cli_submission_ret_id(command_name: str, ret_id: bool, print_idx: bool):
    app = Mock(wraps=hf)
    workflow = Mock(spec=Workflow)
    workflow.path = Path("workflow")
    submitted_js = {0: [0], 1: [1], 2: []}
    app.read_known_submissions_file = Mock(
        return_value=[
            {"path": "other", "sub_idx": 0, "local_id": 99},
            {"path": str(workflow.path), "sub_idx": 0, "local_id": 4},
            {"path": str(workflow.path), "sub_idx": 1, "local_id": 0},
            {"path": str(workflow.path), "sub_idx": 0, "local_id": 7},
            {"path": str(workflow.path), "sub_idx": 2, "local_id": 8},
        ]
    )
    if command_name == "submit":
        command = _make_workflow_CLI(app).commands["submit"]
        submit = workflow.submit
        submit.return_value = submitted_js
        args = []
    else:
        if command_name == "demo-go":
            command = get_demo_workflow_CLI(app).commands["go"]
            submit = app.make_and_submit_demo_workflow = Mock()
        else:
            command = next(cmd for cmd in _make_API_CLI(app) if cmd.name == "go")
            submit = app.make_and_submit_workflow = Mock()
        submit.return_value = (
            (workflow, submitted_js) if ret_id or print_idx else workflow
        )
        args = ["workflow.yaml"]
    args += ["--quiet"]
    if ret_id:
        args.append("--ret-id")
    if print_idx:
        args.append("--print-idx")
    result = CliRunner().invoke(command, args, obj=workflow)
    assert result.exit_code == 0, result.output
    expected = f"{submitted_js}\n" if print_idx else ""
    if ret_id:
        expected += "7\n0\n"
        app.read_known_submissions_file.assert_called_once_with()
    else:
        app.read_known_submissions_file.assert_not_called()
    assert result.output == expected
    assert "ret_id" not in submit.call_args.kwargs


@pytest.mark.parametrize("command_name", ["go", "demo-go", "submit"])
def test_cli_ret_id_requires_known_submission(command_name: str):
    app = Mock(wraps=hf)
    workflow = Mock(spec=Workflow)
    if command_name == "submit":
        command = _make_workflow_CLI(app).commands["submit"]
        submit = workflow.submit
        args = []
    elif command_name == "demo-go":
        command = get_demo_workflow_CLI(app).commands["go"]
        submit = app.make_and_submit_demo_workflow = Mock()
        args = ["workflow.yaml"]
    else:
        command = next(cmd for cmd in _make_API_CLI(app) if cmd.name == "go")
        submit = app.make_and_submit_workflow = Mock()
        args = ["workflow.yaml"]
    result = CliRunner().invoke(
        command, [*args, "--ret-id", "--no-add-to-known"], obj=workflow
    )
    assert result.exit_code == 2
    assert "--ret-id requires --add-to-known" in result.output
    submit.assert_not_called()


@pytest.mark.parametrize("submitted_js", [{}, {0: []}])
def test_cli_ret_id_without_submitted_jobscripts(submitted_js):
    app = Mock(wraps=hf)
    app.read_known_submissions_file = Mock()
    workflow = Mock(spec=Workflow)
    workflow.submit.return_value = submitted_js
    command = _make_workflow_CLI(app).commands["submit"]
    result = CliRunner().invoke(command, ["--ret-id"], obj=workflow)
    assert result.exit_code == 0, result.output
    assert result.output == ""
    app.read_known_submissions_file.assert_not_called()


@pytest.mark.parametrize("option", [[], ["--follow"], ["-f"]])
def test_cli_submit_follow(option: list[str]) -> None:
    workflow = Mock(spec=Workflow)
    command = _make_workflow_CLI(hf).commands["submit"]
    result = CliRunner().invoke(command, option, obj=workflow)
    assert result.exit_code == 0, result.output
    assert workflow.submit.call_args.kwargs["follow"] == bool(option)


@pytest.mark.parametrize("option", [[], ["--delete"]])
def test_cli_submit_delete(option: list[str]) -> None:
    workflow = Mock(spec=Workflow)
    command = _make_workflow_CLI(hf).commands["submit"]
    result = CliRunner().invoke(command, option, obj=workflow)
    assert result.exit_code == 0, result.output
    assert workflow.submit.call_args.kwargs["delete"] == bool(option)


@pytest.mark.parametrize("option", [[], ["--follow"], ["-f"]])
def test_cli_wait_follow(option: list[str]) -> None:
    workflow = Mock(spec=Workflow)
    command = _make_workflow_CLI(hf).commands["wait"]
    result = CliRunner().invoke(
        command, ["--jobscripts", "0:0,1;1:2", *option], obj=workflow
    )
    assert result.exit_code == 0, result.output
    workflow.wait.assert_called_once_with(
        sub_js={0: [0, 1], 1: [2]}, quiet=False, follow=bool(option)
    )


@pytest.mark.parametrize("demo", [False, True])
@pytest.mark.parametrize("option", [[], ["--follow"], ["-f"]])
def test_cli_go_follow(demo: bool, option: list[str]) -> None:
    app = Mock(wraps=hf)
    if demo:
        command = get_demo_workflow_CLI(app).commands["go"]
        submit = app.make_and_submit_demo_workflow = Mock()
    else:
        command = next(cmd for cmd in _make_API_CLI(app) if cmd.name == "go")
        submit = app.make_and_submit_workflow = Mock()
    result = CliRunner().invoke(command, ["workflow.yaml", *option])
    assert result.exit_code == 0, result.output
    assert submit.call_args.kwargs["follow"] == bool(option)


@pytest.mark.parametrize("demo", [False, True])
@pytest.mark.parametrize("option", [[], ["--delete"]])
def test_cli_go_delete(demo: bool, option: list[str]) -> None:
    app = Mock(wraps=hf)
    if demo:
        command = get_demo_workflow_CLI(app).commands["go"]
        submit = app.make_and_submit_demo_workflow = Mock()
    else:
        command = next(cmd for cmd in _make_API_CLI(app) if cmd.name == "go")
        submit = app.make_and_submit_workflow = Mock()
    result = CliRunner().invoke(command, ["workflow.yaml", *option])
    assert result.exit_code == 0, result.output
    assert submit.call_args.kwargs["delete"] == bool(option)


@pytest.mark.parametrize(
    ("options", "task"),
    [
        ([], False),
        (["--run-id", "10", "--run-id", "19"], False),
        (["--task", "3", "--element-idx", "1", "--iteration-idx", "0"], True),
        (["--task", "0"], True),
    ],
)
@pytest.mark.parametrize("sort", [False, True])
@pytest.mark.parametrize("overwrite", [False, True])
def test_cli_export_array_output(
    tmp_path: Path, options: list[str], task: bool, sort: bool, overwrite: bool
):
    workflow = Mock(spec=Workflow)
    destination = tmp_path / "export.npy"
    target = workflow.tasks.get.return_value if task else workflow
    target.export_array_output.return_value = destination
    command = _make_workflow_CLI(hf).commands["export-array-output"]
    result = CliRunner().invoke(
        command,
        [
            "p1.a",
            str(destination),
            *options,
            "--sort" if sort else "--no-sort",
            *(["--overwrite"] if overwrite else []),
        ],
        obj=workflow,
    )
    assert result.exit_code == 0, result.output
    if task:
        workflow.tasks.get.assert_called_once_with(insert_ID=int(options[1]))
        target.export_array_output.assert_called_once_with(
            "p1.a",
            str(destination),
            element_idx=(1,) if len(options) > 2 else None,
            iteration_idx=(0,) if len(options) > 2 else None,
            sort=sort,
            overwrite=overwrite,
        )
    else:
        target.export_array_output.assert_called_once_with(
            "p1.a",
            (10, 19) if options else None,
            str(destination),
            sort=sort,
            overwrite=overwrite,
        )
    assert str(destination) in result.output


@pytest.mark.parametrize(
    "options",
    [
        ["--task", "0", "--run-id", "1"],
        ["--run-id", "1", "--element-idx", "0"],
        ["--run-id", "1", "--iteration-idx", "0"],
    ],
)
def test_cli_export_array_output_invalid_options(tmp_path: Path, options: list[str]):
    workflow = Mock(spec=Workflow)
    command = _make_workflow_CLI(hf).commands["export-array-output"]
    result = CliRunner().invoke(
        command, ["p1.a", str(tmp_path / "export.npy"), *options], obj=workflow
    )
    assert result.exit_code == 2
    workflow.export_array_output.assert_not_called()
    workflow.tasks.get.assert_not_called()


@pytest.mark.parametrize(
    ("args", "names", "status"),
    [
        ([], None, True),
        (["--no-status"], None, False),
        (["p1.a"], ("p1.a",), True),
        (["p1.a", "p2", "--status"], ("p1.a", "p2"), True),
    ],
)
def test_cli_consolidate_array_outputs(
    tmp_path: Path, args: list[str], names: tuple[str, ...] | None, status: bool
):
    workflow = Mock(spec=Workflow)
    workflow.consolidate_array_outputs.return_value = {
        "p1.a": tmp_path / "p1.a" / "consolidated.npz.zst",
        "p1.a.__mask__": tmp_path / "p1.a.__mask__" / "consolidated.npz.zst",
    }
    command = _make_workflow_CLI(hf).commands["consolidate-array-outputs"]
    result = CliRunner().invoke(command, args, obj=workflow)
    assert result.exit_code == 0, result.output
    workflow.consolidate_array_outputs.assert_called_once_with(names, status=status)
    assert result.output.splitlines() == [
        f"p1.a: {tmp_path / 'p1.a' / 'consolidated.npz.zst'}",
        f"p1.a.__mask__: {tmp_path / 'p1.a.__mask__' / 'consolidated.npz.zst'}",
    ]


@pytest.mark.parametrize(
    ("args", "consolidate"),
    [([], True), (["--consolidate"], True), (["--no-consolidate"], False)],
)
@pytest.mark.parametrize("top_level", [False, True])
def test_cli_zip_consolidate_option(
    tmp_path: Path, args: list[str], consolidate: bool, top_level: bool
):
    workflow = Mock(spec=Workflow)
    workflow.zip.return_value = str(tmp_path / "wk.zip")
    if top_level:
        app = Mock(wraps=hf)
        app._resolve_workflow_reference.return_value = str(tmp_path / "wk")
        app.Workflow = Mock(return_value=workflow)
        command = _make_zip_CLI(app)
        result = CliRunner().invoke(command, ["wk", *args])
    else:
        command = _make_workflow_CLI(hf).commands["zip"]
        result = CliRunner().invoke(command, args, obj=workflow)
    assert result.exit_code == 0, result.output
    workflow.zip.assert_called_once_with(
        path=".",
        overwrite=False,
        log=None,
        include_execute=False,
        include_rechunk_backups=False,
        consolidate=consolidate,
    )
    assert str(tmp_path / "wk.zip") in result.output


def test_version(cli_runner) -> None:
    result = cli_runner(["--version"])
    assert result.output.strip() == f"hpcFlow, version {__version__}"


def test_BoolOrString_convert():
    param_type = BoolOrString(["a"])
    assert param_type.convert(True, None, None) == True
    assert param_type.convert(False, None, None) == False
    assert param_type.convert("yes", None, None) == True
    assert param_type.convert("no", None, None) == False
    assert param_type.convert("on", None, None) == True
    assert param_type.convert("off", None, None) == False
    assert param_type.convert("a", None, None) == "a"
    with pytest.raises(click.exceptions.BadParameter):
        param_type.convert("b", None, None)


def test_error_propagated_with_custom_context_class():
    class MyException(ValueError):
        pass

    class MyContextManager:

        # set to True when MyException is raised within this context manager
        raised = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type == MyException:
                self.__class__.raised = True

    @click.group()
    @click.pass_context
    def cli(ctx):
        ctx.with_resource(MyContextManager())

    cli.context_class = ErrorPropagatingClickContext  # use custom click Context

    @cli.command(
        name="my-command"
    )  # explicit, because Click 8.2.0+ removes suffixes like "command" for some reason
    def my_command():
        raise MyException()

    runner = CliRunner()
    runner.invoke(cli, args="my-command")

    assert MyContextManager.raised


def test_error_not_propagated_without_custom_context_class():
    class MyException(ValueError):
        pass

    class MyContextManager:

        # set to True when MyException is raised within this context manager
        raised = False

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type == MyException:
                self.__class__.raised = True

    @click.group()
    @click.pass_context
    def cli(ctx):
        ctx.with_resource(MyContextManager())

    @cli.command()
    def my_command():
        raise MyException()

    runner = CliRunner()
    runner.invoke(cli, args="my-command")

    assert not MyContextManager.raised


def test_std_stream_file_created(tmp_path, cli_runner):
    """Test exception is intercepted and printed to the specified --std-stream file."""
    error_file = tmp_path / "std_stream.txt"
    result = cli_runner(["--std-stream", str(error_file), "internal", "noop", "--raise"])
    assert error_file.is_file()
    std_stream_contents = error_file.read_text()
    assert "ValueError: internal noop raised!" in std_stream_contents
    assert result.exit_code == 1
    assert result.exc_info[0] == SystemExit


def test_std_stream_file_not_created(tmp_path, cli_runner):
    """Test std stream file is not created when no ouput/errors/exceptions"""
    error_file = tmp_path / "std_stream.txt"
    result = cli_runner(["--std-stream", str(error_file), "internal", "noop"])
    assert not error_file.is_file()
    assert result.exit_code == 0


def test_cli_exception(cli_runner):
    """Test exception is passed to click"""
    result = cli_runner(["internal", "noop", "--raise"])
    assert result.exit_code == 1
    assert result.exc_info[0] == ValueError


def test_cli_click_exit_code_zero(tmp_path, cli_runner):
    """Test Click's `Exit` exception is ignored by the `redirect_std_to_file` context manager when the exit code is zero."""
    error_file = tmp_path / "std_stream.txt"
    result = cli_runner(
        ["--std-stream", str(error_file), "internal", "noop", "--click-exit-code", "0"]
    )
    assert result.exit_code == 0
    assert not error_file.is_file()


def test_cli_click_exit_code_non_zero(tmp_path, cli_runner):
    """Test Click's `Exit` exception is not ignored by the `redirect_std_to_file` context manager when the exit code is non-zero."""
    error_file = tmp_path / "std_stream.txt"
    result = cli_runner(
        ["--std-stream", str(error_file), "internal", "noop", "--click-exit-code", "2"]
    )
    assert result.exit_code == 2
    assert error_file.is_file()


def test_cli_make_demo_workflow(tmp_path, cli_runner):
    """Check the demo workflow directory is generated."""
    result = cli_runner(["demo-workflow", "make", "workflow_1", "--path", str(tmp_path)])
    assert result.exit_code == 0
    assert Path(result.stdout_bytes.decode().strip()).is_dir()


def test_cli_make_demo_workflow_add_sub(tmp_path, cli_runner):
    """Check the demo workflow directory is generated, and a submission is added."""
    result = cli_runner(
        [
            "demo-workflow",
            "make",
            "workflow_1",
            "--path",
            str(tmp_path),
            "--add-submission",
        ]
    )
    assert result.exit_code == 0
    wk_path = Path(result.stdout_bytes.decode().strip())
    assert wk_path.is_dir()
    wk = hf.Workflow(wk_path)
    assert len(wk.submissions) == 1


@pytest.mark.skip("Requires casting string types from the CLI")
def test_cli_make_demo_workflow_with_resource_update(tmp_path, cli_runner):
    resources_id = 13123
    result = cli_runner(
        [
            "demo-workflow",
            "make",
            "workflow_1",
            "--path",
            str(tmp_path),
            "--resource",
            "resources_id",
            resources_id,
        ]
    )
    assert result.exit_code == 0
    wk_path = Path(result.stdout_bytes.decode().strip())
    assert wk_path.is_dir()
    wkt = hf.Workflow(wk_path).template
    assert wkt.resources.get().resources_id == resources_id
    assert wkt.resources.get().random_seed == 0  # check this item still exists
