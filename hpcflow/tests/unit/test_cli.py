from pathlib import Path
import json
from unittest.mock import Mock
import pytest

from click.testing import CliRunner
import click.exceptions

from hpcflow import __version__
from hpcflow.app import app as hf
from hpcflow.sdk.cli import ErrorPropagatingClickContext
from hpcflow.sdk.cli_common import BoolOrString


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


@pytest.fixture(params=["go", "demo-go", "submit"])
def submission_command(request):
    if request.param == "go":
        return hf.cli.commands["go"], ["template.yaml"]
    if request.param == "demo-go":
        return hf.cli.commands["demo-workflow"].commands["go"], ["workflow_1"]
    return hf.cli.commands["workflow"].commands["submit"], []


def test_containerised_help(submission_command):
    command, _ = submission_command
    result = CliRunner().invoke(command, ["--help"])
    assert result.exit_code == 0
    assert "--containerised" in result.output
    assert "JSON host submission plan" in result.output


@pytest.mark.parametrize(
    ("containerised", "modify_js"), [(False, False), (False, True), (True, False)]
)
def test_containerised_passed_to_submission_api(
    submission_command, monkeypatch, containerised, modify_js
):
    command, args = submission_command
    plan = {"schema_version": 1, "workflow_path": "workflow", "jobscripts": []}
    submit = Mock(return_value=plan if command.name == "submit" else (None, plan))
    workflow = hf.Workflow.__new__(hf.Workflow)
    if command.name == "submit":
        monkeypatch.setattr(hf.Workflow, "submit", submit)
    else:
        api_name = (
            "make_and_submit_workflow"
            if args == ["template.yaml"]
            else "make_and_submit_demo_workflow"
        )
        monkeypatch.setattr(type(hf), api_name, property(lambda self: submit))
    result = CliRunner().invoke(
        command,
        [
            *args,
            *(["--containerised"] if containerised else []),
            *(["--modify-js"] if modify_js else []),
        ],
        obj=workflow,
    )
    assert result.exit_code == 0
    submit.assert_called_once()
    assert submit.call_args.kwargs["containerised"] is containerised
    assert submit.call_args.kwargs["modify_js"] is modify_js
    if containerised:
        assert json.loads(result.stdout) == plan


@pytest.mark.parametrize("command", ["go", "demo-go", "submit"])
def test_containerised_json_plan(tmp_path, cli_runner, monkeypatch, command):
    launch = Mock(side_effect=AssertionError("Container must not launch jobscripts"))
    monkeypatch.setattr(hf.Jobscript, "submit", launch)
    if command == "submit":
        workflow = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
        assert isinstance(workflow, hf.Workflow)
        args = ["workflow", str(workflow.path), "submit"]
        hf.unload_config()
    elif command == "demo-go":
        args = ["demo-workflow", "go", "workflow_1", "--path", str(tmp_path)]
    else:
        args = [
            "go",
            json.dumps(
                {
                    "name": "plan",
                    "tasks": [{"schema": "test_t1_conditional_OS", "inputs": {"p1": 1}}],
                }
            ),
            "--string",
            "--format",
            "json",
            "--path",
            str(tmp_path),
        ]
    result = cli_runner([*args, "--containerised"])
    assert result.exit_code == 0, result.output
    plan = json.loads(result.stdout)
    assert plan["schema_version"] == 1
    assert len(plan["jobscripts"]) == 1
    workflow_path = Path(plan["workflow_path"])
    assert (workflow_path / plan["jobscripts"][0]["path"]).is_file()
    assert not hf.Workflow(workflow_path).submissions[0].submitted_jobscripts
    launch.assert_not_called()


@pytest.mark.parametrize("option", ["--wait", "--cancel", "--modify-js"])
def test_containerised_rejects_host_only_options(tmp_path, cli_runner, option):
    result = cli_runner(
        [
            "demo-workflow",
            "go",
            "workflow_1",
            "--path",
            str(tmp_path),
            "--containerised",
            option,
        ]
    )
    assert result.exit_code != 0
    assert list(tmp_path.iterdir()) == []


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
