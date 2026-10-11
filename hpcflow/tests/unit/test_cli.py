from pathlib import Path
import os
import subprocess
import sys
from io import StringIO
from unittest.mock import Mock
import pytest

from click.testing import CliRunner
from rich.console import Console
import click.exceptions

from hpcflow import __version__
from hpcflow.app import app as hf
from hpcflow.sdk.cli import (
    ErrorPropagatingClickContext,
    make_cli,
)
from hpcflow.sdk.cli_common import BoolOrString


def test_version(cli_runner) -> None:
    result = cli_runner(["--version"])
    assert result.output.strip() == f"hpcFlow, version {__version__}"


@pytest.mark.parametrize(
    ("force_colour", "no_colour", "strip"),
    [("1", "", False), ("", "", None), ("1", "1", None)],
)
def test_cli_preserves_forced_colour(monkeypatch, force_colour, no_colour, strip):
    monkeypatch.setenv("FORCE_COLOR", force_colour)
    monkeypatch.setenv("NO_COLOR", no_colour)
    init = Mock()
    monkeypatch.setattr("hpcflow.sdk.cli.colorama_init", init)
    make_cli(hf)
    init.assert_called_once_with(autoreset=True, strip=strip, convert=strip)


def test_cli_import_preserves_rich_colour_in_pipe():
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import hpcflow.cli; from rich.console import Console; "
            "Console(legacy_windows=False).print('[red]colour-probe[/red]')",
        ],
        env={**os.environ, "FORCE_COLOR": "1", "NO_COLOR": "", "TTY_INTERACTIVE": "0"},
        capture_output=True,
        encoding="utf-8",
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert "\x1b[31mcolour-probe\x1b[0m" in result.stdout


def test_show_noninteractive_emits_no_spinner_controls(monkeypatch):
    output = StringIO()
    console = Console(
        file=output, force_terminal=True, force_interactive=False, legacy_windows=False
    )
    monkeypatch.setattr("hpcflow.sdk.app.Console", lambda: console)
    monkeypatch.setattr(hf, "_get_known_submissions", Mock(return_value=[]))
    hf._show(no_update=True)
    assert output.getvalue() == ""


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
