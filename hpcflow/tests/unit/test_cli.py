import pytest

from click.testing import CliRunner
import click.exceptions

from hpcflow import __version__
from hpcflow.app import app as hf
from hpcflow.sdk.cli import ErrorPropagatingClickContext
from hpcflow.sdk.cli_common import BoolOrString


def test_version() -> None:
    runner = CliRunner()
    result = runner.invoke(hf.cli, args="--version")
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

    @cli.command()
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


def test_std_stream_file_created(tmp_path):
    """Test exception is intercepted and printed to the specified --std-stream file."""
    error_file = tmp_path / "std_stream.txt"
    runner = CliRunner()
    result = runner.invoke(
        hf.cli, args=f'--std-stream "{str(error_file)}" internal noop --raise'
    )
    assert error_file.is_file()
    std_stream_contents = error_file.read_text()
    assert "ValueError: internal noop raised!" in std_stream_contents
    assert result.exit_code == 1
    assert result.exc_info[0] == SystemExit


def test_std_stream_file_not_created(null_config, tmp_path):
    """Test std stream file is not created when no ouput/errors/exceptions"""
    error_file = tmp_path / "std_stream.txt"
    runner = CliRunner()
    result = runner.invoke(hf.cli, args=f'--std-stream "{str(error_file)}" internal noop')
    assert not error_file.is_file()
    assert result.exit_code == 0


def test_cli_exception():
    """Test exception is passed to click"""
    runner = CliRunner()
    result = runner.invoke(hf.cli, args="internal noop --raise")
    assert result.exit_code == 1
    assert result.exc_info[0] == ValueError


def test_cli_click_exit_code_zero(tmp_path):
    """Test Click's `Exit` exception is ignored by the `redirect_std_to_file` context manager when the exit code is zero."""
    error_file = tmp_path / "std_stream.txt"
    runner = CliRunner()
    result = runner.invoke(
        hf.cli, args=f'--std-stream "{str(error_file)}" internal noop --click-exit-code 0'
    )
    assert result.exit_code == 0
    assert not error_file.is_file()


def test_cli_click_exit_code_non_zero(tmp_path):
    """Test Click's `Exit` exception is not ignored by the `redirect_std_to_file` context manager when the exit code is non-zero."""
    error_file = tmp_path / "std_stream.txt"
    runner = CliRunner()
    result = runner.invoke(
        hf.cli, args=f'--std-stream "{str(error_file)}" internal noop --click-exit-code 2'
    )
    assert result.exit_code == 2
    assert error_file.is_file()
