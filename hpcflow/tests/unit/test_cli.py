import pytest

from click.testing import CliRunner

from hpcflow import __version__
from hpcflow.app import app as hf


def test_version() -> None:
    runner = CliRunner()
    result = runner.invoke(hf.cli, args="--version")
    assert result.output.strip() == f"hpcFlow, version {__version__}"
