from pathlib import Path
from unittest.mock import Mock

from click.testing import CliRunner
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_env_CLI


def test_import_env_from_file_selects_before_substitution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    source = tmp_path / "envs.yaml"
    source.write_text(
        """\
- name: damask_env
  specifiers:
    version: "3.1.0"
    build: ib
  setup: |
    IMG_PATH="<<var:sif_path>>"
  executables:
    - label: damask_grid
      instances:
        - parallel_mode:
          command: singularity exec "<<var:sif_path>>" DAMASK_grid
          num_cores: 1
- name: damask_env
  specifiers:
    version: "3.2.0"
  setup: "<<var:another_recipe_variable>>"
""",
        encoding="utf-8",
        newline="\n",
    )
    saved_path = tmp_path / "configured_envs.yaml"
    save_env = Mock(return_value=saved_path)
    monkeypatch.setattr(hf, "save_env", save_env)

    result = hf.import_env_from_file(
        source,
        "damask_env",
        specifiers={"version": "3.1.0", "build": "ib"},
        variables={"sif_path": "/containers/damask.sif"},
    )

    assert result == saved_path
    imported_env = save_env.call_args.args[0]
    assert imported_env.setup == ('IMG_PATH="/containers/damask.sif"',)
    assert (
        imported_env.executables[0].instances[0].command
        == 'singularity exec "/containers/damask.sif" DAMASK_grid'
    )


@pytest.mark.parametrize(
    ("definitions", "specifiers", "message"),
    [
        (
            '- name: damask_env\n  specifiers: {version: "3.1.0"}\n',
            {"version": "3.2.0"},
            "No environment named",
        ),
        (
            '- name: damask_env\n  specifiers: {version: "3.1.0", build: ib}\n'
            '- name: damask_env\n  specifiers: {version: "3.1.0", build: cpu}\n',
            {"version": "3.1.0"},
            "Multiple environments named",
        ),
    ],
)
def test_import_env_from_file_requires_unique_match(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    definitions: str,
    specifiers: dict[str, str],
    message: str,
) -> None:
    source = tmp_path / "envs.yaml"
    source.write_text(definitions, encoding="utf-8", newline="\n")
    save_env = Mock()
    monkeypatch.setattr(hf, "save_env", save_env)

    with pytest.raises(ValueError, match=message):
        hf.import_env_from_file(source, "damask_env", specifiers=specifiers)
    save_env.assert_not_called()


def test_env_import_cli_passes_selection_and_variables() -> None:
    app = Mock()
    env_cli, _ = _make_env_CLI(app)
    result = CliRunner().invoke(
        env_cli,
        [
            "import",
            "github://hpcflow:matflow-configs@main/environments/cambridge-CSD3-damask.yaml",
            "--name",
            "damask_env",
            "--specifier",
            "version",
            "3.1.0",
            "--var",
            "sif_path",
            "/containers/damask.sif",
        ],
    )

    assert result.exit_code == 0, result.output
    app.import_env_from_file.assert_called_once_with(
        "github://hpcflow:matflow-configs@main/environments/cambridge-CSD3-damask.yaml",
        "damask_env",
        specifiers={"version": "3.1.0"},
        variables={"sif_path": "/containers/damask.sif"},
        replace=False,
    )
