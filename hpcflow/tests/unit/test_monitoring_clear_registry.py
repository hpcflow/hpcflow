"""Config-free recovery from invalid submission registries."""

from unittest.mock import Mock, PropertyMock

from click.testing import CliRunner
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.cli import make_cli
from hpcflow.sdk.core.warnings import DeprecationWarning_


@pytest.fixture
def registry_files(tmp_path, monkeypatch):
    monkeypatch.setattr(hf.run_time_info, "from_CLI", hf.run_time_info.from_CLI)
    monkeypatch.setattr(hf, "_user_data_dir", tmp_path / "data")
    monkeypatch.setattr(
        hf, "load_config", Mock(side_effect=AssertionError("Must not load config"))
    )
    monkeypatch.setattr(
        type(hf),
        "config",
        PropertyMock(side_effect=AssertionError("Must not access config")),
    )
    paths = []
    preserved = []
    for machine in ("local-host", "custom-cluster-alias"):
        folder = hf.user_data_dir / machine
        folder.mkdir(parents=True)
        for name in ("known_submissions_v2.json", "known_submissions.txt"):
            path = folder / name
            path.write_text("invalid registry contents")
            paths.append(path)
        marker = folder / "known_submissions_v2.lock"
        marker.write_text("preserve")
        preserved.append(marker)
        nested = folder / "workflow" / "known_submissions_v2.json"
        nested.parent.mkdir()
        nested.write_text("preserve")
        preserved.append(nested)
    return paths, preserved


@pytest.mark.parametrize("command", ["clear-registry", "clear-known-subs"])
def test_clear_registry_root_cli_without_config(command, registry_files):
    paths, preserved = registry_files
    cli, _ = make_cli(hf)
    if command == "clear-known-subs":
        with pytest.warns(DeprecationWarning_) as captured:
            result = CliRunner().invoke(cli, ["manage", command])
        assert len(captured) == 1
        assert "manage clear-registry" in captured[0].message.solution
    else:
        result = CliRunner().invoke(cli, ["manage", command])
    assert result.exit_code == 0, result.output
    assert all(not path.exists() for path in paths)
    assert all(path.read_text() == "preserve" for path in preserved)
    hf.load_config.assert_not_called()
    assert "DeprecationWarning" not in result.output


def test_clear_registry_sdk_and_deprecated_alias(registry_files):
    paths, preserved = registry_files
    with pytest.warns(DeprecationWarning_, match="deprecated") as captured:
        hf.clear_known_submissions_file()
    assert "clear_registry()" in captured[0].message.solution
    hf.clear_registry()  # Repeating the reset is harmless.
    assert all(not path.exists() for path in paths)
    assert all(path.read_text() == "preserve" for path in preserved)


def test_clear_registry_missing_data_directory(tmp_path, monkeypatch):
    path = tmp_path / "does-not-exist"
    monkeypatch.setattr(hf, "_user_data_dir", path)
    hf.clear_registry()
    assert not path.exists()
