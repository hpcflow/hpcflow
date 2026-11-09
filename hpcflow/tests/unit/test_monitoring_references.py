"""The latest-submission alias follows submission times, including reused IDs."""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_cancel_CLI, _make_show_CLI, _make_workflow_CLI
from hpcflow.sdk.monitoring.models import RegistryData, SubmissionRecord, to_json
from hpcflow.sdk.monitoring.references import (
    resolve_records,
    resolve_submission_id,
    resolve_workflow_path,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json


def record(tmp_path, local_id, workflow_id, submitted):
    return SubmissionRecord(
        str(tmp_path / workflow_id),
        workflow_id,
        workflow_id,
        0,
        f"part-{local_id}",
        submitted,
        local_id=local_id,
    )


@pytest.fixture
def records(tmp_path):
    old = record(tmp_path, 80, "old", "2026-11-08T15:00:00Z")
    latest = record(tmp_path, 2, "latest", "2026-11-09T12:00:00Z")
    finished_later = record(tmp_path, 500, "other", "2026-11-09T10:00:00Z")
    finished_later.finished_at = "2026-11-10T12:00:00Z"
    earlier_part = record(tmp_path, 3, "latest", "2026-11-09T11:00:00Z")
    return [old, latest, finished_later, earlier_part]


@pytest.mark.parametrize("reference", [-1, "-1"])
@pytest.mark.parametrize("ref_type", [None, "assume-id", "id"])
def test_latest_alias_matches_the_latest_records_normal_id(records, reference, ref_type):
    assert resolve_records(records, reference, ref_type) == [records[1], records[3]]
    assert resolve_records(records, reference, ref_type) == resolve_records(records, 2)
    assert resolve_workflow_path(records, reference, ref_type) == Path(
        records[1].workflow_path
    )


def test_submission_order_breaks_equal_timestamp_ties(tmp_path):
    first = record(tmp_path, 100, "first", "2026-11-09T12:00:00Z")
    last = record(tmp_path, 1, "last", "2026-11-09T13:00:00+01:00")
    assert resolve_submission_id([first, last], -1) is last


@pytest.mark.parametrize("resolver", [resolve_records, resolve_workflow_path])
def test_latest_alias_with_no_records_has_a_clear_error(resolver):
    with pytest.raises(ValueError, match="No registered submissions.*-1"):
        resolver([], -1)


def test_explicit_names_and_paths_are_literal(records, tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    literal = records[0]
    literal.workflow_name = "-1"
    literal.workflow_path = str(tmp_path / "-1")
    assert resolve_records(records, "-1", "name") == [literal]
    assert resolve_records(records, "-1", "path") == [literal]
    assert resolve_records(records, Path("-1")) == [literal]
    assert resolve_records(records, "-1") == [records[1], records[3]]


@pytest.mark.parametrize("reference", [-1, "-1"])
def test_workflow_constructor_and_app_resolver_accept_latest(
    tmp_path, monkeypatch, reference
):
    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "registry")
    workflow = hf.make_demo_workflow(
        "workflow_1", path=tmp_path, name="latest", name_add_timestamp=False, status=False
    )
    old = record(tmp_path, 99, "missing-old", "2026-11-08T12:00:00Z")
    latest = record(tmp_path, 0, workflow.id_, "2026-11-09T12:00:00Z")
    latest.workflow_path = str(workflow.path)
    registry = SubmissionRegistry(hf)
    atomic_json(registry.path, to_json(RegistryData([old, latest], 100)))
    assert Path(hf.Workflow(reference).path) == Path(workflow.path)
    assert hf._get_workflow_path_from_local_ID(-1) == Path(workflow.path)
    assert hf._resolve_workflow_reference("-1", "assume-id") == Path(workflow.path)


@pytest.mark.parametrize(
    "args", [["-1"], ["-1", "--view", "detail"], ["--use-cache", "-1"], ["--", "-1"]]
)
def test_show_cli_accepts_negative_reference(args):
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    result = CliRunner().invoke(_make_show_CLI(app), args)
    assert result.exit_code == 0, result.output
    assert app.show.call_args.kwargs["workflow_ref"] == "-1"


def test_show_rejects_conflicting_references():
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    result = CliRunner().invoke(_make_show_CLI(app), ["42", "-1"])
    assert result.exit_code == 2
    assert "Specify WORKFLOW_REF or -1" in result.output
    app.show.assert_not_called()


def test_cancel_cli_passes_negative_positional_reference():
    app = SimpleNamespace(cancel=Mock())
    result = CliRunner().invoke(_make_cancel_CLI(app), ["--", "-1"])
    assert result.exit_code == 0, result.output
    assert app.cancel.call_args.kwargs["workflow_ref"] == "-1"


def test_workflow_cli_passes_negative_reference_before_subcommand(tmp_path):
    app = SimpleNamespace(
        name="hpcflow",
        _resolve_workflow_reference=Mock(return_value=tmp_path),
        Workflow=Mock(),
    )
    command = _make_workflow_CLI(app)

    @command.command("check-latest")
    def check():
        pass

    result = CliRunner().invoke(command, ["--", "-1", "check-latest"])
    assert result.exit_code == 0, result.output
    app._resolve_workflow_reference.assert_called_once_with("-1", "assume-id")
