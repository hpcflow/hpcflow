"""Show/cancel select workflows; unregister removes exact ID entries atomically."""

from pathlib import Path
import socket
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_cancel_CLI, _make_show_CLI, _make_unregister_CLI
from hpcflow.sdk.monitoring.listing import list_registered_submissions
from hpcflow.sdk.monitoring.models import JobRecord, SubmissionRecord
from hpcflow.sdk.monitoring.references import parse_submission_ids, resolve_records
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json
from hpcflow.sdk.monitoring.status import cancel, collect
from hpcflow.sdk.submission.enums import JobscriptElementState


@pytest.fixture
def source(tmp_path, monkeypatch):
    adapter = SimpleNamespace(
        show_cmd=["squeue", "--me"],
        get_job_state_info=Mock(
            return_value={
                "102": JobscriptElementState.running,
                "109": JobscriptElementState.pending,
            }
        ),
        cancel_jobs=Mock(),
    )
    app = SimpleNamespace(
        config={},
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy",
        submission_logger=Mock(),
        get_scheduler=Mock(return_value=adapter),
        Workflow=Mock(side_effect=AssertionError("Unexpected workflow load")),
    )
    rows = []
    for local_id, workflow_id, hour, job_ref in [
        (2, "a", 10, "102"),
        (5, "a", 11, "102"),
        (9, "b", 13, "109"),
        (13, "c", 12, "113"),
    ]:
        row = SubmissionRecord(
            str(tmp_path / workflow_id),
            workflow_id,
            workflow_id,
            0,
            f"part-{local_id}",
            f"2026-11-09T{hour}:00:00Z",
            local_id=local_id,
            activity="active",
            accessibility="readable",
            jobs=[
                JobRecord(
                    0,
                    "slurm",
                    "posix",
                    hostname=socket.gethostname(),
                    scheduler_reference=job_ref,
                )
            ],
        )
        rows.append(row)
        atomic_json(
            Path(row.workflow_path) / ".zattrs",
            {"creation_info": {"id": workflow_id}, "name": workflow_id},
        )
    registry = SubmissionRegistry(app)
    with registry.transaction() as data:
        data.records = rows
    monkeypatch.setattr(hf, "_user_data_hostname_dir", app.user_data_hostname_dir)
    return app, registry, rows, adapter


@pytest.mark.parametrize("reference", ["2,9", " 2, 5,9,2 ", "-1,2,9"])
def test_show_deduplicates_workflows_and_batches_scheduler_queries(source, reference):
    app, _, _, adapter = source
    result = collect(app, reference, include_summary=False, no_update=True)
    assert [row.local_id for row in result] == [9, 5, 2]
    adapter.get_job_state_info.assert_called_once_with(js_refs=["102", "109"])
    app.Workflow.assert_not_called()


@pytest.mark.parametrize("use_cache", [False, True])
def test_listing_supports_multiple_ids(source, use_cache):
    app, _, _, adapter = source
    result = list_registered_submissions(app, "2,9,5", use_cache=use_cache)
    assert {row.local_id for row in result} == {2, 5, 9}
    adapter.get_job_state_info.assert_not_called()
    app.Workflow.assert_not_called()


def test_cancel_sends_one_deduplicated_request_for_selected_workflows(source):
    app, registry, _, adapter = source
    assert cancel(app, "2,5,9,2,-1", quiet=True)
    adapter.cancel_jobs.assert_called_once_with(js_refs=["102", "109"], quiet=True)
    adapter.get_job_state_info.assert_not_called()
    saved = {row.local_id: row for row in registry.read().records}
    assert all(saved[index].cancel_requested_at for index in (2, 5, 9))
    assert not saved[13].cancel_requested_at


@pytest.mark.parametrize("operation", [collect, cancel, list_registered_submissions])
@pytest.mark.parametrize("reference", ["2,999", "2,abc", "2,,9"])
def test_invalid_lists_do_not_query_cancel_or_modify_registry(
    source, operation, reference
):
    app, registry, _, adapter = source
    before = registry.path.read_bytes()
    with pytest.raises(ValueError):
        operation(app, reference)
    assert registry.path.read_bytes() == before
    app.get_scheduler.assert_not_called()
    adapter.cancel_jobs.assert_not_called()
    app.Workflow.assert_not_called()


def test_explicit_names_and_paths_containing_commas_remain_literal(source):
    _, _, rows, _ = source
    rows[3].workflow_name = "2,9"
    assert resolve_records(rows, "2,9", "name", allow_multiple=True) == [rows[3]]
    rows[3].workflow_path = str(Path(rows[3].workflow_path).parent / "2,9")
    assert resolve_records(rows, Path(rows[3].workflow_path), allow_multiple=True) == [
        rows[3]
    ]


def test_registered_comma_name_is_preserved_in_automatic_mode(source):
    _, _, rows, _ = source
    rows[3].workflow_name = "alpha,beta"
    assert resolve_records(rows, "alpha,beta", allow_multiple=True) == [rows[3]]


@pytest.mark.parametrize("ids", ["2,9,2", [2, 9, 2], (-1, 2, 9)])
def test_unregister_removes_only_named_entries_and_frees_ids(source, ids):
    _, registry, rows, adapter = source
    removed = hf.unregister(ids)
    assert set(removed) == {2, 9}
    data = registry.read()
    assert [row.local_id for row in data.records] == [5, 13]
    assert set(data.free_local_ids) == {2, 9}
    assert all(Path(row.workflow_path).exists() for row in rows)
    adapter.cancel_jobs.assert_not_called()
    adapter.get_job_state_info.assert_not_called()


def test_unregister_unknown_id_rejects_whole_selection(source):
    _, registry, _, _ = source
    before = registry.path.read_bytes()
    with pytest.raises(ValueError, match="Unknown registered submission ID: 999"):
        hf.unregister("2,999")
    assert registry.path.read_bytes() == before


def test_unregister_submitting_entry_rejects_whole_selection(source):
    _, registry, _, _ = source
    with registry.transaction() as data:
        data.records[2].submitting = True
    before = registry.path.read_bytes()
    with pytest.raises(ValueError, match="while jobs are being submitted"):
        hf.unregister("2,9")
    assert registry.path.read_bytes() == before


@pytest.mark.parametrize(
    "value", ["", ",", "2,", ",2", "2,,9", "2,word", "2,-2", [], [True], [1.5]]
)
def test_invalid_id_syntax(value):
    with pytest.raises(ValueError):
        parse_submission_ids(value)


@pytest.mark.parametrize("factory", [_make_show_CLI, _make_cancel_CLI])
@pytest.mark.parametrize("args", [["2, 9,2"], ["--", "-1,2"]])
def test_cli_passes_workflow_id_lists(factory, args):
    app = SimpleNamespace(show=Mock(), show_legend=Mock(), cancel=Mock())
    result = CliRunner().invoke(factory(app), args)
    assert result.exit_code == 0, result.output
    callback = app.show if factory is _make_show_CLI else app.cancel
    assert callback.call_args.kwargs["workflow_ref"] == args[-1]


def test_unregister_cli_list_is_atomic_and_reports_removed_ids(source):
    _, registry, _, _ = source
    command = _make_unregister_CLI(hf)
    before = registry.path.read_bytes()
    result = CliRunner().invoke(command, ["2,999"])
    assert result.exit_code == 1
    assert registry.path.read_bytes() == before
    result = CliRunner().invoke(command, ["2, 9,2"])
    assert result.exit_code == 0, result.output
    assert "Unregistered submissions 2,9" in result.output
    assert [row.local_id for row in registry.read().records] == [5, 13]


def test_unregister_cli_latest_alias_in_list(source):
    _, registry, _, _ = source
    result = CliRunner().invoke(_make_unregister_CLI(hf), ["--", "-1,2"])
    assert result.exit_code == 0, result.output
    assert "Unregistered submissions 9,2" in result.output
    assert [row.local_id for row in registry.read().records] == [5, 13]
