"""Monitoring regression tests, including store and scheduler call boundaries."""

from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from copy import deepcopy
from io import StringIO
import json
import zstandard as zstd
import logging
import re
from pathlib import Path
import socket
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_cancel_CLI, _make_show_CLI
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.monitoring.cache import (
    cache_path,
    refresh_actions,
    snapshot as build_snapshot,
)
from hpcflow.sdk.monitoring.listing import list_registered_submissions
from hpcflow.sdk.monitoring.models import (
    ActionRunState,
    ActionSummary,
    IterationActionState,
    JobRecord,
    ProcessReference,
    to_json,
)
from hpcflow.sdk.monitoring.references import resolve_records, resolve_workflow_path
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json, new_record
from hpcflow.sdk.monitoring.render import (
    action_text,
    element_ranges,
    full_action_rows,
    iteration_text as render_iteration_text,
    render,
    render_registered,
)
from hpcflow.sdk.monitoring.status import cancel, collect
from hpcflow.sdk.submission.enums import JobscriptElementState


def snapshot_from_json(entries, ids):
    return build_snapshot(
        {int(key): ActionRunState.from_json(value) for key, value in entries.items()}, ids
    )


def iteration_text_from_json(pattern):
    return render_iteration_text(
        tuple(IterationActionState.from_json(value) for value in pattern)
    )


@pytest.fixture
def app(tmp_path):
    class Settings(dict):
        def get(self, key, default=None):
            return super().get(key, default)

    config = Settings(known_submissions_max_inactive=100, show_max_recent=3)
    return SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy.txt",
        config=config,
        submission_logger=logging.getLogger(__name__),
        Workflow=Mock(side_effect=AssertionError("Unexpected workflow load")),
        get_scheduler=Mock(return_value=Mock(get_job_state_info=Mock(return_value={}))),
        get_info=lambda: {"version": "test"},
    )


def record(tmp_path, local_id=0, activity="active", workflow_id="workflow"):
    path = tmp_path / f"wf-{workflow_id}"
    path.mkdir(exist_ok=True)
    atomic_json(path / ".zattrs", {"creation_info": {"id": workflow_id}})
    item = new_record(str(path), workflow_id, 0, "2026-10-31T10:00:00+00:00")
    item.local_id = local_id
    item.activity = activity
    item.jobs = [
        JobRecord.from_json(
            {
                "jobscript_index": 0,
                **{
                    "jobscript_index": 0,
                    "scheduler_type": "slurm",
                    "os_name": "posix",
                    "scheduler_args": {"del_cmd": "custom-scancel"},
                    "hostname": socket.gethostname(),
                    "scheduler_reference": str(local_id + 100),
                    "is_array": False,
                    "run_ids": [local_id],
                },
            }
        )
    ]
    return item


def seed(app, records):
    registry = SubmissionRegistry(app)
    with registry.transaction() as data:
        data.records = records
        data.next_local_id = max((r.local_id for r in records), default=-1) + 1
    return registry


def entry(state="success", element=0, iteration=0, action=0):
    return {
        "task": 0,
        "task_name": "task",
        "element": element,
        "iteration": iteration,
        "action": action,
        "attempt": 0,
        "state": state,
    }


def cache(record, entries):
    atomic_json(
        cache_path(record).with_suffix(""),
        {
            "schema_version": 1,
            "workflow_id": record.workflow_id,
            "submission_index": record.submission_index,
            "terminal_states": entries,
            "observed_at": "2026-10-31T11:00:00+00:00",
        },
    )


def test_active_first_default_limit_and_no_inactive_store_loads(app, tmp_path):
    records = [record(tmp_path, i, "inactive", str(i)) for i in range(6)]
    records[-1].activity = "active"
    for r in records:
        cache(r, {str(r.local_id): entry()})
    seed(app, records)
    app.config["show_max_recent"] = 2
    app.get_scheduler.return_value.get_job_state_info.return_value = {
        "105": JobscriptElementState.running
    }
    result = collect(app)
    assert len(result) == 3
    assert result[0].local_id == 5
    assert [r.activity for r in result] == ["active", "inactive", "inactive"]
    app.Workflow.assert_not_called()
    app.get_scheduler.return_value.get_job_state_info.assert_called_once_with(
        js_refs=["105"]
    )
    json.dumps(to_json(result))


def test_show_with_existing_config_missing_monitoring_settings(
    tmp_path, modifiable_config, monkeypatch
):
    # Simulate a config written by a release predating the monitoring settings.
    path = hf.config.config_file_path
    text = path.read_text(encoding="utf-8")
    text = re.sub(
        r"(?m)^\s*(?:show_max_recent|known_submissions_max_inactive):[^\n]*\n",
        "",
        text,
    )
    path.write_text(text, encoding="utf-8")
    hf.reload_config(config_dir=path.parent, config_key=hf.config.config_key, warn=False)
    assert hf.config.get("show_max_recent") is None
    assert hf.config.get("known_submissions_max_inactive") is None
    assert hf.config.get("show_max_recent", default=3) == 3
    assert hf.config.get("known_submissions_max_inactive", default=100) == 100
    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "history")
    records = [record(tmp_path, i, "inactive", str(i)) for i in range(5)]
    for r in records:
        cache(r, {str(r.local_id): entry()})
    registry = seed(hf, records)

    result = CliRunner().invoke(_make_show_CLI(hf), [])
    assert result.exit_code == 0, result.output
    assert len(collect(hf)) == 3
    assert len(registry.read().records) == 5


@pytest.mark.parametrize("value", [None, 0, 2])
def test_monitoring_limits_distinguish_missing_settings_and_zero(app, tmp_path, value):
    app.config["show_max_recent"] = value
    app.config["known_submissions_max_inactive"] = value
    records = [record(tmp_path, i, "inactive", str(i)) for i in range(5)]
    for r in records:
        cache(r, {str(r.local_id): entry()})
    registry = seed(app, records)
    assert len(collect(app)) == (3 if value is None else value)
    assert len(registry.read().records) == (5 if value is None else value)


def test_scheduler_outage_retains_unknown_unloadable_workflow(app, tmp_path):
    r = record(tmp_path)
    Path(r.workflow_path).joinpath(".zattrs").unlink()
    registry = seed(app, [r])
    app.config["known_submissions_max_inactive"] = 0
    app.get_scheduler.return_value.get_job_state_info.side_effect = OSError("offline")
    result = collect(app, max_recent=0)
    assert result[0].activity == "unknown"
    assert result[0].accessibility == "incompatible"
    assert registry.read().records[0].jobs


def test_missing_active_workflow_is_still_active_and_cancellable(app, tmp_path):
    r = record(tmp_path)
    Path(r.workflow_path).joinpath(".zattrs").unlink()
    Path(r.workflow_path).rmdir()
    seed(app, [r])
    scheduler = app.get_scheduler.return_value
    scheduler.get_job_state_info.return_value = {"100": JobscriptElementState.running}
    assert collect(app) == []
    assert collect(app, include_missing=True)[0].activity == "active"
    assert cancel(app, "0", quiet=True)
    scheduler.cancel_jobs.assert_called_once_with(js_refs=["100"], quiet=True)
    app.Workflow.assert_not_called()
    assert SubmissionRegistry(app).read().records[0].activity == "unknown"


def test_retention_keeps_active_records_and_does_not_reuse_ids(app, tmp_path):
    app.config["known_submissions_max_inactive"] = 1
    records = [record(tmp_path, i, "inactive", str(i)) for i in range(5)]
    records[0].activity = "active"
    records[1].activity = "unknown"
    for i, r in enumerate(records):
        r.finished_at = f"2026-10-{i + 1:02}T10:00:00+00:00"
    registry = seed(app, records)
    assert {r.local_id for r in registry.read().records} == {0, 1, 4}
    sub = SimpleNamespace(
        workflow=SimpleNamespace(path="new", id_="new", name="new"), index=0
    )
    assert registry.begin(sub, "now") == 5


@pytest.mark.parametrize("limit", [None, 0, 2])
def test_unknown_retention_defaults_and_independent_activity_limits(app, tmp_path, limit):
    app.config["known_submissions_max_unknown"] = limit
    app.config["known_submissions_max_inactive"] = 1
    records = [record(tmp_path, i, "unknown", str(i)) for i in range(15)]
    for i, r in enumerate(records):
        r.submitted_at = f"2026-11-01T12:{i:02}:00Z"
    records.extend(record(tmp_path, i, "active", str(i)) for i in (15, 16))
    records.extend(record(tmp_path, i, "inactive", str(i)) for i in (17, 18))
    registry = seed(app, records)
    retained = registry.read()
    expected = 10 if limit is None else limit
    assert {r.local_id for r in retained.records if r.activity == "unknown"} == set(
        range(15 - expected, 15)
    )
    assert {r.local_id for r in retained.records if r.activity == "active"} == {15, 16}
    assert len([r for r in retained.records if r.activity == "inactive"]) == 1
    assert retained.next_local_id == 19


def test_unknown_retention_uses_submission_time_and_id_not_last_check(app, tmp_path):
    app.config["known_submissions_max_unknown"] = 1
    records = [record(tmp_path, i, "unknown", str(i)) for i in range(3)]
    records[0].submitted_at = records[1].submitted_at = "2026-11-02T12:00:00Z"
    records[2].submitted_at = "2026-11-01T12:00:00Z"
    records[2].last_checked_at = records[2].finished_at = "2026-11-07T12:00:00Z"
    registry = seed(app, list(reversed(records)))
    assert [r.local_id for r in registry.read().records] == [1]
    assert registry.read().next_local_id == 3


def test_existing_unknown_history_prunes_on_update_not_read_only_operations(
    app, tmp_path
):
    from hpcflow.sdk.monitoring.models import RegistryData, to_json

    records = [record(tmp_path, i, "unknown", str(i)) for i in range(15)]
    registry = SubmissionRegistry(app)
    # Simulate a registry written before the unknown retention limit existed.
    atomic_json(registry.path, to_json(RegistryData(records, next_local_id=15)))
    before = registry.path.read_bytes()
    assert len(list_registered_submissions(app)) == 15
    app.get_scheduler.return_value.get_job_state_info.side_effect = OSError("offline")
    assert len(collect(app, no_update=True, include_summary=False)) == 15
    assert registry.path.read_bytes() == before
    registry.update([])
    assert {r.local_id for r in registry.read().records} == set(range(5, 15))
    assert registry.read().next_local_id == 15


def test_unknown_retention_setting_is_applied_from_real_config(
    tmp_path, modifiable_config, monkeypatch
):
    hf.config.set("known_submissions_max_unknown", 2)
    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "registry")
    registry = SubmissionRegistry(hf)
    with registry.transaction() as data:
        data.records = [record(tmp_path, i, "unknown", str(i)) for i in range(5)]
        data.next_local_id = 5
    assert {r.local_id for r in registry.read().records} == {3, 4}


def test_concurrent_updates_preserve_added_jobs(app, tmp_path):
    r = record(tmp_path)
    registry = seed(app, [r])
    stale = deepcopy(r)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(
            pool.map(
                lambda i: registry.add_job(
                    0,
                    JobRecord.from_json({"jobscript_index": 0, **{"jobscript_index": i}}),
                ),
                range(1, 9),
            )
        )
    stale.activity = "inactive"
    registry.update([stale])
    current = registry.read().records[0]
    assert len(current.jobs) == 9
    assert current.activity == "active"


def test_reference_filter_happens_before_scheduler_queries(app, tmp_path):
    a = record(tmp_path, 0, workflow_id="a")
    b = record(tmp_path, 1, workflow_id="b")
    seed(app, [a, b])
    cache(a, {"0": entry()})
    app.get_scheduler.return_value.get_job_state_info.return_value = {
        "100": JobscriptElementState.running
    }
    assert [r.workflow_id for r in collect(app, "0")] == ["a"]
    app.get_scheduler.return_value.get_job_state_info.assert_called_once_with(
        js_refs=["100"]
    )
    assert resolve_records([a, b], a.workflow_path, "path") == [a]


def test_registered_name_selects_all_parts_of_one_workflow(app, tmp_path):
    records = [
        record(tmp_path, 0),
        record(tmp_path, 1),
        record(tmp_path, 2, workflow_id="other"),
    ]
    for r in records[:2]:
        r.workflow_name = "FriendlyName"
    registry = seed(app, records)
    assert {
        r.local_id for r in resolve_records(registry.read().records, "FriendlyName")
    } == {0, 1}
    assert {r.local_id for r in list_registered_submissions(app, "FriendlyName")} == {
        0,
        1,
    }
    assert resolve_workflow_path(records, "FriendlyName") == Path(
        records[0].workflow_path
    )
    with pytest.raises(ValueError, match="Unknown registered workflow name"):
        resolve_records(records, "friendlyname", "name")
    app.Workflow.assert_not_called()


def test_ambiguous_name_does_not_query_or_cancel_any_workflow(app, tmp_path):
    records = [record(tmp_path, i, workflow_id=str(i)) for i in range(2)]
    for r in records:
        r.workflow_name = "duplicate"
    seed(app, records)
    for operation in (collect, cancel, list_registered_submissions):
        with pytest.raises(ValueError, match="Ambiguous workflow name.*ID 0.*ID 1"):
            operation(app, "duplicate")
    assert resolve_workflow_path(records, "0") == Path(records[0].workflow_path)
    app.Workflow.assert_not_called()
    app.get_scheduler.assert_not_called()


def test_numeric_workflow_name_can_be_explicitly_selected(tmp_path):
    records = [record(tmp_path, i, workflow_id=str(i)) for i in range(2)]
    records[1].workflow_name = "0"
    assert resolve_records(records, "0") == [records[0]]
    assert resolve_records(records, "0", "name") == [records[1]]
    assert resolve_workflow_path(records, "0", "name") == Path(records[1].workflow_path)
    with pytest.raises(ValueError, match="Invalid submission ID"):
        resolve_records(records, "name/with/slashes", "id")


def test_name_path_collision_requires_explicit_interpretation(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    relative_path = tmp_path / "conflict"
    relative_path.mkdir()
    r = record(tmp_path)
    r.workflow_name = "conflict"
    with pytest.raises(ValueError, match="--ref-type name or --ref-type path"):
        resolve_records([r], "conflict")
    assert resolve_workflow_path([r], "conflict", "name") == Path(r.workflow_path)
    assert resolve_workflow_path([r], "conflict", "path") == relative_path
    assert resolve_workflow_path([r], "./conflict") == relative_path
    assert resolve_workflow_path([r], Path("conflict")) == relative_path
    assert resolve_workflow_path([r], "conflict", "name") != relative_path


def test_registered_name_can_cancel_missing_workflow(app, tmp_path):
    r = record(tmp_path)
    r.workflow_name = "missing-workflow"
    path = Path(r.workflow_path)
    (path / ".zattrs").unlink()
    path.rmdir()
    seed(app, [r])
    assert cancel(app, "missing-workflow", quiet=True)
    app.get_scheduler.return_value.cancel_jobs.assert_called_once_with(
        js_refs=["100"], quiet=True
    )
    app.Workflow.assert_not_called()


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_workflow_constructor_and_shared_resolver_accept_registered_names(
    tmp_path, monkeypatch, store
):
    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "registry")
    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="named"), path=tmp_path, name_use_dir=True, store=store
    )
    registry = SubmissionRegistry(hf)
    registry.begin(
        SimpleNamespace(workflow=workflow, index=0), "2026-11-01T12:00:00+00:00"
    )
    before = registry.path.read_bytes()
    assert hf._resolve_workflow_reference(workflow.name, None) == Path(workflow.path)
    assert hf._resolve_workflow_reference(workflow.name, "name") == Path(workflow.path)
    assert hf.Workflow(workflow.name).id_ == workflow.id_
    assert hf.Workflow(0).id_ == workflow.id_
    assert registry.path.read_bytes() == before


def test_unregistered_bare_paths_keep_workflow_constructor_behaviour(
    tmp_path, monkeypatch
):
    from hpcflow.sdk.core.errors import WorkflowNotFoundError

    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "registry")
    monkeypatch.chdir(tmp_path)
    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="local"), path=tmp_path, name_add_timestamp=False
    )
    assert hf.Workflow("local").id_ == workflow.id_
    with pytest.raises(WorkflowNotFoundError):
        hf.Workflow("unregistered-and-missing")


def test_terminal_cache_only_fetches_unresolved_runs_and_preserves_iterations(
    app, tmp_path
):
    r = record(tmp_path)
    r.jobs[0].run_ids = [0, 1]
    cache(r, {"0": entry()})
    run = SimpleNamespace(
        id_=1,
        start_time=None,
        end_time=None,
        task=SimpleNamespace(index=0, unique_name="task"),
        element=SimpleNamespace(index=0),
        element_iteration=SimpleNamespace(index=1),
        element_action=SimpleNamespace(action_idx=0),
        index=0,
        status=EARStatus.success,
    )
    workflow = SimpleNamespace(
        _store=SimpleNamespace(cache_ctx=nullcontext),
        get_EARs_from_IDs=Mock(return_value=[run]),
    )
    result = refresh_actions(r, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1])
    assert result.complete
    workflow.get_EARs_from_IDs.reset_mock()
    refresh_actions(r, workflow)
    workflow.get_EARs_from_IDs.assert_not_called()
    assert len(result.groups[0].pattern) == 2


def test_grouping_identical_patterns_and_distinct_orders():
    entries = {}
    for element in range(100):
        entries[str(element * 2)] = entry(element=element)
        entries[str(element * 2 + 1)] = entry("running", element=element, action=1)
    grouped = snapshot_from_json(entries, list(range(200)))
    assert len(grouped.groups) == 1
    assert element_ranges(grouped.groups[0].elements) == "[0–99]"
    entries["0"]["state"], entries["1"]["state"] = "running", "success"
    assert len(snapshot_from_json(entries, list(range(200))).groups) == 2
    assert element_ranges([1, 2, 3, 5, 7, 8]) == "[1–3,5,7–8]"


def test_no_update_writes_neither_registry_nor_cache(app, tmp_path):
    r = record(tmp_path)
    registry = seed(app, [r])
    cache(r, {"0": entry()})
    before = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    collect(app, no_update=True)
    after = {p: p.read_bytes() for p in tmp_path.rglob("*") if p.is_file()}
    assert before == after
    assert registry.read().records[0].activity == "active"


def test_replaced_workflow_never_reuses_cached_actions(app, tmp_path):
    r = record(tmp_path, activity="inactive")
    seed(app, [r])
    cache(r, {"0": entry()})
    atomic_json(Path(r.workflow_path) / ".zattrs", {"creation_info": {"id": "different"}})
    result = collect(app)
    assert result[0].accessibility == "incompatible"
    assert result[0].last_action_summary is None
    app.Workflow.assert_not_called()


def test_bad_cache_rebuilt(app, tmp_path):
    r = record(tmp_path)
    cache_path(r).parent.mkdir(parents=True)
    cache_path(r).write_text("broken", encoding="utf-8")
    run = SimpleNamespace(
        id_=0,
        start_time=None,
        end_time=None,
        task=SimpleNamespace(index=0, unique_name="task"),
        element=SimpleNamespace(index=0),
        element_iteration=SimpleNamespace(index=0),
        element_action=SimpleNamespace(action_idx=0),
        index=0,
        status=EARStatus.error,
    )
    workflow = SimpleNamespace(
        _store=SimpleNamespace(cache_ctx=nullcontext),
        get_EARs_from_IDs=Mock(return_value=[run]),
    )
    assert refresh_actions(r, workflow).counts == {"error": 1}
    assert (
        json.loads(zstd.ZstdDecompressor().decompress(cache_path(r).read_bytes()))[
            "terminal_states"
        ]["0"]["state"]
        == "error"
    )


def test_legacy_import_preserves_ids_and_file(app):
    app._parse_known_submissions_line = hf._parse_known_submissions_line
    app.known_subs_file_path.write_text("42::wf::1::0::2026-10-31 10:00:00::C:/old::::\n")
    original = app.known_subs_file_path.read_bytes()
    registry = SubmissionRegistry(app)
    assert registry.read().records[0].local_id == 42
    assert registry.read().next_local_id == 43
    with registry.transaction():
        pass
    assert app.known_subs_file_path.read_bytes() == original


def test_cli_options_do_not_conflict_and_pass_reference():
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    command = _make_show_CLI(app)
    result = CliRunner().invoke(command, ["42", "-r", "0", "--ref-type", "id", "--full"])
    assert result.exit_code == 0, result.output
    app.show.assert_called_once_with(
        max_recent=0,
        full=True,
        no_update=False,
        workflow_ref="42",
        ref_type="id",
        include_loop_skipped=False,
        list_only=False,
        use_cache=False,
        ignore_scheduler_interval=False,
        columns=None,
        template_names=None,
        group_elements=True,
        group_iterations=True,
        status=True,
        include_missing=False,
        view=None,
    )
    assert CliRunner().invoke(command, ["-r", "-1"]).exit_code != 0
    app.show.reset_mock()
    assert CliRunner().invoke(command, ["--include-loop-skipped"]).exit_code == 0
    assert app.show.call_args.kwargs["include_loop_skipped"] is True
    app.show.reset_mock()
    assert CliRunner().invoke(command, ["--list"]).exit_code == 0
    assert app.show.call_args.kwargs["list_only"] is True
    app.show.reset_mock()
    assert (
        CliRunner().invoke(command, ["FriendlyName", "--ref-type", "name"]).exit_code == 0
    )
    assert app.show.call_args.kwargs["ref_type"] == "name"
    app.cancel = Mock()
    result = CliRunner().invoke(
        _make_cancel_CLI(app), ["FriendlyName", "--ref-type", "name"]
    )
    assert result.exit_code == 0, result.output
    assert app.cancel.call_args.kwargs["ref_is_path"] == "name"


def test_list_registered_submissions_is_complete_and_read_only(app, tmp_path):
    records = [record(tmp_path, i, "inactive", str(i)) for i in range(7)]
    for i, r in enumerate(records):
        r.submitted_at = f"2026-11-01T12:00:{i:02}+00:00"
    records[5].activity = "unknown"
    records[6].activity = "active"
    path = Path(records[0].workflow_path)
    (path / ".zattrs").unlink()
    path.rmdir()
    (Path(records[1].workflow_path) / ".zattrs").write_text("incompatible")
    registry = seed(app, records)
    before = registry.path.read_bytes()
    # Neither the default display count nor a newly lowered retention setting
    # may hide or remove registered records during a listing.
    app.config["show_max_recent"] = 0
    app.config["known_submissions_max_inactive"] = 0
    assert [
        r.local_id for r in list_registered_submissions(app, include_missing=True)
    ] == [
        6,
        5,
        4,
        3,
        2,
        1,
        0,
    ]
    assert [r.local_id for r in list_registered_submissions(app, max_recent=1)] == [
        6,
        5,
        4,
    ]
    assert [r.local_id for r in list_registered_submissions(app, max_recent=0)] == [
        6,
        5,
    ]
    assert registry.path.read_bytes() == before
    app.Workflow.assert_not_called()
    app.get_scheduler.assert_not_called()


def test_list_registered_submissions_filters_all_parts_of_workflow(app, tmp_path):
    records = [
        record(tmp_path, 0),
        record(tmp_path, 1),
        record(tmp_path, 2, workflow_id="other"),
    ]
    records[1].submission_index = 1
    seed(app, records)
    assert {r.local_id for r in list_registered_submissions(app, "0")} == {0, 1}
    assert {
        r.local_id
        for r in list_registered_submissions(app, records[0].workflow_path, "path")
    } == {0, 1}
    assert list_registered_submissions(app, tmp_path / "unregistered", "path") == []
    app.Workflow.assert_not_called()


def test_show_list_cli_bypasses_live_monitoring(tmp_path, monkeypatch):
    from hpcflow.sdk.monitoring import status

    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "history")
    records = [record(tmp_path, i, "inactive", f"reg{i}") for i in range(6)]
    registry = seed(hf, records)
    before = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    collect_mock = Mock(side_effect=AssertionError("List must not collect live states"))
    workflow_mock = Mock(side_effect=AssertionError("List must not load workflows"))
    scheduler_mock = Mock(side_effect=AssertionError("List must not query schedulers"))
    monkeypatch.setattr(status, "collect", collect_mock)
    monkeypatch.setattr(type(hf), "Workflow", property(lambda self: workflow_mock))
    monkeypatch.setattr(hf, "get_scheduler", scheduler_mock)
    result = CliRunner().invoke(_make_show_CLI(hf), ["--list"], env={"COLUMNS": "300"})
    assert result.exit_code == 0, result.output
    assert "Last activity" in result.output
    assert "Submission" in result.output
    assert all(f"reg{i}" in result.output for i in range(6))
    assert registry.path.exists()
    assert before == {
        path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()
    }
    collect_mock.assert_not_called()
    workflow_mock.assert_not_called()
    scheduler_mock.assert_not_called()


def test_list_empty_registry_creates_no_files(app, tmp_path):
    records = list_registered_submissions(app)
    assert records == []
    output = StringIO()
    render_registered(records, console=Console(file=output))
    assert output.getvalue().strip() == "No registered submissions."
    assert not app.user_data_hostname_dir.exists()


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize(
    "use_dir, timestamp_format", [(True, None), (True, "%Y%m%d-%H%M%S"), (False, None)]
)
def test_registered_workflow_uses_stored_name_with_configured_directory_layout(
    app, tmp_path, modifiable_config, store, use_dir, timestamp_format
):
    hf.config.set("workflow_name_use_dir", use_dir)
    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="template-name"),
        name="display-label",
        path=tmp_path,
        store=store,
        ts_name_fmt=timestamp_format,
    )
    if use_dir:
        assert Path(workflow.path).name not in (workflow.name, "display-label")
    registry = SubmissionRegistry(app)
    registry.begin(
        SimpleNamespace(workflow=workflow, index=0), "2026-11-01T12:00:00+00:00"
    )
    stored = registry.read().records[0]
    assert stored.workflow_name == workflow.name
    assert stored.workflow_name_from_metadata
    assert (
        list_registered_submissions(app, include_missing=True)[0].workflow_name
        == workflow.name
    )
    for renderer in (render, render_registered):
        output = StringIO()
        renderer([stored], console=Console(file=output, width=300))
        assert workflow.name in output.getvalue()
    app.Workflow.assert_not_called()


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("timestamp_format", [None, "%Y%m%d-%H%M%S"])
def test_existing_cached_workflow_name_backfilled_from_header(
    app, tmp_path, store, timestamp_format
):
    from hpcflow.sdk.monitoring.status import check_identity

    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="named-workflow"),
        name_use_dir=True,
        path=tmp_path,
        store=store,
        ts_name_fmt=timestamp_format,
    )
    r = new_record(str(workflow.path), workflow.id_, 0, "2026-11-01T12:00:00+00:00")
    r.local_id = 0
    r.activity = "inactive"
    r.jobs = [JobRecord.from_json({"jobscript_index": 0, **{"run_ids": [0]}})]
    check_identity(r)
    # Simulate an existing record whose header token is already cached, but
    # which still contains the timestamp-only basename written by old code.
    r.workflow_name = Path(workflow.path).name
    r.workflow_name_from_metadata = False
    cache(r, {"0": entry()})
    registry = seed(app, [r])
    result = collect(app)
    assert result[0].workflow_name == workflow.name
    assert registry.read().records[0].workflow_name == workflow.name
    header = Path(workflow.path) / (".zattrs" if store == "zarr" else "metadata.json")
    header.unlink()
    assert (
        list_registered_submissions(app, include_missing=True)[0].workflow_name
        == workflow.name
    )
    app.Workflow.assert_not_called()


def test_list_recovers_legacy_timestamp_directory_name_without_workflow_access(
    app, tmp_path
):
    path = tmp_path / "old-workflow" / "2026-10-31_224531"
    r = new_record(str(path), "workflow-id", 0, "2026-11-01T12:00:00+00:00")
    r.local_id = 0
    r.activity = "inactive"
    r.workflow_name = path.name
    r.workflow_name_from_metadata = False
    registry = seed(app, [r])
    before = registry.path.read_bytes()
    assert (
        list_registered_submissions(app, include_missing=True)[0].workflow_name
        == "old-workflow_2026-10-31_224531"
    )
    assert registry.path.read_bytes() == before
    assert not path.exists()
    app.Workflow.assert_not_called()


def test_explicit_workflow_name_overrides_timestamp_path_inference():
    from hpcflow.sdk.monitoring.naming import registered_workflow_name

    r = new_record(
        "parent/2026-10-31_224531",
        "workflow-id",
        0,
        "now",
        workflow_name="2026-10-31_224531",
    )
    assert registered_workflow_name(r) == "2026-10-31_224531"


@pytest.mark.parametrize("full", [False, True])
@pytest.mark.parametrize(
    "reason",
    [
        SkipReason.LOOP_TERMINATION,
        SkipReason.UPSTREAM_FAILURE,
        SkipReason.TASK_CONDITION_NOT_MET,
    ],
)
def test_only_loop_termination_skips_hidden_by_default(full, reason):
    entries = {
        "0": entry(),
        "1": {**entry("skipped", iteration=1), "skip_reason": reason.value},
    }
    summary = snapshot_from_json(entries, [0, 1])
    output = StringIO()
    Console(file=output, width=160).print(action_text(summary, full))
    symbol = {
        SkipReason.LOOP_TERMINATION: "L",
        SkipReason.UPSTREAM_FAILURE: "s",
        SkipReason.TASK_CONDITION_NOT_MET: "C",
    }[reason]
    has_skip_icon = bool(
        re.search(rf"(?<!\w){symbol}(?::\d+)?(?=\s|$)", output.getvalue())
    )
    assert has_skip_icon is (reason is not SkipReason.LOOP_TERMINATION)
    output = StringIO()
    Console(file=output, width=160).print(
        action_text(summary, full, include_loop_skipped=True)
    )
    assert re.search(rf"(?<!\w){symbol}(?::\d+)?(?=\s|$)", output.getvalue())
    assert summary.complete
    assert summary.counts == {"success": 1, "skipped": 1}


def test_filter_loop_skips_before_grouping_elements():
    entries = {
        "0": entry(element=0),
        "1": {
            **entry("skipped", element=0, iteration=1),
            "skip_reason": SkipReason.LOOP_TERMINATION.value,
        },
        "2": entry(element=1),
    }
    summary = snapshot_from_json(entries, [0, 1, 2])
    assert len(summary.groups) == 2
    assert summary.without_loop_skipped.groups[0].elements == [0, 1]
    assert len(summary.without_loop_skipped.groups) == 1


def test_loop_skips_do_not_hide_other_actions_in_same_iteration():
    entries = {
        "0": entry(),
        "1": {
            **entry("skipped", action=1),
            "skip_reason": SkipReason.LOOP_TERMINATION.value,
        },
        "2": {
            **entry("skipped", action=2),
            "skip_reason": SkipReason.UPSTREAM_FAILURE.value,
        },
    }
    filtered = snapshot_from_json(entries, [0, 1, 2]).without_loop_skipped
    assert filtered.counts == {"success": 1, "skipped": 1}
    assert [action.action for action in filtered.groups[0].pattern] == [0, 2]


def test_entirely_loop_skipped_actions_are_hidden_without_affecting_completion():
    summary = snapshot_from_json(
        {"0": {**entry("skipped"), "skip_reason": SkipReason.LOOP_TERMINATION.value}}, [0]
    )
    assert summary.complete
    assert summary.without_loop_skipped == ActionSummary.from_json(
        {"counts": {}, "display_counts": {}, "groups": []}
    )
    assert action_text(summary, full=False).plain == ""
    assert action_text(summary, full=False, include_loop_skipped=True).plain == "L:1"


def test_old_skip_cache_refreshes_only_unclassified_skipped_runs(tmp_path):
    r = record(tmp_path)
    r.jobs[0].run_ids = [0, 1]
    cache(r, {"0": entry(), "1": entry("skipped", iteration=1)})
    run = SimpleNamespace(
        id_=1,
        start_time=None,
        end_time=None,
        task=SimpleNamespace(index=0, unique_name="task"),
        element=SimpleNamespace(index=0),
        element_iteration=SimpleNamespace(index=1),
        element_action=SimpleNamespace(action_idx=0),
        index=0,
        status=EARStatus.skipped,
        skip_reason=SkipReason.LOOP_TERMINATION,
    )
    workflow = SimpleNamespace(
        _store=SimpleNamespace(cache_ctx=nullcontext),
        get_EARs_from_IDs=Mock(return_value=[run]),
    )
    summary = refresh_actions(r, workflow)
    workflow.get_EARs_from_IDs.assert_called_once_with([1])
    assert summary.without_loop_skipped.counts == {"success": 1}
    assert (
        json.loads(zstd.ZstdDecompressor().decompress(cache_path(r).read_bytes()))[
            "terminal_states"
        ]["1"]["skip_reason"]
        == SkipReason.LOOP_TERMINATION.value
    )
    workflow.get_EARs_from_IDs.reset_mock()
    refresh_actions(r, workflow)
    workflow.get_EARs_from_IDs.assert_not_called()


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_real_skip_reasons_cached_and_filtered_without_reloading(app, tmp_path, store):
    from hpcflow.sdk.monitoring.registry import job_record

    task = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[hf.ValueSequence(path="resources.any.resources_id", values=[0, 1, 2])],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="skip-monitoring", path=tmp_path, tasks=[task], store=store
    )
    sub = workflow.add_submission(status=False)
    ids = list(sub.all_EAR_IDs)
    assert len(ids) >= 3
    workflow._store.set_EAR_skip({i: SkipReason.UPSTREAM_FAILURE.value for i in ids[1:]})
    workflow._store.set_EAR_skip({ids[0]: SkipReason.LOOP_TERMINATION.value})
    r = new_record(
        str(workflow.path), workflow.id_, sub.index, "2026-11-01T12:00:00+00:00"
    )
    r.local_id = 0
    r.activity = "inactive"
    r.jobs = [job_record(job) for job in sub.jobscripts]
    seed(app, [r])
    app.Workflow = Mock(side_effect=hf.Workflow)
    summary = collect(app)[0].last_action_summary
    assert action_text(summary, False).plain == f"s:{len(ids) - 1}"
    assert (
        action_text(summary, False, include_loop_skipped=True).plain
        == f"s:{len(ids) - 1} | L:1"
    )
    app.Workflow.reset_mock()
    assert collect(app)[0].last_action_summary.without_loop_skipped.counts == {
        "skipped": len(ids) - 1
    }
    app.Workflow.assert_not_called()


def test_large_full_display_is_grouped(app, tmp_path):
    r = record(tmp_path)
    entries = {str(i): entry(element=i) for i in range(100)}
    r.last_action_summary = snapshot_from_json(entries, list(range(100)))
    r.accessibility = "readable"
    output = StringIO()
    render(
        [r],
        full=True,
        console=Console(file=output, width=160),
        columns=("id", "name", "status", "actions"),
    )
    assert "[0–99]" in output.getvalue()
    assert len(output.getvalue().splitlines()) < 12


def test_large_iteration_display_compresses_completed_and_pending_ranges():
    pattern = [
        (
            iteration,
            0,
            0,
            "success" if iteration < 23 else "running" if iteration == 23 else "pending",
        )
        for iteration in range(10_000)
    ]
    summary = ActionSummary.from_json(
        {
            "groups": [
                {
                    "task": "system_analysis",
                    "pattern": pattern,
                    "elements": list(range(10)),
                }
            ]
        }
    )
    output = StringIO()
    Console(file=output, width=160).print(action_text(summary, full=True))
    lines = output.getvalue().splitlines()
    assert len(lines) == 3
    assert "system_analysis" in lines[0]
    assert lines[0].index("[0–9]") < lines[0].index("■")
    assert "iters {0–22}" in lines[0]
    assert "iters {23}" in lines[1]
    assert "iters {24–9999}" in lines[2]
    assert lines[0].index("■") == lines[1].index("●") == lines[2].index(".")


@pytest.mark.parametrize("width", [100, 200])
def test_full_icons_align_across_tasks_and_submissions(tmp_path, width):
    records = [record(tmp_path, i, workflow_id=str(i)) for i in range(2)]
    records[0].last_action_summary = ActionSummary.from_json(
        {
            "groups": [
                {
                    "task": "short",
                    "elements": list(range(10)),
                    "pattern": [(0, 0, 0, "success")],
                },
                {
                    "task": "longer_task_name",
                    "elements": [0],
                    "pattern": [(0, 0, 0, "running"), (1, 0, 0, "error")],
                },
            ]
        }
    )
    records[1].last_action_summary = ActionSummary.from_json(
        {
            "groups": [
                {
                    "task": "another_task",
                    "elements": [0, 1, 5, 6, 10],
                    "pattern": [
                        (0, 0, 0, "success"),
                        (1, 0, 0, "error"),
                        (2, 0, 0, "success"),
                    ],
                }
            ]
        }
    )
    for r in records:
        r.accessibility = "readable"
    output = StringIO()
    render(
        records,
        full=True,
        console=Console(file=output, width=width),
        columns=("id", "name", "status", "actions"),
    )
    lines = output.getvalue().splitlines()
    icon_columns = [
        match.start() for line in lines if (match := re.search("[■●E]", line))
    ]
    assert len(icon_columns) == 5
    assert len(set(icon_columns)) == 1
    assert "[0–1,5–6,10]" in output.getvalue()
    assert "iters {0,2}" in output.getvalue()
    assert (
        next(line for line in lines if "short" in line).index("[0–9]") < icon_columns[0]
    )


def test_repeated_iteration_patterns_preserve_action_order_and_colours():
    pattern = [
        (iteration, action, 0, state)
        for iteration, states in enumerate(
            [
                ("success", "running"),
                ("success", "running"),
                ("running", "success"),
                ("running", "success"),
            ]
        )
        for action, state in enumerate(states)
    ]
    text = iteration_text_from_json(pattern)
    assert text.plain == "iters {0–1}: ■● | {2–3}: ●■"
    assert any(span.style == EARStatus.running.colour for span in text.spans)


def test_iters_label_is_italic_in_text_and_full_table():
    pattern = [(0, 0, 0, "success"), (1, 0, 0, "success")]
    summary = ActionSummary.from_json(
        {"groups": [{"task": "task", "elements": [0], "pattern": pattern}]}
    )
    console = Console()
    for label in (
        iteration_text_from_json(pattern),
        full_action_rows(summary)[0].iterations,
    ):
        assert label.get_style_at_offset(console, 0).italic
        assert not label.get_style_at_offset(console, 6).italic
    output = StringIO()
    Console(file=output, force_terminal=True, color_system="standard", width=160).print(
        action_text(summary, full=True)
    )
    assert "\x1b[3miters\x1b[0m" in output.getvalue()


def test_skip_reasons_have_separate_icons_counts_and_iteration_ranges():
    entries = {
        str(index): {**entry("skipped", iteration=index), "skip_reason": reason.value}
        for index, reason in enumerate(
            (
                SkipReason.UPSTREAM_FAILURE,
                SkipReason.LOOP_TERMINATION,
                SkipReason.TASK_CONDITION_NOT_MET,
            )
        )
    }
    summary = snapshot_from_json(entries, [0, 1, 2])
    assert action_text(summary, full=False).plain == "s:1 | C:1"
    assert (
        action_text(summary, full=False, include_loop_skipped=True).plain
        == "s:1 | L:1 | C:1"
    )
    rows = full_action_rows(summary)
    assert [row.icons.plain for row in rows] == ["s", "L", "C"]
    assert rows[1].icons.spans[0].style == EARStatus.pending.colour
    assert rows[2].icons.spans[0].style == EARStatus.pending.colour
    assert summary.counts == {"skipped": 3}
    assert summary.complete


@pytest.mark.parametrize(
    ("pattern", "expected"),
    [
        ([], ""),
        ([(0, 0, 0, "success"), (0, 1, 0, "pending")], "■."),
        ([(7, 0, 0, "running")], "iters {7}: ●"),
        ([(0, 0, 0, "success"), (2, 0, 0, "success")], "iters {0}: ■ | {2}: ■"),
        ([(0, 0, 0, "success"), (1, 0, 1, "success")], "iters {0}: ■ | {1}: ■"),
        ([(0, 0, 0, "success"), (1, 1, 0, "success")], "iters {0}: ■ | {1}: ■"),
        ([(0, 0, 0, "pending"), (1, 0, 0, "submitted")], "iters {0}: . | {1}: ."),
        (
            [(0, 0, 0, "success"), (1, 0, 0, "error"), (2, 0, 0, "success")],
            "iters {0}: ■ | {1}: E | {2}: ■",
        ),
    ],
)
def test_iteration_ranges_preserve_gaps_attempts_and_distinct_states(pattern, expected):
    assert iteration_text_from_json(pattern).plain == expected


@pytest.mark.parametrize(
    "setting",
    [
        "show_max_recent",
        "known_submissions_max_inactive",
        "known_submissions_max_unknown",
    ],
)
@pytest.mark.parametrize("value", [-1, True, 1.5, "2"])
def test_invalid_config_limits(setting, value, modifiable_config):
    from hpcflow.sdk.config.errors import ConfigChangeValidationError

    with pytest.raises(ConfigChangeValidationError):
        hf.config.set(setting, value)


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_real_workflow_header_and_expired_history(app, tmp_path, store):
    from hpcflow.sdk.monitoring.status import check_identity

    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="monitor"), path=tmp_path, store=store
    )
    r = new_record(str(workflow.path), workflow.id_, 0, "now")
    check_identity(r)
    app.Workflow = Mock(return_value=workflow)
    assert collect(app, workflow_ref=workflow.path) == []


@pytest.mark.parametrize("interrupt", [False, True])
def test_partial_submission_records_references_before_next_job(
    tmp_path, monkeypatch, interrupt
):
    from hpcflow.sdk.core.errors import JobscriptSubmissionFailure, SubmissionFailure

    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "history")
    task = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 100},
        sequences=[hf.ValueSequence(path="resources.any.resources_id", values=[0, 1])],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="partial-monitoring", path=tmp_path, tasks=[task]
    )
    submission = workflow.add_submission(min_jobscripts=False, status=False)
    assert len(submission.jobscripts) == 2
    registry = SubmissionRegistry(hf)
    for indices, shell in submission.get_unique_shells():
        monkeypatch.setattr(shell, "get_version_info", lambda: {})

    first, second = submission.jobscripts

    def submit_first(*args, **kwargs):
        first._set_process_ID(99999999)
        first._set_submit_cmdline(["fake-job"])
        workflow._store._pending.commit_all()
        return "99999999"

    def submit_second(*args, **kwargs):
        saved = registry.read().records
        assert len(saved) == 1
        assert saved[0].jobs[0].scheduler_reference == ProcessReference(
            99999999, ("fake-job",)
        )
        if interrupt:
            raise KeyboardInterrupt()
        raise JobscriptSubmissionFailure(
            "test failure", submit_cmd=[], js_idx=1, js_path="test"
        )

    monkeypatch.setattr(first, "submit", submit_first)
    monkeypatch.setattr(second, "submit", submit_second)
    with pytest.raises(KeyboardInterrupt if interrupt else SubmissionFailure):
        with workflow.batch_update():
            submission.submit(status=None, quiet=True)
    saved = registry.read().records[0]
    assert not saved.submitting
    assert saved.jobs[0].run_ids
    assert submission._submission_parts[saved.submission_part_id] == [first.index]
    assert not hf.known_subs_file_path.exists()


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_real_terminal_cache_avoids_reloading_workflow(app, tmp_path, store):
    from hpcflow.sdk.monitoring.registry import job_record

    task = hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": 100})
    workflow = hf.Workflow.from_template_data(
        template_name="terminal-monitoring", path=tmp_path, tasks=[task], store=store
    )
    sub = workflow.add_submission(status=False)
    ids = list(sub.all_EAR_IDs)
    for run_id in ids:
        workflow._store.set_EAR_start(run_id, None, None)
        workflow._store.set_EAR_end(run_id, 0, True, False)
    r = new_record(str(workflow.path), workflow.id_, 0, "2026-10-31T12:00:00+00:00")
    r.local_id = 0
    r.activity = "inactive"
    r.jobs = [job_record(job) for job in sub.jobscripts]
    seed(app, [r])
    app.Workflow = Mock(side_effect=hf.Workflow)
    result = collect(app)
    assert result[0].last_action_summary.counts == {"success": len(ids)}
    assert len(
        json.loads(zstd.ZstdDecompressor().decompress(cache_path(r).read_bytes()))[
            "terminal_states"
        ]
    ) == len(ids)
    app.Workflow.reset_mock()
    collect(app)
    app.Workflow.assert_not_called()


def test_partial_cancellation_preserves_references_and_attempts_other_contexts(
    app, tmp_path
):
    first = record(tmp_path, 0)
    second = record(tmp_path, 1)
    second.jobs[0].scheduler_type = "sge"
    registry = seed(app, [first, second])
    failing, working = Mock(), Mock()
    failing.cancel_jobs.side_effect = OSError("scheduler unavailable")
    app.get_scheduler.side_effect = lambda name, *args: (
        failing if name == "slurm" else working
    )
    with pytest.raises(RuntimeError, match="scheduler unavailable"):
        cancel(app, "0", quiet=True)
    working.cancel_jobs.assert_called_once_with(js_refs=["101"], quiet=True)
    records = registry.read().records
    assert len(records) == 2
    assert records[0].activity == "active"
    assert records[1].activity == "unknown"
    assert all(record.jobs for record in records)


def test_no_update_refreshes_without_creating_cache(tmp_path):
    r = record(tmp_path)
    run = SimpleNamespace(
        id_=0,
        start_time=None,
        end_time=None,
        task=SimpleNamespace(index=0, unique_name="task"),
        element=SimpleNamespace(index=0),
        element_iteration=SimpleNamespace(index=0),
        element_action=SimpleNamespace(action_idx=0),
        index=0,
        status=EARStatus.success,
    )
    workflow = SimpleNamespace(
        _store=SimpleNamespace(cache_ctx=nullcontext),
        get_EARs_from_IDs=Mock(return_value=[run]),
    )
    assert refresh_actions(r, workflow, no_update=True).complete
    assert not cache_path(r).parent.exists()


def test_cache_write_failure_still_returns_live_action_details(tmp_path, monkeypatch):
    from hpcflow.sdk.monitoring import cache as cache_module

    r = record(tmp_path)
    run = SimpleNamespace(
        id_=0,
        start_time=None,
        end_time=None,
        task=SimpleNamespace(index=0, unique_name="task"),
        element=SimpleNamespace(index=0),
        element_iteration=SimpleNamespace(index=0),
        element_action=SimpleNamespace(action_idx=0),
        index=0,
        status=EARStatus.success,
    )
    workflow = SimpleNamespace(
        _store=SimpleNamespace(cache_ctx=nullcontext),
        get_EARs_from_IDs=Mock(return_value=[run]),
    )
    monkeypatch.setattr(
        cache_module, "atomic_bytes", Mock(side_effect=PermissionError("read only"))
    )
    result = refresh_actions(r, workflow)
    assert result.counts == {"success": 1}
    assert "read only" in result.cache_error


def test_legacy_and_iso_times_sort_consistently():
    from hpcflow.sdk.monitoring.registry import sort_timestamp

    assert sort_timestamp("2026-10-31 12:00:00.000000") == sort_timestamp(
        "2026-10-31T12:00:00+00:00"
    )
    assert sort_timestamp("2026-10-31T12:00:00Z") == sort_timestamp(
        "2026-10-31T12:00:00+00:00"
    )
    assert sort_timestamp("2026-10-31 13:00:00.000000") > sort_timestamp(
        "2026-10-31T12:00:00+00:00"
    )
