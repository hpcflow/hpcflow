"""Missing-workflow visibility, compressed caches, and aligned scheduler icons."""

from io import StringIO
from datetime import datetime
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from zipfile import ZipFile

from click.testing import CliRunner
import pytest
from rich.console import Console
import zstandard as zstd

from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.monitoring.availability import existing_records
from hpcflow.sdk.monitoring.cache import cache_path, read_cache, write_cache
from hpcflow.sdk.monitoring.listing import list_registered_submissions
from hpcflow.sdk.monitoring.models import (
    ActionRunState,
    JobRecord,
    SubmissionRecord,
    TerminalStateCache,
    to_json,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json
from hpcflow.sdk.monitoring.render import render
from hpcflow.sdk.monitoring.status import collect


@pytest.fixture
def app(tmp_path):
    return SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy",
        config={"show_max_recent": 2, "known_submissions_max_inactive": 100},
        submission_logger=logging.getLogger(__name__),
        Workflow=Mock(side_effect=AssertionError("Unexpected workflow load")),
        get_scheduler=Mock(side_effect=AssertionError("Unexpected scheduler query")),
    )


def record(tmp_path, index=0):
    return SubmissionRecord(
        str(tmp_path / f"workflow-{index}"),
        f"wf-{index}",
        f"workflow-{index}",
        0,
        "part",
        f"2026-11-01T12:0{index}:00Z",
        local_id=index,
        activity="inactive",
    )


def header(record):
    atomic_json(
        Path(record.workflow_path) / ".zattrs",
        {"creation_info": {"id": record.workflow_id}, "name": record.workflow_name},
    )


def terminal_cache(record, run_ids=(0,)):
    return TerminalStateCache(
        record.workflow_id,
        record.submission_index,
        {
            i: ActionRunState(0, "analysis", i, 0, 0, 0, EARStatus.success)
            for i in run_ids
        },
    )


@pytest.mark.parametrize("listing", [False, True])
@pytest.mark.parametrize("include_missing", [False, True])
@pytest.mark.parametrize("legacy_parser", [False, True])
def test_missing_workflows_do_not_consume_recent_slots_or_erase_registry(
    app, tmp_path, listing, include_missing, legacy_parser, monkeypatch
):
    # Windows may return the same wall-clock timestamp for a whole observation.
    # Inactive history must be ordered by its stored times, not this scan's clock.
    monkeypatch.setattr(
        "hpcflow.sdk.monitoring.status.timestamp", lambda: "2026-11-07T13:00:00+00:00"
    )
    if legacy_parser:

        def fromisoformat(value):
            # Reproduce Python 3.10's parser on newer test interpreters too.
            if isinstance(value, str) and value.endswith("Z"):
                raise ValueError("Invalid isoformat string")
            return datetime.fromisoformat(value)

        monkeypatch.setattr(
            "hpcflow.sdk.monitoring.registry.datetime",
            SimpleNamespace(fromisoformat=fromisoformat),
        )
    records = [record(tmp_path, i) for i in range(4)]
    for r in records[:3]:
        header(r)
    registry = SubmissionRegistry(app)
    with registry.transaction() as data:
        data.records = records
    before = registry.path.read_bytes()
    if listing:
        result = list_registered_submissions(
            app, max_recent=2, include_missing=include_missing
        )
    else:
        result = collect(
            app, no_update=True, include_summary=False, include_missing=include_missing
        )
    assert [r.local_id for r in result] == ([3, 2] if include_missing else [2, 1])
    if include_missing:
        assert result[0].accessibility == "missing"
    assert registry.path.read_bytes() == before
    app.Workflow.assert_not_called()
    app.get_scheduler.assert_not_called()


def test_only_new_inactive_observations_receive_current_timestamps(
    app, tmp_path, monkeypatch
):
    from hpcflow.sdk.monitoring.status import observe_activity

    now = "2026-11-07T13:00:00+00:00"
    old = "2026-11-01T12:00:00+00:00"
    monkeypatch.setattr("hpcflow.sdk.monitoring.status.timestamp", lambda: now)
    records = [record(tmp_path, i) for i in range(3)]
    records[0].last_checked_at = old
    records[1].activity = "active"
    records[2].last_checked_at = records[2].inactive_at = old
    observe_activity(app, records)
    assert records[0].last_checked_at == old and records[0].inactive_at == ""
    assert records[1].activity == "inactive"
    assert records[1].last_checked_at == records[1].inactive_at == now
    assert records[2].last_checked_at == records[2].inactive_at == old
    app.get_scheduler.assert_not_called()


def test_restored_workflow_is_visible_despite_old_missing_observation(app, tmp_path):
    r = record(tmp_path)
    r.accessibility = "missing"
    with SubmissionRegistry(app).transaction() as data:
        data.records = [r]
    assert collect(app, no_update=True) == []
    header(r)
    result = collect(app, no_update=True, include_summary=False)
    assert len(result) == 1 and result[0].accessibility == "readable"


def test_permission_error_is_not_treated_as_deletion(tmp_path, monkeypatch):
    r = record(tmp_path)
    monkeypatch.setattr(Path, "stat", Mock(side_effect=PermissionError("access denied")))
    assert existing_records([r]) == [r]
    assert r.accessibility != "missing"


@pytest.mark.parametrize("listing", [False, True])
def test_include_missing_cli_option(listing):
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    result = CliRunner().invoke(
        _make_show_CLI(app), ["--include-missing"] + (["--list"] if listing else [])
    )
    assert result.exit_code == 0, result.output
    assert app.show.call_args.kwargs["include_missing"] is True


def test_compressed_cache_roundtrip_and_concurrent_state_merge(tmp_path):
    r = record(tmp_path)
    original = terminal_cache(r, range(100))
    write_cache(r, original)
    path = cache_path(r)
    assert path.name == "terminal_states.json.zst"
    data = path.read_bytes()
    assert data.startswith(b"\x28\xb5\x2f\xfd")
    assert len(data) < len(json.dumps(to_json(original)).encode())
    assert read_cache(r) == original
    write_cache(r, terminal_cache(r, [100]))
    assert set(read_cache(r).terminal_states) == set(range(101))


def test_complete_legacy_cache_migrates_without_workflow_reads(app, tmp_path):
    r = record(tmp_path)
    header(r)
    r.jobs = [JobRecord(0, run_ids=[0])]
    original = terminal_cache(r)
    legacy = cache_path(r).with_suffix("")
    atomic_json(legacy, to_json(original))
    before = legacy.read_bytes()
    with SubmissionRegistry(app).transaction() as data:
        data.records = [r]
    assert collect(app, no_update=True)[0].last_action_summary.complete
    assert legacy.read_bytes() == before and not cache_path(r).exists()
    assert collect(app)[0].last_action_summary.complete
    assert cache_path(r).exists() and not legacy.exists()
    assert read_cache(r) == original
    app.Workflow.assert_not_called()


def test_failed_cache_write_preserves_legacy_file(tmp_path, monkeypatch):
    from hpcflow.sdk.monitoring import cache as cache_module

    r = record(tmp_path)
    original = terminal_cache(r)
    legacy = cache_path(r).with_suffix("")
    atomic_json(legacy, to_json(original))
    before = legacy.read_bytes()
    monkeypatch.setattr(
        cache_module, "atomic_bytes", Mock(side_effect=OSError("disk full"))
    )
    with pytest.raises(OSError, match="disk full"):
        write_cache(r, terminal_cache(r, [1]))
    assert legacy.read_bytes() == before
    assert read_cache(r) == original
    assert not cache_path(r).exists()


@pytest.mark.parametrize("compressed", [False, True])
def test_archive_cache_formats_are_read_without_writes(tmp_path, compressed):
    r = record(tmp_path)
    r.workflow_path = str(tmp_path / "workflow.zip")
    original = terminal_cache(r)
    data = json.dumps(to_json(original)).encode()
    suffix = ".zst" if compressed else ""
    with ZipFile(r.workflow_path, "w") as archive:
        archive.writestr(
            f"artifacts/submissions/0/tmp/terminal_states.json{suffix}",
            zstd.ZstdCompressor().compress(data) if compressed else data,
        )
    before = Path(r.workflow_path).read_bytes()
    assert read_cache(r) == original
    write_cache(r, terminal_cache(r, [1]))
    assert Path(r.workflow_path).read_bytes() == before


def test_corrupt_compressed_cache_can_fall_back_to_legacy_json(tmp_path):
    r = record(tmp_path)
    original = terminal_cache(r)
    atomic_json(cache_path(r).with_suffix(""), to_json(original))
    cache_path(r).write_bytes(b"invalid compressed data")
    assert read_cache(r) == original
    cache_path(r).with_suffix("").unlink()
    assert read_cache(r) is None


def test_status_text_aligns_after_different_numbers_of_icons(tmp_path):
    records = [record(tmp_path, i) for i in range(3)]
    for r, activity, states in zip(
        records,
        ["active", "unknown", "inactive"],
        [["pending", "running", "errored"], ["running"], []],
    ):
        r.activity = activity
        r.scheduler_states = states
    output = StringIO()
    render(records, columns=["id", "status"], console=Console(file=output, width=100))
    lines = [
        line
        for line in output.getvalue().splitlines()
        if any(word in line for word in ("active", "unknown", "inactive"))
    ]
    assert len(lines) == 3
    offsets = [line.index(r.activity) for line, r in zip(lines, records)]
    assert len(set(offsets)) == 1
    assert lines[0].index("○") < lines[0].index("●") < offsets[0]
    assert lines[1].index("●") < offsets[1]
