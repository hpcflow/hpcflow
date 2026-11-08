"""Workflow readers are bounded, isolated, and publish ready rows on the caller."""

from collections import defaultdict
from threading import Barrier, Event, Lock, get_ident, local
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from hpcflow.sdk.log import TimeIt
from hpcflow.sdk.monitoring import status
from hpcflow.sdk.monitoring.models import ActionSummary, SubmissionRecord


@pytest.mark.parametrize("profiling", [False, True])
def test_parallel_collection_streams_rows_with_at_most_four_workers(
    tmp_path, monkeypatch, profiling
):
    monkeypatch.setattr(TimeIt, "active", profiling)
    monkeypatch.setattr(TimeIt, "timers", defaultdict(list))
    monkeypatch.setattr(TimeIt, "_worker_traces", local())
    for name in ("trace", "trace_idx", "trace_prev", "trace_idx_prev"):
        monkeypatch.setattr(TimeIt, name, [])
    caller = get_ident()
    rows = [
        SubmissionRecord(
            str(tmp_path / f"workflow-{i}"),
            f"id-{i}",
            f"workflow-{i}",
            0,
            "part",
            "",
            local_id=i,
            activity="active",
        )
        for i in range(6)
    ]
    # A second submission from workflow zero must share its worker/cache, but the
    # first row must be published before that worker finishes the second one.
    rows.append(
        SubmissionRecord(
            rows[0].workflow_path,
            rows[0].workflow_id,
            rows[0].workflow_name,
            1,
            "part-2",
            "",
            local_id=6,
            activity="active",
        )
    )
    app = SimpleNamespace(config={}, submission_logger=Mock())
    registry = Mock()
    registry.read.return_value = SimpleNamespace(records=rows)
    registry.update.return_value = set(range(7))
    monkeypatch.setattr(status, "SubmissionRegistry", lambda app: registry)
    monkeypatch.setattr(status, "existing_records", lambda rows, missing: rows)
    queried = Event()

    def observe(*args):
        assert get_ident() == caller
        queried.set()

    observer = Mock(side_effect=observe)
    monkeypatch.setattr(status, "observe_activity", observer)
    first_wave = Barrier(4)
    first_published = Event()
    lock = Lock()
    active_paths = set()
    worker_caches = {}
    peak = 0

    def read(app, row, loaded, include_summary, no_update, progress):
        nonlocal peak
        assert get_ident() != caller
        assert queried.is_set()
        assert rows[row.local_id].last_action_summary is None
        with lock:
            assert row.workflow_path not in active_paths
            active_paths.add(row.workflow_path)
            peak = max(peak, len(active_paths))
            assert peak <= 4
            if row.workflow_path in worker_caches:
                assert worker_caches[row.workflow_path] is loaded
            else:
                assert all(cache is not loaded for cache in worker_caches.values())
                worker_caches[row.workflow_path] = loaded
        if row.local_id < 4:
            first_wave.wait(timeout=10)
        if row.local_id == 6:
            assert first_published.wait(timeout=10)
        row.workflow_name = f"ready-{row.local_id}"
        progress(f"Reading row {row.local_id}")
        row.last_action_summary = ActionSummary(counts={"success": 1})
        with lock:
            active_paths.remove(row.workflow_path)

    monkeypatch.setattr(status, "_collect_record", read)
    updates = []
    messages = []

    def update(event):
        assert get_ident() == caller
        assert [row.local_id for row in event.records] == list(range(7))
        for row in event.records:
            if id(row) in event.pending:
                assert row.last_action_summary is None
                assert not row.workflow_name.startswith("ready-")
            else:
                assert row.last_action_summary.counts == {"success": 1}
        if id(rows[0]) not in event.pending:
            first_published.set()
        updates.append(len(event.pending))

    def progress(message):
        assert get_ident() == caller
        messages.append(message)

    result = status.collect(app, max_recent=3, progress=progress, on_update=update)
    assert result == rows
    assert peak == 4
    assert updates == [7, 7, 6, 5, 4, 3, 2, 1, 0]
    assert len(messages) == 7
    observer.assert_called_once()
    registry.update.assert_called_once()
    assert [row.workflow_name for row in result] == [f"ready-{i}" for i in range(7)]
    if profiling:
        assert len(TimeIt.timers[("collect",)]) == 1
        assert len(TimeIt.timers[("_collect_selected.<locals>.read_group",)]) == 6
        assert not TimeIt.trace
    else:
        assert not TimeIt.timers


def test_parallel_read_failure_preserves_other_rows(tmp_path, monkeypatch):
    rows = [
        SubmissionRecord(str(tmp_path / str(i)), str(i), str(i), 0, "part", "")
        for i in range(2)
    ]
    caller = get_ident()
    app = SimpleNamespace(submission_logger=Mock())

    def check(row):
        if row.workflow_id == "0":
            raise OSError("store unavailable")

    monkeypatch.setattr(status, "check_identity", check)
    updates = []

    def update(event):
        assert get_ident() == caller
        updates.append(len(event.pending))

    status._collect_selected(app, rows, {}, False, True, None, update)
    assert rows[0].accessibility == "unreadable"
    assert rows[0].detail_error == "store unavailable"
    assert rows[1].accessibility == "readable"
    assert updates == [1, 0]
