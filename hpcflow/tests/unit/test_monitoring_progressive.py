"""Progressive tables use the same observations as completed table output."""

from contextlib import nullcontext
from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.monitoring import app as monitoring_app, status
from hpcflow.sdk.monitoring.models import ActionSummary, SubmissionRecord
from hpcflow.sdk.monitoring.status import CollectionProgress


def record(index):
    return SubmissionRecord(
        "unused",
        f"workflow-{index}",
        f"workflow-{index}",
        0,
        "part",
        "",
        local_id=index,
        activity="active",
        accessibility="readable",
    )


@pytest.mark.parametrize("terminal,enabled", [(True, True), (False, True), (True, False)])
def test_show_progressive_output_and_static_fallback(monkeypatch, terminal, enabled):
    monkeypatch.setenv("TERM", "xterm")
    output = StringIO()
    console = Console(file=output, force_terminal=terminal, width=120)
    monkeypatch.setattr(monitoring_app, "Console", lambda: console)
    rows = [record(0), record(1)]

    def collect(*args, **kwargs):
        callback = kwargs["on_update"]
        assert (callback is not None) == (terminal and enabled)
        if callback:
            callback(CollectionProgress(rows, frozenset(map(id, rows)), True))
            assert "Loading..." in output.getvalue()
            assert "Checking..." in output.getvalue()
            kwargs["progress"]("Reading action states: workflow-0")
        rows[0].last_action_summary = ActionSummary(counts={"success": 1})
        if callback:
            callback(CollectionProgress(rows, frozenset([id(rows[1])])))
            assert "\u25a0:1" in output.getvalue()
        rows[1].last_action_summary = ActionSummary(counts={"error": 1})
        if callback:
            callback(CollectionProgress(rows, frozenset()))
        return rows

    collector = Mock(side_effect=collect)
    monkeypatch.setattr(status, "collect", collector)
    hf.show(columns=["name", "status", "actions"], status=enabled)
    collector.assert_called_once()
    assert "\u25a0:1" in output.getvalue()
    assert "E:1" in output.getvalue()
    if not terminal or not enabled:
        assert "Loading..." not in output.getvalue()
        assert output.getvalue().count("workflow-0") == 1


def test_collector_reports_ready_and_failed_rows_without_extra_queries(monkeypatch):
    rows = [record(0), record(1)]
    app = SimpleNamespace(config={}, submission_logger=Mock())
    registry = Mock()
    registry.read.return_value = SimpleNamespace(records=rows)
    monkeypatch.setattr(status, "SubmissionRegistry", lambda app: registry)
    monkeypatch.setattr(status, "existing_records", lambda rows, include_missing: rows)
    observe = Mock()
    monkeypatch.setattr(status, "observe_activity", observe)
    monkeypatch.setattr(status, "check_identity", lambda record: None)
    monkeypatch.setattr(status, "read_cache", lambda record: None)
    workflow = SimpleNamespace(_store=SimpleNamespace(cache_ctx=nullcontext))
    monkeypatch.setattr(status, "load_workflow", lambda *args: workflow)
    refresh = Mock(
        side_effect=[ActionSummary(counts={"success": 1}), OSError("unreadable")]
    )
    monkeypatch.setattr(status, "refresh_actions", refresh)
    updates = []

    def update(progress):
        updates.append(
            (
                progress.checking_activity,
                len(progress.pending),
                [r.accessibility for r in progress.records],
            )
        )

    result = status.collect(app, max_recent=3, no_update=True, on_update=update)
    assert result == rows
    assert [(checking, pending) for checking, pending, _ in updates] == [
        (True, 2),
        (False, 2),
        (False, 1),
        (False, 0),
    ]
    assert updates[-1][2] == ["readable", "unreadable"]
    observe.assert_called_once()
    assert refresh.call_count == 2
    registry.update.assert_not_called()
