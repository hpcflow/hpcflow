"""Explicit views remain consistent in CLI, SDK, listings, and adaptive rendering."""

from io import StringIO
from unittest.mock import Mock

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.monitoring import app as monitoring_app, listing, status
from hpcflow.sdk.monitoring.models import (
    ActionSummary,
    ElementGroup,
    IterationActionState,
    SubmissionRecord,
)
from hpcflow.sdk.monitoring.render import render
from hpcflow.sdk.monitoring.views import resolve_view


def submission(large=False):
    groups = [
        ElementGroup("task", (IterationActionState(i, 0, 0, "success"),), [i])
        for i in range(50 if large else 1)
    ]
    return SubmissionRecord(
        "unused",
        "workflow",
        "workflow",
        0,
        "part",
        "",
        local_id=0,
        activity="active",
        accessibility="readable",
        last_action_summary=ActionSummary(counts={"success": len(groups)}, groups=groups),
    )


@pytest.mark.parametrize("large", [False, True])
@pytest.mark.parametrize("view", ["counts", "tasks", "detail", "auto"])
def test_explicit_views_and_full_override(large, view):
    output = StringIO()
    render(
        [submission(large)],
        view=view,
        full=True,
        columns=["actions"],
        console=Console(file=output, width=120),
    )
    text = output.getvalue()
    assert ("Task summary" in text) == (view == "tasks" or view == "auto" and large)
    if view == "counts":
        assert "task" not in text
        assert "\u25a0:" in text
    elif view == "detail" or view == "auto" and not large:
        assert "[0]" in text
        assert "task" in text


@pytest.mark.parametrize("list_only", [False, True])
@pytest.mark.parametrize("view", ["counts", "tasks", "detail", "auto"])
def test_cli_views_work_for_live_and_registry_listings(monkeypatch, list_only, view):
    live = Mock(return_value=[submission()])
    saved = Mock(return_value=[submission()])
    monkeypatch.setattr(status, "collect", live)
    monkeypatch.setattr(listing, "list_registered_submissions", saved)
    args = ["--view", view, "--columns", "actions", "--no-status"]
    if list_only:
        args.append("--list")
    result = CliRunner().invoke(_make_show_CLI(hf), args)
    assert result.exit_code == 0, result.output
    assert ("Task summary" in result.output) == (view == "tasks")
    assert (saved.call_count, live.call_count) == ((1, 0) if list_only else (0, 1))


def test_defaults_and_legacy_options():
    assert resolve_view() == "counts"
    assert resolve_view(full=True) == "auto"
    assert resolve_view(group_iterations=False) == "detail"
    assert resolve_view("counts", full=True) == "counts"
    assert resolve_view("detail", full=True) == "detail"


@pytest.mark.parametrize("reference", [None, 0, "1,2", "-1"])
@pytest.mark.parametrize("use_cache", [False, True])
def test_default_view_depends_on_workflow_reference(monkeypatch, reference, use_cache):
    output = StringIO()
    monkeypatch.setattr(
        monitoring_app, "Console", lambda: Console(file=output, width=120)
    )
    monkeypatch.setattr(status, "collect", Mock(return_value=[submission(large=True)]))
    monkeypatch.setattr(
        listing,
        "list_registered_submissions",
        Mock(return_value=[submission(large=True)]),
    )
    hf.show(
        workflow_ref=reference, use_cache=use_cache, columns=["actions"], status=False
    )
    text = output.getvalue()
    assert ("Task summary" in text) == (reference is not None)
    if reference is None:
        assert "\u25a0:50" in text


@pytest.mark.parametrize("args", [["0"], ["-1"], ["1,2"], ["0", "--use-cache"]])
def test_cli_reference_defaults_to_auto_with_explicit_counts_override(monkeypatch, args):
    monkeypatch.setattr(status, "collect", Mock(return_value=[submission(large=True)]))
    monkeypatch.setattr(
        listing,
        "list_registered_submissions",
        Mock(return_value=[submission(large=True)]),
    )
    command = _make_show_CLI(hf)
    result = CliRunner().invoke(command, [*args, "--columns", "actions", "--no-status"])
    assert result.exit_code == 0, result.output
    assert "Task summary" in result.output
    result = CliRunner().invoke(
        command, [*args, "--columns", "actions", "--view", "counts", "--no-status"]
    )
    assert result.exit_code == 0, result.output
    assert "Task summary" not in result.output
    assert "\u25a0:50" in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["--view", "invalid"],
        ["--view", "tasks", "--no-group"],
        ["--view", "counts", "--no-group-elements"],
    ],
)
def test_invalid_cli_view_combinations_do_not_collect(monkeypatch, args):
    collector = Mock()
    monkeypatch.setattr(status, "collect", collector)
    result = CliRunner().invoke(_make_show_CLI(hf), args)
    assert result.exit_code == 2, result.output
    collector.assert_not_called()


def test_invalid_sdk_view_does_not_collect(monkeypatch):
    collector = Mock()
    monkeypatch.setattr(status, "collect", collector)
    with pytest.raises(ValueError, match="Unknown show view"):
        hf.show(view="invalid")
    collector.assert_not_called()


def test_progressive_output_honors_explicit_task_view(monkeypatch):
    from hpcflow.sdk.monitoring import app as monitoring_app
    from hpcflow.sdk.monitoring.status import CollectionProgress

    monkeypatch.setenv("TERM", "xterm")
    output = StringIO()
    console = Console(file=output, force_terminal=True, width=120)
    monkeypatch.setattr(monitoring_app, "Console", lambda: console)
    rows = [submission()]

    def collect(*args, **kwargs):
        kwargs["on_update"](CollectionProgress(rows, frozenset()))
        assert "Task summary" in output.getvalue()  # Rendered before collection ends.
        return rows

    monkeypatch.setattr(status, "collect", collect)
    hf.show(view="tasks", columns=["actions"])
    assert "Exact ranges: --view detail" in output.getvalue()
