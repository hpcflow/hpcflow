"""Every view preserves one overall state per element across all iterations."""

from io import StringIO
from unittest.mock import Mock

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.monitoring import listing, status
from hpcflow.sdk.monitoring.columns import prepare_context
from hpcflow.sdk.monitoring.models import (
    ActionSummary,
    ElementGroup,
    IterationActionState,
    SubmissionRecord,
    to_json,
)
from hpcflow.sdk.monitoring.render import render


def record(count=60):
    success = (
        IterationActionState(0, 0, 0, "error"),
        IterationActionState(0, 0, 1, "success"),
        IterationActionState(1, 0, 0, "success"),
    )
    running = (
        IterationActionState(0, 0, 0, "success"),
        IterationActionState(1, 0, 0, "running"),
        IterationActionState(1, 1, 0, "pending"),
    )
    return SubmissionRecord(
        "unused",
        "workflow",
        "long_workflow_name_" * 5,
        0,
        "part",
        "",
        local_id=0,
        activity="active",
        accessibility="readable",
        last_action_summary=ActionSummary(
            groups=[
                ElementGroup("analysis", success, list(range(0, count, 2))),
                ElementGroup("analysis", running, list(range(1, count, 2))),
            ]
        ),
    )


@pytest.mark.parametrize("view", ["counts", "tasks", "detail", "auto"])
def test_element_views_preserve_states_and_compact_fragmented_ranges(view):
    row = record()
    original = to_json(row)
    context = prepare_context([row], ["elements"], False, False, False, view=view)
    assert context.elements(row).counts == {"success": 30, "running": 30}
    assert (id(row) in context.compact_elements) == (view in ("tasks", "auto"))
    output = StringIO()
    render(
        [row],
        columns=["name", "elements"],
        view=view,
        console=Console(file=output, width=100),
    )
    text = output.getvalue()
    assert ("Task element summary" in text) == (view in ("tasks", "auto"))
    if view != "detail":
        assert "\u25a0:30" in text and "\u25cf:30" in text
    assert "iters/element" not in text
    assert "E:" not in text  # A successful retry supersedes the original error.
    assert to_json(row) == original


def test_small_auto_element_view_retains_grouped_detail():
    row = record(2)
    context = prepare_context([row], ["elements"], False, False, False, view="auto")
    assert not context.compact_elements
    assert context.elements(row).counts == {"success": 1, "running": 1}


@pytest.mark.parametrize("list_only", [False, True])
def test_element_task_view_works_in_cli_and_cached_listings(monkeypatch, list_only):
    live = Mock(return_value=[record()])
    saved = Mock(return_value=[record()])
    monkeypatch.setattr(status, "collect", live)
    monkeypatch.setattr(listing, "list_registered_submissions", saved)
    args = ["--view", "tasks", "--columns", "elements", "--no-status"]
    if list_only:
        args.append("--list")
    result = CliRunner().invoke(_make_show_CLI(hf), args)
    assert result.exit_code == 0, result.output
    assert "Task element summary" in result.output
    assert "\u25a0:30" in result.output and "\u25cf:30" in result.output
    assert (saved.call_count, live.call_count) == ((1, 0) if list_only else (0, 1))


def test_both_task_columns_and_activity_qualification():
    row = record()
    row.activity = "unknown"
    context = prepare_context(
        [row], ["actions", "elements"], False, False, False, view="tasks"
    )
    assert context.compact_actions == context.compact_elements == {id(row)}
    assert context.elements(row).counts == {"success": 30, "unknown": 30}
    output = StringIO()
    render(
        [row],
        columns=["actions", "elements"],
        view="tasks",
        console=Console(file=output, width=180),
    )
    assert "Task summary" in output.getvalue()
    assert "Task element summary" in output.getvalue()
    assert "?:30" in output.getvalue()


def test_ungrouping_disables_auto_element_compaction():
    row = record()
    for option in ("group_elements", "group_iterations"):
        context = prepare_context(
            [row], ["elements"], False, False, False, view="auto", **{option: False}
        )
        assert not context.compact_elements


def test_element_task_summary_preserves_workflow_order_for_all_states():
    from hpcflow.sdk.monitoring.compact import compact_element_text
    from hpcflow.sdk.monitoring.elements import element_summary

    names = ["z_first", "b_second", "a_third", "c_fourth"]
    summary = ActionSummary(
        groups=[
            ElementGroup(name, (IterationActionState(0, 0, 0, state),), [0])
            for name, state in zip(names, ["success", "unknown", "interrupted", "error"])
        ]
    )
    text = compact_element_text(element_summary(summary)).plain
    positions = [text.index(name) for name in names]
    assert positions == sorted(positions)
    assert "\u25a0:1" in text and "?:1" in text and "I:1" in text and "E:1" in text
