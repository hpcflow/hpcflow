"""Bound iterative output without losing progress, retries, or diagnostic states."""

from io import StringIO

from rich.console import Console
from rich.table import Table
from rich.text import Text

from hpcflow.sdk.monitoring.action_render import action_text
from hpcflow.sdk.monitoring.compact import compact_action_text, task_progress
from hpcflow.sdk.monitoring.models import (
    ActionSummary,
    ElementGroup,
    IterationActionState,
    SubmissionRecord,
    to_json,
)
from hpcflow.sdk.monitoring.render import render


def step(iteration, action, state, attempt=0):
    return IterationActionState(iteration, action, attempt, state)


def large_summary():
    # Different progress on every element defeated identical-history grouping.
    return ActionSummary(
        groups=[
            ElementGroup(
                "a_very_long_task_name_" * 3,
                tuple(
                    step(i, 0, "success" if i < element else "pending")
                    for i in range(100)
                ),
                [element],
            )
            for element in range(50)
        ]
    )


def test_weighted_progress_retries_sparse_iterations_and_partial_cache():
    summary = ActionSummary(
        groups=[
            ElementGroup(
                "task",
                (
                    step(0, 0, "error"),
                    step(0, 0, "success", 1),
                    step(0, 1, "success"),
                    step(2, 0, "success"),  # Missing action 1 cannot be counted complete.
                    step(8, 0, "condition_skipped"),
                    step(8, 1, "condition_skipped"),
                ),
                [0, 1, 2],
            ),
            ElementGroup("task", (step(0, 0, "pending"), step(0, 1, "pending")), [3]),
        ]
    )
    original = to_json(summary)
    (task,) = task_progress(summary)
    assert task.completed.label == "0\u20131"
    assert task.visible.label == "1\u20133"  # Counts, not max iteration + 1.
    assert task.condition_skipped.label == "0\u20131"
    assert task.counts == {"error": 3, "success": 9, "condition_skipped": 6, "pending": 2}
    assert task.problem_group_count == 0  # Successful retry superseded the failure.
    assert len(task.elements) == 4
    assert to_json(summary) == original


def test_problem_details_are_bounded_and_workflow_task_order_preserved():
    groups = [ElementGroup("done", (step(0, 0, "success"),), [0])]
    groups += [ElementGroup("failed", (step(i, 2, "error"),), [i]) for i in range(100)]
    groups += [ElementGroup("active", (step(0, 0, "running"),), [0])]
    summary = ActionSummary(groups=groups)
    tasks = task_progress(summary)
    assert [t.name for t in tasks] == ["done", "failed", "active"]
    assert len(tasks[1].problems) == 3
    assert tasks[1].problem_group_count == 100
    text = compact_action_text(summary).plain
    assert "+97 more problem groups" in text
    assert "elements [0]; iters {0}; action 2" in text
    assert len(text.splitlines()) < 25


def test_auto_compaction_and_explicit_detail_escape():
    summary = large_summary()
    compact = action_text(summary, full=True)
    assert isinstance(compact, Text)
    assert "Task summary" in compact.plain
    assert "0\u201349 done / 100 visible" in compact.plain
    assert isinstance(action_text(summary, full=True, group_iterations=False), Table)
    assert isinstance(action_text(summary, full=True, group_elements=False), Table)
    small = ActionSummary(groups=[ElementGroup("small", (step(0, 0, "running"),), [0])])
    assert isinstance(action_text(small, full=True), Table)


def test_long_names_do_not_hide_compact_icons_at_realistic_terminal_widths():
    record = SubmissionRecord(
        "unused",
        "id",
        "workflow_name_" * 7,
        0,
        "part",
        "",
        local_id=6,
        activity="active",
        accessibility="readable",
        last_action_summary=large_summary(),
    )
    for width in (100, 140, 209):
        output = StringIO()
        render([record], full=True, console=Console(file=output, width=width))
        text = output.getvalue()
        assert "\u25a0:1225" in text
        assert ".:3775" in text
        assert "Task summary" in text
        assert len(text.splitlines()) < 40


def test_problem_references_do_not_invent_cross_products():
    summary = ActionSummary(
        groups=[
            ElementGroup("task", (step(1, 0, "interrupted"),), [0]),
            ElementGroup("task", (step(2, 0, "interrupted"),), [1]),
        ]
    )
    text = compact_action_text(summary).plain
    assert "elements [0]; iters {1}" in text
    assert "elements [1]; iters {2}" in text
    assert "elements [0,1]" not in text


def test_failures_are_not_hidden_behind_earlier_upstream_skips():
    groups = [ElementGroup("task", (step(i, 0, "skipped"),), [i]) for i in range(10)]
    groups.append(ElementGroup("task", (step(99, 0, "error"),), [99]))
    (task,) = task_progress(ActionSummary(groups=groups))
    assert task.problems[0].state == "error"
    assert task.problems[0].elements == (99,)
    assert task.problem_group_count == 11


def test_automatic_view_preserves_loop_skip_visibility_and_unknown_icons():
    summary = large_summary()
    visible = large_summary()
    summary.without_loop_skipped = visible
    for group in summary.groups:
        group.pattern += (step(100, 0, "loop_skipped"),)
    visible.groups.append(ElementGroup("remote", (step(0, 0, "unknown"),), [0]))
    default = action_text(summary, full=True)
    assert isinstance(default, Text)
    assert "L:" not in default.plain
    assert "?:1" in default.plain
    included = action_text(summary, full=True, include_loop_skipped=True)
    assert isinstance(included, Text)
    assert "L:50" in included.plain
