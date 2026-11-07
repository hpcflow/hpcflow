"""Infer interruption only for runs belonging to confirmed inactive submissions."""

from io import StringIO

import pytest
from rich.console import Console

from hpcflow.sdk.monitoring.columns import SHOW_COLUMNS, prepare_context
from hpcflow.sdk.monitoring.models import (
    ActionSummary,
    ElementGroup,
    IterationActionState,
    SubmissionRecord,
    to_json,
)
from hpcflow.sdk.monitoring.states import ACTION_STATES


@pytest.mark.parametrize("activity", ["active", "inactive", "unknown"])
@pytest.mark.parametrize("full", [False, True])
@pytest.mark.parametrize("column", ["actions", "elements"])
@pytest.mark.parametrize("include_loop_skipped", [False, True])
def test_interrupted_rendering(activity, full, column, include_loop_skipped):
    visible = ActionSummary(
        counts={"running": 1},
        groups=[ElementGroup("task", (IterationActionState(0, 0, 0, "running"),), [0])],
    )
    summary = ActionSummary(
        counts={"running": 1, "skipped": 1},
        display_counts={"running": 1, "loop_skipped": 1},
        groups=[
            ElementGroup(
                "task",
                (
                    IterationActionState(0, 0, 0, "running"),
                    IterationActionState(1, 0, 0, "loop_skipped"),
                ),
                [0],
            )
        ],
        without_loop_skipped=visible,
    )
    record = SubmissionRecord(
        "unused",
        "workflow-id",
        "workflow",
        0,
        "part",
        "",
        activity=activity,
        accessibility="readable",
        last_action_summary=summary,
    )
    original = to_json(summary)
    context = prepare_context([record], [column], full, include_loop_skipped, False)
    output = StringIO()
    Console(file=output, width=120, color_system=None).print(
        SHOW_COLUMNS[column].render(record, context)
    )
    expected_name = {
        "active": "running",
        "inactive": "interrupted",
        "unknown": "unknown",
    }[activity]
    expected = ACTION_STATES[expected_name]
    assert expected.symbol in output.getvalue()
    for name in ("running", "interrupted", "unknown"):
        if name != expected_name:
            assert ACTION_STATES[name].symbol not in output.getvalue()
    assert to_json(summary) == original
    assert not context.actions(record).complete
    assert not context.actions(record).finished_at
    assert context.actions(record).counts["running"] == 1


def test_interrupted_element_priority():
    from hpcflow.sdk.monitoring.elements import overall_element_state

    def pattern(*states):
        return tuple(
            IterationActionState(0, i, 0, state) for i, state in enumerate(states)
        )

    assert overall_element_state(pattern("interrupted", "pending")) == "interrupted"
    assert overall_element_state(pattern("interrupted", "error")) == "error"
    assert overall_element_state(pattern("unknown", "pending")) == "unknown"
    assert overall_element_state(pattern("unknown", "error")) == "error"


def test_unknown_activity_preserves_confirmed_terminal_states():
    from hpcflow.sdk.monitoring.action_render import display_summary

    summary = ActionSummary(
        counts={"success": 2, "error": 1, "running": 1, "pending": 1},
        groups=[ElementGroup("task", (IterationActionState(0, 0, 0, "error"),), [0])],
    )
    view = display_summary(summary, unknown=True)
    assert view.display_counts == {"success": 2, "error": 1, "unknown": 1, "pending": 1}
    assert view.groups[0].pattern[0].state == "error"
