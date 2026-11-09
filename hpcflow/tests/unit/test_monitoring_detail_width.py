"""Long range labels surrender width before detail-view status icons."""

from io import StringIO
import re

import pytest
from rich.console import Console
from rich.cells import cell_len

from hpcflow.sdk.monitoring.action_render import action_text
from hpcflow.sdk.monitoring.models import (
    ActionSummary,
    ElementGroup,
    IterationActionState,
    SubmissionRecord,
    to_json,
)
from hpcflow.sdk.monitoring.render import render


def pattern(*states):
    return tuple(
        IterationActionState(0, index, 0, state) for index, state in enumerate(states)
    )


@pytest.mark.parametrize("width", [60, 80, 120, 160])
@pytest.mark.parametrize("column", ["actions", "elements"])
def test_fragmented_element_ranges_keep_icons_visible_and_aligned(width, column):
    rows = [
        SubmissionRecord(
            "unused",
            "first",
            "first-workflow",
            0,
            "part",
            "",
            local_id=1,
            activity="active",
            accessibility="readable",
            last_action_summary=ActionSummary(
                groups=[
                    ElementGroup(
                        "compute", pattern("success", "running"), list(range(0, 2000, 2))
                    ),
                    ElementGroup("validate", pattern("success"), [1, 3, 5]),
                ]
            ),
        ),
        SubmissionRecord(
            "unused",
            "second",
            "second-workflow",
            0,
            "part",
            "",
            local_id=2,
            activity="active",
            accessibility="readable",
            last_action_summary=ActionSummary(
                groups=[
                    ElementGroup("other_task", pattern("success", "running"), [0, 1]),
                ]
            ),
        ),
    ]
    original = to_json(rows)
    output = StringIO()
    render(
        rows,
        columns=["id", "name", "status", column],
        view="detail",
        console=Console(file=output, width=width, color_system=None),
    )
    text = output.getvalue()
    assert text.count("\u25cf") == 2
    assert text.count("\u25a0") == (3 if column == "actions" else 1)
    assert re.search(r"\[[^\n]*\u2026\]", text)
    icon_columns = [
        min(line.index(icon) for icon in ("\u25a0", "\u25cf") if icon in line)
        for line in text.splitlines()
        if "\u25a0" in line or "\u25cf" in line
    ]
    assert len(set(icon_columns)) == 1
    assert all(cell_len(line) <= width for line in text.splitlines())
    assert to_json(rows) == original


def test_detail_table_can_be_reformatted_at_different_widths_without_mutation():
    summary = ActionSummary(
        groups=[
            ElementGroup("task", pattern("success", "running"), list(range(0, 100, 2))),
        ]
    )
    original = to_json(summary)
    table = action_text(summary, full=True, compact=False)
    outputs = []
    for width in (30, 240, 30):
        output = StringIO()
        Console(file=output, width=width, color_system=None).print(table)
        text = output.getvalue()
        assert "\u25a0\u25cf" in text
        outputs.append(text)
    assert "\u2026" in outputs[0]
    assert "\u2026" not in outputs[1]
    assert outputs[0] == outputs[2]
    assert to_json(summary) == original


def test_long_icon_patterns_wrap_without_losing_states():
    summary = ActionSummary(
        groups=[
            ElementGroup(
                "task", pattern(*(["success", "running"] * 30)), list(range(0, 500, 2))
            ),
        ]
    )
    output = StringIO()
    Console(file=output, width=30, color_system=None).print(
        action_text(summary, full=True, compact=False)
    )
    text = output.getvalue()
    assert text.count("\u25a0") == 30
    assert text.count("\u25cf") == 30
