"""Configurable table composition, element rollups, and datetime rendering."""

from datetime import timezone
from io import StringIO
import logging
from pathlib import Path
import socket
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.config.errors import ConfigChangeValidationError
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.skip_reason import SkipReason
from hpcflow.sdk.monitoring.cache import cache_path, snapshot
from hpcflow.sdk.monitoring.columns import (
    DEFAULT_LIST_COLUMNS,
    DEFAULT_SHOW_COLUMNS,
    SHOW_COLUMNS,
    RenderContext,
    configured_columns,
    format_datetime,
    status_cell,
    availability_cell,
    times_cell,
    validate_columns,
)
from hpcflow.sdk.monitoring.elements import element_summary, overall_element_state
from hpcflow.sdk.monitoring.models import (
    ActionRunState,
    ActionSummary,
    ElementGroup,
    IterationActionState,
    JobRecord,
    SubmissionRecord,
    TerminalStateCache,
    to_json,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json
from hpcflow.sdk.monitoring.render import render
from hpcflow.sdk.monitoring.status import collect
from hpcflow.sdk.submission.enums import JobscriptElementState


def submission(summary=None, local_id=0):
    return SubmissionRecord(
        "unused",
        f"workflow-{local_id}",
        "example-workflow",
        0,
        "part",
        "2026-11-01T12:00:00+00:00",
        local_id=local_id,
        activity="active",
        accessibility="readable",
        started_at="2026-11-01T12:05:00+00:00",
        last_action_summary=summary,
    )


def pattern(*states):
    return tuple(
        IterationActionState(0, action, 0, state) for action, state in enumerate(states)
    )


@pytest.fixture(autouse=True)
def local_timezone_is_utc(monkeypatch):
    # Keep existing display assertions independent of the test machine's timezone.
    monkeypatch.setattr(
        "hpcflow.sdk.monitoring.timezones.get_localzone", lambda: timezone.utc
    )


@pytest.fixture
def registry_app(tmp_path):
    return SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy.txt",
        config={"show_max_recent": 3, "known_submissions_max_inactive": 100},
        submission_logger=logging.getLogger(__name__),
        Workflow=Mock(side_effect=AssertionError("Unexpected workflow load")),
        get_scheduler=Mock(return_value=Mock(get_job_state_info=Mock(return_value={}))),
    )


@pytest.mark.parametrize("column", ["actions", "elements"])
@pytest.mark.parametrize("full", [False, True])
@pytest.mark.parametrize("cached", [False, True])
def test_state_read_failures_are_visible_without_availability_column(
    column, full, cached
):
    r = submission(
        ActionSummary(
            counts={"success": 1},
            groups=[ElementGroup("task", pattern("success"), [0])],
            observed_at="2026-11-01T12:00:00+00:00",
        )
        if cached
        else None
    )
    r.accessibility = "unreadable"
    r.detail_error = "store is temporarily unavailable"
    output = StringIO()
    render(
        [r],
        columns=["id", column],
        full=full,
        console=Console(file=output, width=180, color_system=None),
    )
    text = output.getvalue()
    assert "States unavailable: store is temporarily unavailable" in text
    assert ("Cached:" in text) == cached
    if cached:
        assert "\u25a0" in text


def test_scheduler_status_icons_keep_colours_order_and_unique_states():
    r = submission()
    r.activity = "active"
    r.scheduler_states = ["running", "pending", "running", "errored"]
    cell = status_cell(r, RenderContext())
    assert cell.plain == "○ ● E active"
    for state in (
        JobscriptElementState.pending,
        JobscriptElementState.running,
        JobscriptElementState.errored,
    ):
        assert any(
            cell.plain[span.start : span.end] == state.symbol
            and span.style == state.colour
            for span in cell.spans
        )


def test_scheduler_icons_remain_visible_for_inaccessible_workflows():
    r = submission()
    r.activity = "active"
    r.accessibility = "missing"
    r.scheduler_states = ["running", "unrecognised"]
    r.scheduler_error = "Another scheduler failed"
    assert status_cell(r, RenderContext()).plain == "● unrecognised active"
    assert (
        availability_cell(r, RenderContext()).plain == "missing / scheduler unavailable"
    )


def test_column_order_exclusion_and_times_layout():
    summary = ActionSummary(
        groups=[ElementGroup("analysis", pattern("success", "running"), [0, 1])]
    )
    r = submission(summary)
    output = StringIO()
    render(
        [r],
        console=Console(file=output, width=180),
        columns=("times", "name", "elements"),
    )
    table = output.getvalue()
    header = next(line for line in table.splitlines() if "Times" in line)
    assert header.index("Times") < header.index("Name") < header.index("Elements")
    assert "Actions" not in header and "Status" not in header and "ID" not in header
    assert "●:2" in table
    assert times_cell(r, RenderContext()).plain.splitlines() == [
        "submit  2026-11-01 12:00:00",
        "start              12:05:00",
        "end     —",
    ]


@pytest.mark.parametrize(
    "value, expected",
    [
        ("2026-11-01 12:00:00.123456", "2026-11-01 12:00:00"),
        ("2026-11-01T12:00:00+01:00", "2026-11-01 11:00:00"),
        ("2026-11-01T12:00:00Z", "2026-11-01 12:00:00"),
        ("", "—"),
        ("unrecognised timestamp", "unrecognised timestamp"),
    ],
)
def test_datetime_columns_normalise_utc_and_handle_missing_values(value, expected):
    assert format_datetime(value) == expected


def test_separate_datetime_columns():
    r = submission()
    r.finished_at = "2026-11-01T12:10:00+00:00"
    output = StringIO()
    render(
        [r], console=Console(file=output, width=200), columns=("submit", "start", "end")
    )
    table = output.getvalue()
    assert "Submit" in table and "Start" in table and "End" in table
    assert "2026-11-01 12:00:00" in table
    assert "2026-11-01 12:05:00" in table
    assert "2026-11-01 12:10:00" in table


@pytest.mark.parametrize(
    "start, end, expected_start, expected_end",
    [
        (
            "2026-11-01T12:05:00+00:00",
            "2026-11-01T12:10:00+00:00",
            "12:05:00",
            "12:10:00",
        ),
        (
            "2026-11-01T23:50:00+00:00",
            "2026-11-02T00:10:00+00:00",
            "23:50:00",
            "2026-11-02 00:10:00",
        ),
        (
            "2026-11-02T00:05:00+00:00",
            "2026-11-02T00:10:00+00:00",
            "2026-11-02 00:05:00",
            "2026-11-02 00:10:00",
        ),
        (
            "2026-11-02T00:05:00+01:00",
            "2026-11-02T00:10:00+01:00",
            "23:05:00",
            "23:10:00",
        ),
    ],
)
def test_times_column_omits_only_dates_matching_submit(
    start, end, expected_start, expected_end
):
    r = submission()
    r.started_at, r.finished_at = start, end
    assert times_cell(r, RenderContext()).plain.splitlines() == [
        "submit  2026-11-01 12:00:00",
        "start   " + (" " * 11 if expected_start[2] == ":" else "") + expected_start,
        "end     " + (" " * 11 if expected_end[2] == ":" else "") + expected_end,
    ]


def test_times_column_keeps_dates_when_submit_date_is_unknown():
    r = submission()
    r.submitted_at = ""
    assert times_cell(r, RenderContext()).plain.splitlines() == [
        "submit  —",
        "start   2026-11-01 12:05:00",
        "end     —",
    ]


def test_element_summary_counts_elements_across_all_iterations():
    summary = ActionSummary(
        groups=[
            ElementGroup(
                "analysis",
                (
                    IterationActionState(0, 0, 0, "success"),
                    IterationActionState(0, 1, 0, "success"),
                    IterationActionState(1, 0, 0, "success"),
                    IterationActionState(1, 1, 0, "running"),
                ),
                [0, 1, 2],
            ),
            ElementGroup("analysis", pattern("error", "skipped"), [3, 4]),
            ElementGroup("analysis", pattern("success", "success"), [5]),
        ]
    )
    elements = element_summary(summary)
    assert elements.counts == {"running": 3, "error": 2, "success": 1}
    assert sum(elements.counts.values()) == 6
    assert all(len(group.pattern) == 1 for group in elements.groups)
    output = StringIO()
    render(
        [submission(summary)],
        full=True,
        console=Console(file=output, width=180),
        columns=("elements",),
    )
    assert "[0–2]" in output.getvalue() and "[3–4]" in output.getvalue()
    assert "iters" not in output.getvalue()


@pytest.mark.parametrize(
    "states, expected",
    [
        (("running", "error"), "error"),
        (("running", "skipped"), "skipped"),
        (("aborted", "pending"), "aborted"),
        (("pending", "running"), "running"),
        (("submitted", "pending"), "submitted"),
        (("prepared", "pending"), "prepared"),
        (("success", "pending"), "pending"),
        (("success", "success"), "success"),
        (("condition_skipped", "success"), "condition_skipped"),
        (("loop_skipped", "success"), "success"),
        (("loop_skipped",), "loop_skipped"),
    ],
)
def test_overall_element_state_preserves_failure_and_progress(states, expected):
    assert overall_element_state(pattern(*states)) == expected


def test_successful_retry_supersedes_prior_failure_per_iteration_and_action():
    assert (
        overall_element_state(
            (
                IterationActionState(0, 0, 0, "error"),
                IterationActionState(0, 0, 1, "success"),
                IterationActionState(1, 0, 0, "success"),
            )
        )
        == "success"
    )


def test_element_column_respects_hidden_loop_skips():
    states = {
        0: ActionRunState(0, "analysis", 0, 0, 0, 0, EARStatus.success),
        1: ActionRunState(
            0,
            "analysis",
            0,
            1,
            0,
            0,
            EARStatus.skipped,
            skip_reason=SkipReason.LOOP_TERMINATION,
        ),
        2: ActionRunState(
            0,
            "analysis",
            1,
            0,
            0,
            0,
            EARStatus.skipped,
            skip_reason=SkipReason.TASK_CONDITION_NOT_MET,
        ),
        3: ActionRunState(
            0,
            "analysis",
            2,
            0,
            0,
            0,
            EARStatus.skipped,
            skip_reason=SkipReason.LOOP_TERMINATION,
        ),
    }
    r = submission(snapshot(states, list(states)))
    assert RenderContext().elements(r).counts == {"success": 1, "condition_skipped": 1}
    assert RenderContext(include_loop_skipped=True).elements(r).counts == {
        "success": 1,
        "condition_skipped": 1,
        "loop_skipped": 1,
    }


def test_configured_columns_preserve_old_defaults_and_allow_reordering():
    assert configured_columns({}) == DEFAULT_SHOW_COLUMNS
    assert configured_columns({}, list_only=True) == DEFAULT_LIST_COLUMNS
    config = {"show_columns": ["elements", "name"], "show_list_columns": ["path", "id"]}
    assert configured_columns(config) == ("elements", "name")
    assert configured_columns(config, list_only=True) == ("path", "id")
    assert configured_columns(config, override=["times", "name"]) == ("times", "name")


@pytest.mark.parametrize("columns", [[], ["invalid"], ["id", "id"], "id,name", [1]])
def test_invalid_column_selections_fail_before_rendering(columns):
    with pytest.raises(ValueError):
        validate_columns(columns)


@pytest.mark.parametrize("setting", ["show_columns", "show_list_columns"])
@pytest.mark.parametrize("value", [[], ["invalid"], [1], "id,name"])
def test_column_config_schema_rejects_invalid_values(modifiable_config, setting, value):
    with pytest.raises(ConfigChangeValidationError):
        hf.config.set(setting, value)


def test_column_config_accepts_all_available_columns(modifiable_config):
    names = list(SHOW_COLUMNS)
    hf.config.set("show_columns", names)
    hf.config.set("show_list_columns", list(reversed(names)))
    assert configured_columns(hf.config) == tuple(names)
    assert configured_columns(hf.config, list_only=True) == tuple(reversed(names))


def test_show_uses_config_and_cli_override(modifiable_config, monkeypatch):
    from hpcflow.sdk.monitoring import status

    hf.config.set("show_columns", ["name", "elements", "times"])
    r = submission(
        ActionSummary(groups=[ElementGroup("analysis", pattern("success"), [0])])
    )
    collect_mock = Mock(return_value=[r])
    monkeypatch.setattr(status, "collect", collect_mock)
    command = _make_show_CLI(hf)
    result = CliRunner().invoke(command, [], env={"COLUMNS": "160"})
    assert result.exit_code == 0, result.output
    assert "Elements" in result.output and "Times" in result.output
    assert collect_mock.call_args.kwargs["include_summary"] is True
    result = CliRunner().invoke(
        command, ["--columns", "submit,name"], env={"COLUMNS": "160"}
    )
    assert result.exit_code == 0, result.output
    assert "Elements" not in result.output and "Times" not in result.output
    assert collect_mock.call_args.kwargs["include_summary"] is False
    assert CliRunner().invoke(command, ["--columns", "id,id"]).exit_code == 2


def test_metadata_only_columns_skip_action_cache_and_store(
    registry_app, tmp_path, monkeypatch
):
    from hpcflow.sdk.monitoring import status

    r = submission()
    r.workflow_path = str(tmp_path / "workflow")
    r.activity = "inactive"
    atomic_json(
        Path(r.workflow_path) / ".zattrs",
        {"creation_info": {"id": r.workflow_id}, "name": r.workflow_name},
    )
    with SubmissionRegistry(registry_app).transaction() as data:
        data.records.append(r)
    cache_mock = Mock(side_effect=AssertionError("Unexpected cache read"))
    monkeypatch.setattr(status, "read_cache", cache_mock)
    assert collect(registry_app, include_summary=False)[0].local_id == r.local_id
    cache_mock.assert_not_called()
    registry_app.Workflow.assert_not_called()


def test_additional_state_columns_do_not_add_scheduler_queries(registry_app, tmp_path):
    records = []
    for index in range(2):
        r = submission(local_id=index)
        r.activity = "active"
        r.workflow_path = str(tmp_path / f"workflow-{index}")
        r.jobs = [
            JobRecord(
                0,
                "slurm",
                "posix",
                hostname=socket.gethostname(),
                scheduler_reference=str(100 + index),
                run_ids=[index],
            )
        ]
        atomic_json(
            Path(r.workflow_path) / ".zattrs",
            {"creation_info": {"id": r.workflow_id}, "name": r.workflow_name},
        )
        atomic_json(
            cache_path(r).with_suffix(""),
            to_json(
                TerminalStateCache(
                    r.workflow_id,
                    0,
                    {index: ActionRunState(0, "analysis", 0, 0, 0, 0, EARStatus.success)},
                )
            ),
        )
        records.append(r)
    with SubmissionRegistry(registry_app).transaction() as data:
        data.records = records
    scheduler = registry_app.get_scheduler.return_value
    scheduler.get_job_state_info.return_value = {
        "100": JobscriptElementState.running,
        "101": JobscriptElementState.running,
    }
    result = collect(registry_app)
    render(
        result,
        full=True,
        columns=("actions", "elements", "times"),
        console=Console(file=StringIO(), width=240),
    )
    scheduler.get_job_state_info.assert_called_once_with(js_refs=["100", "101"])
    registry_app.Workflow.assert_not_called()


def test_list_with_state_and_time_columns_remains_read_only(tmp_path, monkeypatch):
    from hpcflow.sdk.monitoring import status

    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "registry")
    r = submission(
        ActionSummary(
            observed_at="cached-time",
            groups=[ElementGroup("analysis", pattern("success"), [0, 1])],
        )
    )
    registry = SubmissionRegistry(hf)
    with registry.transaction() as data:
        data.records.append(r)
    before = registry.path.read_bytes()
    forbidden = Mock(side_effect=AssertionError("List must not collect live states"))
    monkeypatch.setattr(status, "collect", forbidden)
    result = CliRunner().invoke(
        _make_show_CLI(hf),
        ["--list", "--include-missing", "--columns", "name,elements,times"],
        env={"COLUMNS": "180"},
    )
    assert result.exit_code == 0, result.output
    assert "Elements" in result.output and "Cached: cached-time" in result.output
    assert (
        "submit" in result.output and "start" in result.output and "end" in result.output
    )
    assert registry.path.read_bytes() == before
    forbidden.assert_not_called()
