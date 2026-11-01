"""Timezone conversion of monitoring timestamps, including DST transitions."""

from datetime import datetime
from io import StringIO
from unittest.mock import Mock
from zoneinfo import ZoneInfo

import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.config.errors import ConfigChangeValidationError
from hpcflow.sdk.monitoring.columns import RenderContext, times_cell
from hpcflow.sdk.monitoring.models import ActionSummary, SubmissionRecord, to_json
from hpcflow.sdk.monitoring.render import render, render_registered
from hpcflow.sdk.monitoring.timezones import configured_timezone, resolve_timezone


@pytest.fixture(autouse=True)
def current_local_time_is_london_winter(monkeypatch):
    monkeypatch.setattr(
        "hpcflow.sdk.monitoring.columns.local_now",
        lambda: datetime(2026, 11, 1, 12, tzinfo=ZoneInfo("Europe/London")),
    )


def record(submit, start, end):
    return SubmissionRecord(
        "unused",
        "workflow-id",
        "example",
        0,
        "part",
        submit,
        started_at=start,
        finished_at=end,
    )


def test_local_default_uses_timezone_rules_for_each_instant(monkeypatch):
    london = ZoneInfo("Europe/London")
    local = Mock(return_value=london)
    monkeypatch.setattr("hpcflow.sdk.monitoring.timezones.get_localzone", local)
    r = record("2026-10-25T00:00:00Z", "2026-10-25T00:30:00Z", "2026-10-25T01:30:00Z")
    before = to_json(r)
    assert configured_timezone({}) is london
    local.reset_mock()
    context = RenderContext()
    assert times_cell(r, context).plain.splitlines() == [
        "submit  2026-10-25 01:00:00 BST (+01:00)",
        "start              01:30:00 BST (+01:00)",
        "end                01:30:00",
    ]
    local.assert_called_once_with()
    assert to_json(r) == before


def test_spring_clock_change():
    r = record("2026-03-29T00:00:00Z", "2026-03-29T00:30:00Z", "2026-03-29T01:30:00Z")
    assert times_cell(
        r, RenderContext(display_timezone=ZoneInfo("Europe/London"))
    ).plain.splitlines() == [
        "submit  2026-03-29 00:00:00",
        "start              00:30:00",
        "end                02:30:00 BST (+01:00)",
    ]


@pytest.mark.parametrize(
    "submit,start,expected_start",
    [
        # Different UTC dates, but the same local date.
        ("2026-07-01T22:30:00Z", "2026-07-02T00:30:00Z", "20:30:00 EDT (-04:00)"),
        # Same UTC date, but different local dates.
        (
            "2026-07-01T03:30:00Z",
            "2026-07-01T05:30:00Z",
            "2026-07-01 01:30:00 EDT (-04:00)",
        ),
    ],
)
def test_date_omission_compares_display_dates(submit, start, expected_start):
    r = record(submit, start, "")
    result = times_cell(
        r, RenderContext(display_timezone=ZoneInfo("America/New_York"))
    ).plain.splitlines()
    assert (
        result[1]
        == "start   " + (" " * 11 if expected_start[2] == ":" else "") + expected_start
    )


@pytest.mark.parametrize("renderer", [render, render_registered])
def test_all_datetime_columns_use_requested_zone(renderer):
    r = record("2026-07-01T00:00:00Z", "2026-07-01T00:05:00Z", "2026-07-01T00:10:00Z")
    output = StringIO()
    renderer(
        [r],
        console=Console(file=output, width=250),
        columns=["submit", "start", "end", "times"],
        display_timezone=resolve_timezone("Asia/Kolkata"),
    )
    text = output.getvalue()
    assert "2026-07-01 05:30:00 IST (+05:30)" in text
    assert "2026-07-01 05:35:00 IST (+05:30)" in text
    assert "2026-07-01 05:40:00 IST (+05:30)" in text
    assert "start              05:35:00" in text
    assert "start              05:35:00 IST (+05:30)" in text


@pytest.mark.parametrize("renderer", [render, render_registered])
@pytest.mark.parametrize("mixed_offsets", [False, True])
def test_times_suffix_decision_uses_current_local_timezone(renderer, mixed_offsets):
    summer = record("2026-07-01T12:00:00Z", "", "")
    other = record(
        "2026-11-01T12:00:00Z" if mixed_offsets else "2026-07-02T12:00:00Z",
        "",
        "",
    )
    output = StringIO()
    renderer(
        [summer, other],
        console=Console(file=output, width=120),
        columns=["times"],
        display_timezone=ZoneInfo("Europe/London"),
    )
    text = output.getvalue()
    if mixed_offsets:
        assert "2026-07-01 13:00:00 BST (+01:00)" in text
        assert "2026-11-01 12:00:00" in text
        assert "GMT" not in text
    else:
        assert "2026-07-01 13:00:00" in text
        assert "2026-07-02 13:00:00" in text
        assert text.count("BST (+01:00)") == 2


def test_missing_and_invalid_times_do_not_require_suffixes():
    r = record("unrecognised timestamp", "2026-07-01T12:00:00Z", "")
    assert times_cell(
        r, RenderContext(display_timezone=ZoneInfo("Europe/London"))
    ).plain.splitlines() == [
        "submit  unrecognised timestamp",
        "start   2026-07-01 13:00:00 BST (+01:00)",
        "end     —",
    ]


@pytest.mark.parametrize("renderer", [render, render_registered])
@pytest.mark.parametrize("current_month", [7, 11])
def test_all_time_columns_compare_with_current_local_time(
    renderer, current_month, monkeypatch
):
    now = Mock(
        return_value=datetime(
            2026, current_month, 1, 12, tzinfo=ZoneInfo("Europe/London")
        )
    )
    monkeypatch.setattr("hpcflow.sdk.monitoring.columns.local_now", now)
    r = record("2026-07-01T12:00:00Z", "2026-07-01T12:05:00Z", "2026-07-01T12:10:00Z")
    r.last_action_summary = ActionSummary(observed_at="2026-07-01T12:15:00Z")
    r.accessibility = "unavailable"
    output = StringIO()
    renderer(
        [r],
        console=Console(file=output, width=300),
        columns=["submit", "start", "end", "times", "actions"],
        display_timezone=ZoneInfo("Europe/London"),
    )
    text = output.getvalue()
    suffix = " BST (+01:00)" if current_month == 11 else ""
    assert "2026-07-01 13:00:00" + suffix in text
    assert "2026-07-01 13:05:00" + suffix in text
    assert "2026-07-01 13:10:00" + suffix in text
    assert "start              13:05:00" + suffix in text
    assert "Cached: 2026-07-01 13:15:00" + suffix in text
    if current_month == 7:
        assert "BST" not in text and "+01:00" not in text
    now.assert_called_once_with()


def test_explicit_timezone_ignores_machine_timezone(monkeypatch):
    local = Mock(side_effect=AssertionError("Unexpected local timezone lookup"))
    monkeypatch.setattr("hpcflow.sdk.monitoring.timezones.get_localzone", local)
    assert configured_timezone({"show_timezone": "UTC"}) == ZoneInfo("UTC")
    assert configured_timezone({"show_timezone": "Europe/London"}) == ZoneInfo(
        "Europe/London"
    )
    local.assert_not_called()


@pytest.mark.parametrize("name", ["", " ", 0, "Invalid/Timezone", "../Europe/London"])
def test_invalid_timezones_are_actionable(name):
    with pytest.raises(ValueError, match="show_timezone"):
        resolve_timezone(name)


@pytest.mark.parametrize("list_only", [False, True])
def test_app_passes_configured_timezone(modifiable_config, monkeypatch, list_only):
    from hpcflow.sdk.monitoring import listing, render as render_module, status

    hf.config.set("show_timezone", "Asia/Kolkata")
    monkeypatch.setattr(status, "collect", Mock(return_value=[]))
    monkeypatch.setattr(listing, "list_registered_submissions", Mock(return_value=[]))
    renderer = Mock()
    monkeypatch.setattr(
        render_module, "render_registered" if list_only else "render", renderer
    )
    hf.show(list_only=list_only)
    assert renderer.call_args.kwargs["display_timezone"] == ZoneInfo("Asia/Kolkata")


def test_invalid_config_timezone_fails_before_collecting(modifiable_config, monkeypatch):
    from hpcflow.sdk.monitoring import status

    hf.config.set("show_timezone", "Invalid/Timezone")
    collect = Mock(side_effect=AssertionError("Unexpected collection"))
    monkeypatch.setattr(status, "collect", collect)
    with pytest.raises(ValueError, match="Unknown show_timezone"):
        hf.show()
    collect.assert_not_called()


@pytest.mark.parametrize("value", ["", 0, [], False])
def test_timezone_config_schema(modifiable_config, value):
    with pytest.raises(ConfigChangeValidationError):
        hf.config.set("show_timezone", value)
