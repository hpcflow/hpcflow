"""Cached show views reformat registry snapshots without accessing workflows."""

from io import StringIO
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.monitoring import app as monitoring_app, filtering, listing, status
from hpcflow.sdk.monitoring.models import (
    ActionSummary,
    ElementGroup,
    IterationActionState,
    RegistryData,
    SubmissionRecord,
    to_json,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json


@pytest.fixture
def saved(tmp_path, monkeypatch):
    monkeypatch.setattr(hf, "_user_data_hostname_dir", tmp_path / "registry")
    monkeypatch.setattr(
        hf,
        "_config",
        {
            "show_max_recent": 1,
            "show_columns": ["id", "name", "status", "actions"],
            "show_list_columns": ["id", "path"],
            "show_timezone": "UTC",
        },
    )
    rows = [
        SubmissionRecord(
            str(tmp_path / f"workflow-{index}"),
            str(index),
            f"saved-{index}",
            0,
            "part",
            f"2026-11-0{index + 1}T12:00:00+00:00",
            local_id=index,
            activity="active" if index == 0 else "inactive",
            scheduler_states=["running"] if index == 0 else [],
            accessibility="missing" if index == 2 else "readable",
            template_name="alpha" if index % 2 == 0 else None,
            last_action_summary=ActionSummary(
                counts={"success": 2},
                groups=[
                    ElementGroup(
                        "analysis", (IterationActionState(0, 0, 0, "success"),), [0, 1]
                    )
                ],
                observed_at="2026-11-08T12:30:00+00:00",
            ),
        )
        for index in range(6)
    ]
    registry = SubmissionRegistry(hf)
    atomic_json(registry.path, to_json(RegistryData(rows, 6)))
    before = registry.path.read_bytes()
    forbidden = Mock(side_effect=AssertionError("Cached show must not inspect workflows"))
    monkeypatch.setattr(status, "collect", forbidden)
    monkeypatch.setattr(listing, "existing_records", forbidden)
    monkeypatch.setattr(filtering, "_read_template_name", forbidden)
    output = StringIO()
    monkeypatch.setattr(
        monitoring_app,
        "Console",
        lambda: Console(file=output, width=160, color_system=None),
    )
    return registry, before, output, forbidden


@pytest.mark.parametrize("view", ["counts", "tasks", "detail", "auto"])
@pytest.mark.parametrize("column", ["actions", "elements"])
def test_cached_show_reformats_saved_states_without_workflow_io(saved, view, column):
    registry, before, output, forbidden = saved
    hf.show(use_cache=True, columns=["name", column, "times"], view=view)
    text = output.getvalue()
    assert "saved-0" in text and "saved-5" in text
    assert all(f"saved-{index}" not in text for index in range(1, 5))
    assert "Cached: 2026-11-08 12:30:00" in text
    assert "\u25a0" in text
    assert "Loading..." not in text
    forbidden.assert_not_called()
    assert registry.path.read_bytes() == before


def test_cached_template_filter_uses_saved_names_only(saved):
    registry, before, output, forbidden = saved
    hf.show(use_cache=True, template_names="alpha")
    text = output.getvalue()
    assert "saved-0" in text and "saved-4" in text
    assert "saved-5" not in text
    assert "Status (cached)" in text
    assert "\u25cf" in text
    forbidden.assert_not_called()
    assert registry.path.read_bytes() == before


@pytest.mark.parametrize("reference", ["0", "saved-0"])
def test_cached_show_selects_registered_workflow(saved, reference):
    _, _, output, forbidden = saved
    hf.show(use_cache=True, workflow_ref=reference)
    assert "saved-0" in output.getvalue()
    assert "saved-5" not in output.getvalue()
    forbidden.assert_not_called()


@pytest.mark.parametrize("include_missing", [False, True])
def test_cached_missing_filter_uses_saved_availability(saved, include_missing):
    _, _, output, forbidden = saved
    hf.show(use_cache=True, max_recent=99, include_missing=include_missing)
    assert ("saved-2" in output.getvalue()) == include_missing
    forbidden.assert_not_called()


def test_cached_show_without_saved_summary_is_explicit(saved):
    registry, _, output, forbidden = saved
    data = registry.read()
    data.records[0].last_action_summary = None
    atomic_json(registry.path, to_json(data))
    hf.show(use_cache=True, workflow_ref=0)
    assert "No cached action states. Run show to refresh." in output.getvalue()
    forbidden.assert_not_called()


def test_cached_listing_keeps_listing_defaults(saved):
    _, _, output, forbidden = saved
    hf.show(use_cache=True, list_only=True)
    text = output.getvalue()
    assert "Path" in text and "Actions" not in text
    assert all(f"workflow-{index}" in text for index in (0, 1, 3, 4, 5))
    forbidden.assert_not_called()


def test_cli_passes_cache_option_with_formatting_flags():
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    result = CliRunner().invoke(
        _make_show_CLI(app),
        ["42", "--use-cache", "--columns", "name,elements,times", "--view", "tasks"],
    )
    assert result.exit_code == 0, result.output
    assert app.show.call_args.kwargs["use_cache"] is True
    assert app.show.call_args.kwargs["columns"] == ("name", "elements", "times")
    assert app.show.call_args.kwargs["workflow_ref"] == "42"
