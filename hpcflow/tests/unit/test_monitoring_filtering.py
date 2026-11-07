"""Template-name filtering across CLI, live monitoring, and registered history."""

import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

from click.testing import CliRunner
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.monitoring.filtering import filter_templates
from hpcflow.sdk.monitoring.listing import list_registered_submissions
from hpcflow.sdk.monitoring.models import SubmissionRecord, to_json
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json
from hpcflow.sdk.monitoring.status import collect


@pytest.fixture
def app(tmp_path):
    return SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy.txt",
        config={"show_max_recent": 1, "known_submissions_max_inactive": 100},
        submission_logger=logging.getLogger(__name__),
        Workflow=Mock(side_effect=AssertionError("Unexpected workflow load")),
        get_scheduler=Mock(side_effect=AssertionError("Unexpected scheduler query")),
        get_info=lambda: {},
    )


def submission(tmp_path, index, name):
    (tmp_path / str(index)).mkdir(exist_ok=True)
    return SubmissionRecord(
        str(tmp_path / str(index)),
        str(index),
        f"custom-workflow-{index}",
        0,
        "part",
        f"2026-11-01T12:0{index}:00Z",
        local_id=index,
        activity="inactive",
        template_name=name,
    )


@pytest.mark.parametrize("list_only", [False, True])
def test_cli_accepts_trimmed_comma_separated_names(list_only):
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    result = CliRunner().invoke(
        _make_show_CLI(app),
        ["42", "--tname", "alpha, beta,alpha"] + (["--list"] if list_only else []),
    )
    assert result.exit_code == 0, result.output
    assert app.show.call_args.kwargs["template_names"] == ("alpha", "beta")
    assert app.show.call_args.kwargs["workflow_ref"] == "42"
    assert app.show.call_args.kwargs["list_only"] is list_only


@pytest.mark.parametrize("value", ["", " , ", "alpha,", ",alpha"])
def test_cli_rejects_empty_template_names(value):
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    result = CliRunner().invoke(_make_show_CLI(app), ["--tname", value])
    assert result.exit_code == 2
    assert "--tname" in result.output
    app.show.assert_not_called()


@pytest.mark.parametrize("list_only", [False, True])
def test_filter_precedes_recent_limit_and_monitoring(
    app, tmp_path, monkeypatch, list_only
):
    from hpcflow.sdk.monitoring import status

    records = [
        submission(tmp_path, i, name) for i, name in enumerate(["alpha", "beta", "gamma"])
    ]
    # An unrelated active record must also be filtered out.
    records[2].activity = "active"
    with SubmissionRegistry(app).transaction() as data:
        data.records = records
    observe = Mock()
    monkeypatch.setattr(status, "observe_activity", observe)
    if list_only:
        result = list_registered_submissions(app, max_recent=1, template_names="alpha")
        observe.assert_not_called()
    else:
        result = collect(
            app,
            max_recent=1,
            no_update=True,
            include_summary=False,
            template_names=["alpha"],
        )
        assert [r.local_id for r in observe.call_args.args[1]] == [0]
    assert [r.local_id for r in result] == [0]
    app.Workflow.assert_not_called()
    app.get_scheduler.assert_not_called()


def test_filter_combines_with_workflow_reference(app, tmp_path):
    with SubmissionRegistry(app).transaction() as data:
        data.records = [submission(tmp_path, 0, "alpha"), submission(tmp_path, 1, "beta")]
    assert list_registered_submissions(app, 0, "id", template_names="beta") == []
    assert (
        list_registered_submissions(app, 0, "id", template_names="alpha")[0].local_id == 0
    )


def test_older_records_recover_header_name_without_writing_registry(app, tmp_path):
    r = submission(tmp_path, 0, None)
    atomic_json(
        Path(r.workflow_path) / ".zattrs",
        {
            "creation_info": {"id": r.workflow_id},
            "template": {"name": "alpha"},
        },
    )
    registry = SubmissionRegistry(app)
    with registry.transaction() as data:
        data.records = [r]
    original = registry.path.read_bytes()
    result = list_registered_submissions(app, template_names="alpha")
    assert len(result) == 1 and result[0].template_name == "alpha"
    assert registry.path.read_bytes() == original
    app.Workflow.assert_not_called()


def test_missing_or_replaced_older_workflows_do_not_match(tmp_path):
    missing = submission(tmp_path, 0, None)
    Path(missing.workflow_path).rmdir()
    replaced = submission(tmp_path, 1, None)
    atomic_json(
        Path(replaced.workflow_path) / ".zattrs",
        {
            "creation_info": {"id": "replacement"},
            "template": {"name": "alpha"},
        },
    )
    assert filter_templates([missing, replaced], "alpha") == []
    # Saved names remain usable even when a workflow is missing.
    missing.template_name = "alpha"
    assert filter_templates([missing], "alpha") == [missing]


def test_registration_saves_template_name(app, tmp_path):
    workflow = SimpleNamespace(
        path=tmp_path / "custom",
        id_="wf",
        name="custom-name",
        template=SimpleNamespace(name="alpha"),
    )
    registry = SubmissionRegistry(app)
    registry.begin(SimpleNamespace(workflow=workflow, index=0), "part")
    r = registry.read().records[0]
    assert r.template_name == "alpha"
    assert to_json(r)["template_name"] == "alpha"


@pytest.mark.parametrize("list_only", [False, True])
def test_sdk_passes_filter_to_collection(monkeypatch, list_only):
    from hpcflow.sdk.monitoring import listing, render, status

    collector = Mock(return_value=[])
    monkeypatch.setattr(listing, "list_registered_submissions", collector)
    monkeypatch.setattr(status, "collect", collector)
    monkeypatch.setattr(render, "render", Mock())
    monkeypatch.setattr(render, "render_registered", Mock())
    hf.show(list_only=list_only, template_names=["alpha", "beta"])
    if list_only:
        assert collector.call_args.args[4] == ["alpha", "beta"]
    else:
        assert collector.call_args.kwargs["template_names"] == ["alpha", "beta"]
