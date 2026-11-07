"""Remote observations, archives, independent grouping, and progress reporting."""

from io import StringIO
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock
from zipfile import ZipFile

from click.testing import CliRunner
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_show_CLI
from hpcflow.sdk.monitoring.action_render import full_action_rows
from hpcflow.sdk.monitoring.cache import read_cache
from hpcflow.sdk.monitoring.columns import (
    DEFAULT_SHOW_COLUMNS,
    RenderContext,
    availability_cell,
    status_cell,
)
from hpcflow.sdk.monitoring.filtering import filter_templates
from hpcflow.sdk.monitoring.models import (
    ActionRunState,
    ActionSummary,
    ElementGroup,
    IterationActionState,
    JobRecord,
    ProcessReference,
    SubmissionRecord,
    TerminalStateCache,
    to_json,
)
from hpcflow.sdk.monitoring.registry import SubmissionRegistry, atomic_json, job_record
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.monitoring.render import render
from hpcflow.sdk.monitoring.status import check_identity, collect, observe_activity
from hpcflow.sdk.submission.enums import JobscriptElementState


@pytest.fixture
def app(tmp_path):
    return SimpleNamespace(
        user_data_hostname_dir=tmp_path / "registry",
        known_subs_file_path=tmp_path / "legacy",
        config={"show_max_recent": 3, "known_submissions_max_inactive": 100},
        submission_logger=logging.getLogger(__name__),
        Workflow=hf.Workflow,
        get_scheduler=Mock(side_effect=AssertionError("Unexpected scheduler query")),
    )


def record(tmp_path):
    return SubmissionRecord(
        str(tmp_path / "workflow"),
        "id",
        "workflow",
        0,
        "part",
        "2026-11-01T12:00:00Z",
        local_id=0,
    )


@pytest.mark.parametrize("reference", [None, "id", "name", "path"])
def test_explicit_workflow_defaults_to_all_registered_submissions(
    app, tmp_path, reference
):
    records = []
    for i in range(4):
        r = record(tmp_path)
        r.local_id = i
        r.submission_index = i
        r.activity = "inactive"
        r.submitted_at = f"2026-11-01T12:0{i}:00Z"
        records.append(r)
    atomic_json(
        Path(records[0].workflow_path) / ".zattrs",
        {"creation_info": {"id": "id"}, "name": "workflow"},
    )
    with SubmissionRegistry(app).transaction() as data:
        data.records = records
    value = {None: None, "id": 0, "name": "workflow", "path": records[0].workflow_path}[
        reference
    ]
    result = collect(
        app, workflow_ref=value, ref_type=reference, no_update=True, include_summary=False
    )
    assert len(result) == (3 if reference is None else 4)
    if reference is not None:
        for limit in (0, 2):
            assert (
                len(
                    collect(
                        app,
                        workflow_ref=value,
                        ref_type=reference,
                        max_recent=limit,
                        no_update=True,
                        include_summary=False,
                    )
                )
                == limit
            )


def test_explicit_archive_keeps_all_submissions_after_completion_is_discovered(
    app, tmp_path
):
    path = tmp_path / "four-submissions.zip"
    records = []
    with ZipFile(path, "w") as archive:
        archive.writestr(
            ".zattrs", json.dumps({"creation_info": {"id": "id"}, "name": "workflow"})
        )
        for i in range(4):
            r = record(tmp_path)
            r.workflow_path = str(path)
            r.local_id = i
            r.submission_index = i
            r.jobs = [JobRecord(0, run_ids=[i])]
            records.append(r)
            cache = TerminalStateCache(
                "id", i, {i: ActionRunState(0, "task", 0, 0, 0, 0, EARStatus.success)}
            )
            archive.writestr(
                f"artifacts/submissions/{i}/tmp/terminal_states.json",
                json.dumps(to_json(cache)),
            )
    with SubmissionRegistry(app).transaction() as data:
        data.records = records
    app.Workflow = Mock(side_effect=AssertionError("Embedded caches should suffice"))
    result = collect(app, workflow_ref=str(path), no_update=True)
    assert len(result) == 4
    assert all(
        r.activity == "inactive" and r.last_action_summary.complete for r in result
    )
    assert len(collect(app, workflow_ref=str(path), max_recent=2, no_update=True)) == 2


@pytest.mark.parametrize("reference", ["123", ProcessReference(123, ("python",))])
def test_foreign_scheduler_does_not_make_workflow_incompatible(app, tmp_path, reference):
    r = record(tmp_path)
    r.jobs = [
        JobRecord(
            0, "direct", "posix", hostname="other-machine", scheduler_reference=reference
        )
    ]
    observe_activity(app, [r])
    assert r.activity == "unknown"
    assert r.scheduler_availability == "remote" and r.scheduler_error is None
    assert "remote" in availability_cell(r, RenderContext()).plain
    app.get_scheduler.assert_not_called()


def test_shared_scheduler_remains_queryable_from_another_host(app, tmp_path):
    r = record(tmp_path)
    r.jobs = [
        JobRecord(
            0,
            "slurm",
            "posix",
            hostname="other-login",
            machine="shared-cluster",
            scheduler_reference="123",
        )
    ]
    app.config["machine"] = "shared-cluster"
    scheduler = Mock(
        get_job_state_info=Mock(return_value={"123": JobscriptElementState.running})
    )
    app.get_scheduler = Mock(return_value=scheduler)
    progress = Mock()
    observe_activity(app, [r], progress)
    assert r.activity == "active" and r.scheduler_error is None
    scheduler.get_job_state_info.assert_called_once_with(js_refs=["123"])
    assert "Querying slurm" in progress.call_args.args[0]


def test_foreign_job_registration_does_not_construct_scheduler():
    class ForeignJob:
        index = 0
        submit_hostname = "other-machine"
        submit_machine = "other-cluster"
        scheduler_name = "slurm"
        scheduler_job_ID = "123"
        os_name = "posix"
        is_array = False
        all_EAR_IDs = [0, 1]

        def _get_submission_scheduler_args(self):
            return {}

        @property
        def scheduler(self):
            raise AssertionError("Cannot construct a foreign scheduler")

        @property
        def scheduler_js_ref(self):
            raise AssertionError("This property also constructs a scheduler")

    saved = job_record(ForeignJob())
    assert saved.scheduler_reference == "123" and saved.hostname == "other-machine"


@pytest.mark.parametrize("archive", [False, True])
def test_read_remote_workflow_and_zip_without_modifying_archive(app, tmp_path, archive):
    task = hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": 100})
    workflow = hf.Workflow.from_template_data(
        template_name="remote-test", path=tmp_path, tasks=[task], store="zarr"
    )
    sub = workflow.add_submission(status=False)
    part = "2026-11-01T12:00:00Z"
    workflow._store.update_at_submit_metadata(
        sub.index, {part: list(range(len(sub.jobscripts)))}
    )
    ids = list(sub.all_EAR_IDs)
    for run_id in ids:
        workflow._store.set_EAR_start(run_id, None, None)
        workflow._store.set_EAR_end(run_id, 0, True, False)
    if archive:
        path = Path(
            workflow.zip(str(tmp_path / "archive.zip"), consolidate=False, status=False)
        )
        before = path.read_bytes()
        with ZipFile(path) as zipped:
            assert ".zattrs" in zipped.namelist()
    else:
        path = Path(workflow.path)
        r = SubmissionRecord(
            str(path), workflow.id_, workflow.name, 0, part, part, local_id=0
        )
        r.jobs = [job_record(job) for job in sub.jobscripts]
        for job in r.jobs:
            job.hostname = "other-machine"
        with SubmissionRegistry(app).transaction() as data:
            data.records = [r]
    messages = []
    results = collect(
        app, workflow_ref=str(path), no_update=True, progress=messages.append
    )
    assert len(results) == 1
    r = results[0]
    assert r.accessibility == "readable", r.detail_error
    assert r.last_action_summary.counts == {"success": len(ids)}
    assert r.last_action_summary.cache_error is None
    assert r.activity == "inactive"
    assert r.scheduler_availability == ("archived" if archive else "remote")
    assert "Loading workflow" in "\n".join(messages)
    assert filter_templates(results, "remote-test") == results
    if archive:
        assert r.workflow_path == str(path)
        assert path.read_bytes() == before
        # Registered archives must use the same reader, not only explicit paths.
        r.local_id = 0
        with SubmissionRegistry(app).transaction() as data:
            data.records = [r]
        assert collect(app, no_update=True)[0].accessibility == "readable"
        assert path.read_bytes() == before
    app.get_scheduler.assert_not_called()


def test_zip_header_identity_and_embedded_terminal_cache(tmp_path):
    r = record(tmp_path)
    r.workflow_path = str(tmp_path / "workflow.zip")

    def write_archive(workflow_id):
        with ZipFile(r.workflow_path, "w") as archive:
            archive.writestr(
                ".zattrs",
                json.dumps(
                    {
                        "creation_info": {"id": workflow_id},
                        "name": "archived-workflow",
                        "template": {"name": "archived-template"},
                    }
                ),
            )
            archive.writestr(
                "artifacts/submissions/0/tmp/terminal_states.json",
                json.dumps(
                    {
                        "schema_version": 1,
                        "workflow_id": workflow_id,
                        "submission_index": 0,
                        "terminal_states": {},
                    }
                ),
            )

    write_archive(r.workflow_id)
    check_identity(r)
    assert r.workflow_name == "archived-workflow"
    assert read_cache(r) is not None
    r.template_name = None
    assert filter_templates([r], "archived-template") == [r]
    write_archive("a-different-workflow")
    with pytest.raises(ValueError, match="replaced"):
        check_identity(r)
    assert read_cache(r) is None


def test_show_progress_can_be_disabled(monkeypatch):
    from hpcflow.sdk.monitoring import (
        app as app_module,
        render as rendering,
        status as collection,
    )

    console = Mock()
    # Use an ordinary context manager so cleanup is checked without rendering a spinner.
    from contextlib import contextmanager

    updates = []

    @contextmanager
    def status_context(message):
        yield SimpleNamespace(update=updates.append)

    console.status.side_effect = status_context
    monkeypatch.setattr(app_module, "Console", lambda: console)

    def collect_progress(*args, progress=None, **kwargs):
        if progress:
            progress("Loading workflow: example")
            progress("Querying slurm scheduler (2 jobs)")
        return []

    monkeypatch.setattr(collection, "collect", collect_progress)
    monkeypatch.setattr(rendering, "render", Mock())
    hf.show(status=True)
    assert updates == ["Loading workflow: example", "Querying slurm scheduler (2 jobs)"]
    console.status.reset_mock()
    updates.clear()
    hf.show(status=False)
    console.status.assert_not_called()
    assert updates == []


@pytest.mark.parametrize(
    "group_elements,group_iterations,count",
    [(True, True, 1), (False, True, 2), (True, False, 3), (False, False, 6)],
)
def test_independent_grouping_and_icon_alignment(
    tmp_path, group_elements, group_iterations, count
):
    summary = ActionSummary(
        groups=[
            ElementGroup(
                "task",
                tuple(IterationActionState(i, 0, 0, "success") for i in range(3)),
                [0, 1],
            )
        ]
    )
    rows = full_action_rows(summary, group_elements, group_iterations)
    assert len(rows) == count
    if not group_elements:
        assert all(row.elements != "[0–1]" for row in rows)
    if not group_iterations:
        assert all("–" not in row.iterations.plain for row in rows)
    r = record(tmp_path)
    r.last_action_summary = summary
    output = StringIO()
    render(
        [r],
        full=True,
        columns=["actions"],
        console=Console(file=output, width=120),
        group_elements=group_elements,
        group_iterations=group_iterations,
    )
    icon_positions = [
        line.index("■") for line in output.getvalue().splitlines() if "■" in line
    ]
    assert len(icon_positions) == count and len(set(icon_positions)) == 1


def test_compact_status_and_optional_availability(tmp_path):
    r = record(tmp_path)
    r.activity = "active"
    r.accessibility = "missing"
    r.scheduler_states = ["running"]
    r.scheduler_error = "command missing"
    assert status_cell(r, RenderContext()).plain == "● active"
    assert (
        availability_cell(r, RenderContext()).plain == "missing / scheduler unavailable"
    )
    assert DEFAULT_SHOW_COLUMNS == ("id", "name", "status", "actions")


def test_cli_grouping_and_progress_options():
    app = SimpleNamespace(show=Mock(), show_legend=Mock())
    command = _make_show_CLI(app)
    result = CliRunner().invoke(
        command,
        ["--no-group", "--no-status", "--columns", "name,status,availability,actions"],
    )
    assert result.exit_code == 0, result.output
    kwargs = app.show.call_args.kwargs
    assert kwargs["group_elements"] is False and kwargs["group_iterations"] is False
    assert kwargs["status"] is False
    app.show.reset_mock()
    result = CliRunner().invoke(command, ["--no-group-iterations"])
    assert result.exit_code == 0, result.output
    assert app.show.call_args.kwargs["group_elements"] is True
    assert app.show.call_args.kwargs["group_iterations"] is False
