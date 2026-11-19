"""Run coordinates, execution columns, direct-ID reads, and scheduler-free arrays."""

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from io import StringIO
import os
import stat
from types import SimpleNamespace
from unittest.mock import Mock
from zoneinfo import ZoneInfo

from click.testing import CliRunner
import numpy as np
import pytest
from rich.console import Console

from hpcflow.app import app as hf
from hpcflow.sdk.cli import _make_workflow_CLI
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.actions import ElementActionRun
from hpcflow.sdk.core.workflow import Workflow
from hpcflow.sdk.monitoring import runs
from hpcflow.sdk.monitoring.run_columns import run_columns
from hpcflow.sdk.monitoring.run_filters import index_selection


@pytest.fixture
def workflow(monkeypatch):
    stamp = datetime(2026, 11, 9, 12, tzinfo=timezone.utc)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return (stamp + timedelta(minutes=15)).astimezone(tz)

    monkeypatch.setattr(runs, "datetime", Clock)
    entries = []
    for index, state in enumerate(
        (EARStatus.error, EARStatus.success, EARStatus.running, EARStatus.pending)
    ):
        entry = SimpleNamespace(
            id_=10 + index,
            status=state,
            task=SimpleNamespace(
                insert_ID=2 if index < 3 else 7,
                index=0 if index < 3 else 1,
                unique_name="analysis" if index < 3 else "post",
            ),
            element=SimpleNamespace(index=3 if index < 2 else 4 if index == 2 else 0),
            element_iteration=SimpleNamespace(
                index=4 if index < 2 else 5 if index == 2 else 0
            ),
            element_action=SimpleNamespace(
                action_idx=5 if index < 2 else 6 if index == 2 else 0
            ),
            action=SimpleNamespace(short_name=lambda: "solve"),
            index=1 if index == 1 else 0,
            submission_idx=0 if index < 3 else None,
            exit_code=2 if index == 0 else 0 if index == 1 else None,
            run_hostname=f"node-{index}" if index < 3 else None,
            start_time=stamp + timedelta(minutes=index * 5) if index < 3 else None,
            end_time=(
                stamp + timedelta(seconds=125)
                if index == 0
                else stamp + timedelta(minutes=5) if index == 1 else None
            ),
        )
        entries.append(entry)
    return SimpleNamespace(
        _app=SimpleNamespace(config={"show_timezone": "UTC"}),
        _store=SimpleNamespace(cached_load=nullcontext, cache_ctx=nullcontext),
        num_EARs=14,
        get_all_EARs=Mock(return_value=entries),
        get_EARs_from_IDs=Mock(
            side_effect=lambda ids: [entry for entry in entries if entry.id_ in ids]
        ),
        submissions=[
            SimpleNamespace(
                jobscripts=[
                    SimpleNamespace(
                        index=7,
                        is_array=True,
                        all_EAR_IDs=[10, 11, 12],
                        blocks=[SimpleNamespace(EAR_ID=np.array([[10, 12], [11, -1]]))],
                    )
                ]
            )
        ],
    )


@pytest.mark.parametrize(
    "filters,expected",
    [
        ({"task_idx": 0}, [10, 11, 12]),
        ({"task_idx": "1"}, [13]),
        ({"task_id": 2}, [10, 11, 12]),
        ({"task_id": "7"}, [13]),
        ({"task_name": "analysis"}, [10, 11, 12]),
        ({"task_name": ["analysis", "post"]}, [10, 11, 12, 13]),
        ({"element_idx": "3"}, [10, 11]),
        ({"iteration_idx": "4-5"}, [10, 11, 12]),
        ({"action_idx": [5]}, [10, 11]),
        ({"run_idx": 1}, [11]),
        ({"run_id": "11,13,11"}, [11, 13]),
        (
            {
                "task_idx": 0,
                "element_idx": 3,
                "iteration_idx": 4,
                "action_idx": 5,
                "run_idx": 1,
            },
            [11],
        ),
        ({"state": "error", "run_idx": 1}, []),
    ],
)
def test_coordinate_filters_combine_and_distinguish_ids_from_indices(
    workflow, filters, expected
):
    assert [
        record.run_id for record in runs.collect_runs(workflow, **filters)
    ] == expected


def test_run_id_filter_reads_only_requested_valid_ids(workflow):
    result = runs.collect_runs(workflow, run_id="11,13,1000000000")
    assert [record.run_id for record in result] == [11, 13]
    workflow.get_EARs_from_IDs.assert_called_once_with([11, 13])
    workflow.get_all_EARs.assert_not_called()


def test_unmatched_id_does_not_read_runs(workflow):
    assert runs.collect_runs(workflow, run_id=1000000000) == []
    workflow.get_EARs_from_IDs.assert_not_called()
    workflow.get_all_EARs.assert_not_called()


def test_large_ranges_merge_without_expansion():
    selected = index_selection("2-1000000000,0,1-3", "indices")
    assert selected.ranges == ((0, 1000000000),)
    assert list(selected.within(4)) == [0, 1, 2, 3]


@pytest.mark.parametrize(
    "filters",
    [
        {"task_idx": "-1"},
        {"element_idx": "5-2"},
        {"iteration_idx": "0,,2"},
        {"run_id": []},
        {"run_idx": [True]},
        {"action_idx": "invalid"},
        {"task_name": ","},
        {"columns": "unknown"},
        {"columns": "attempt,run_idx"},
    ],
)
def test_invalid_filters_and_columns_fail_before_store_reads(workflow, filters):
    workflow._store.cached_load = Mock(
        side_effect=AssertionError("Unexpected store read")
    )
    with pytest.raises(ValueError):
        runs.collect_runs(workflow, **filters)
    workflow._store.cached_load.assert_not_called()


def test_execution_metadata_and_array_index_are_not_task_element_index(workflow):
    records = runs.collect_runs(workflow)
    assert [(r.jobscript, r.array_idx) for r in records] == [
        (7, 0),
        (7, 0),
        (7, 1),
        (None, None),
    ]
    assert records[2].element == 4
    assert records[0].task_idx == 0 and records[0].task_id == 2
    assert [r.host for r in records] == ["node-0", "node-1", "node-2", None]
    assert [r.duration for r in records] == [
        timedelta(seconds=125),
        timedelta(0),
        timedelta(minutes=5),
        None,
    ]


def test_array_indices_and_jobscript_indices_are_scoped_to_submission(workflow):
    workflow.get_all_EARs.return_value[2].submission_idx = 1
    workflow.submissions.append(
        SimpleNamespace(
            jobscripts=[
                SimpleNamespace(
                    index=2,
                    is_array=True,
                    blocks=[SimpleNamespace(EAR_ID=np.array([[-1, -1, 12]]))],
                )
            ]
        )
    )
    records = runs.collect_runs(workflow)
    assert (records[0].submission, records[0].jobscript, records[0].array_idx) == (
        0,
        7,
        0,
    )
    assert (records[2].submission, records[2].jobscript, records[2].array_idx) == (
        1,
        2,
        2,
    )


def test_non_array_job_has_no_array_index(workflow):
    workflow.submissions[0].jobscripts[0].is_array = False
    assert all(record.array_idx is None for record in runs.collect_runs(workflow))


def test_array_mapping_loads_each_block_once(workflow):
    array = workflow.submissions[0].jobscripts[0].blocks[0].EAR_ID
    reads = Mock(return_value=array)

    class Block:
        @property
        def EAR_ID(self):
            return reads()

    workflow.submissions[0].jobscripts[0].blocks = [Block()]
    runs.collect_runs(workflow, columns="run_id,array_idx")
    reads.assert_called_once()


def test_columns_without_job_coordinates_skip_parent_mapping(workflow):
    workflow.submissions = None  # Any parent lookup would fail.
    assert len(runs.collect_runs(workflow, columns="run_id,host,duration")) == 4


def test_columns_show_times_duration_and_host_in_requested_order(workflow):
    output = StringIO()
    runs.show_runs(
        workflow,
        "error",
        Console(file=output, width=220),
        columns="host,run_id,start,end,duration,jobscript,array_idx",
    )
    text = output.getvalue()
    assert (
        text.index("Run host")
        < text.index("Run ID")
        < text.index("Start")
        < text.index("End")
    )
    assert "node-0" in text and "0:02:05" in text
    assert "2026-11-09 12:00:00" in text and "2026-11-09 12:02:05" in text
    assert "Task" not in text


def test_missing_and_zero_duration_are_distinct(workflow):
    output = StringIO()
    console = Console(file=output, width=160)
    runs.show_runs(
        workflow,
        console=console,
        run_id="11,13",
        columns="run_id,host,start,end,duration,array_idx",
    )
    assert "0:00:00" in output.getvalue()
    assert "—" in output.getvalue()


def test_duration_uses_elapsed_utc_across_dst_change(workflow):
    zone = ZoneInfo("Europe/London")
    entry = workflow.get_all_EARs.return_value[0]
    entry.start_time = datetime(2026, 10, 25, 1, 30, tzinfo=zone, fold=0)
    entry.end_time = datetime(2026, 10, 25, 1, 45, tzinfo=zone, fold=1)
    assert runs.collect_runs(workflow, "error")[0].duration == timedelta(minutes=75)


def test_no_matches_describe_coordinate_filter(workflow):
    output = StringIO()
    runs.show_runs(workflow, console=Console(file=output), element_idx=999)
    assert "No runs matching the requested coordinates." in output.getvalue()


def test_column_aliases_and_all():
    assert run_columns("id,attempt,run_host,jobscript_array_idx") == (
        "run_id",
        "run_idx",
        "host",
        "array_idx",
    )
    assert {"host", "start", "end", "duration", "array_idx"} <= set(run_columns("all"))


def snapshot_entry(mode, size, inode):
    return [str(value) for value in (mode, inode, 1, 1, 0, 0, size, 0, 0, 0, inode, 1)]


def snapshot_run(workflow, payload):
    class SnapshotRun(SimpleNamespace):
        snapshot_end = ElementActionRun.snapshot_end

    old = workflow.get_all_EARs.return_value[0]
    entry = SnapshotRun(**vars(old), _snapshot_end=payload, _ss_end_obj=None)
    workflow.get_all_EARs.return_value[0] = entry
    return entry


def test_end_size_uses_saved_snapshot_without_accessing_the_directory(
    workflow, monkeypatch
):
    snapshot_run(
        workflow,
        {
            "use_strings": True,
            "data": {
                ".": snapshot_entry(stat.S_IFDIR, 4096, 1),
                "result.dat": snapshot_entry(stat.S_IFREG, 1234, 2),
                "nested": snapshot_entry(stat.S_IFDIR, 4096, 3),
                "nested/other.dat": snapshot_entry(stat.S_IFREG, 66, 4),
                "link": snapshot_entry(stat.S_IFLNK, 999, 5),
            },
        },
    )
    output = StringIO()
    console = Console(file=output, width=160)
    calculate = Mock(wraps=runs._end_snapshot_size)
    monkeypatch.setattr(runs, "_end_snapshot_size", calculate)
    forbidden = Mock(side_effect=AssertionError("Do not inspect live run directories"))
    with monkeypatch.context() as patch:
        patch.setattr(os, "stat", forbidden)
        patch.setattr(os, "scandir", forbidden)
        runs.show_runs(
            workflow, "error", console, columns="run_id,end_size,end_size_bytes"
        )
    text = output.getvalue()
    assert "1.3 kB" in text and "1300" in text
    calculate.assert_called_once()
    forbidden.assert_not_called()


@pytest.mark.parametrize(
    "payload,expected",
    [
        (None, None),
        ({"data": {}, "use_strings": True}, 0),
        ({"data": {".": snapshot_entry(stat.S_IFDIR, 4096, 1)}, "use_strings": True}, 0),
    ],
)
def test_end_size_distinguishes_missing_snapshot_from_zero(workflow, payload, expected):
    snapshot_run(workflow, payload)
    record = runs.collect_runs(workflow, "error", columns="run_id,end_size")[0]
    assert record.end_size_bytes == expected
    output = StringIO()
    runs.show_runs(workflow, "error", Console(file=output), columns="end_size")
    assert ("—" if expected is None else "0 bytes") in output.getvalue()


def test_end_snapshot_is_not_loaded_when_column_is_not_requested(workflow):
    class SnapshotRun(SimpleNamespace):
        @property
        def snapshot_end(self):
            raise AssertionError("End snapshot is not needed for these columns")

    old = workflow.get_all_EARs.return_value[0]
    workflow.get_all_EARs.return_value[0] = SnapshotRun(**vars(old))
    record = runs.collect_runs(workflow, "error", columns="run_id,state")[0]
    assert record.end_size_bytes is None


def test_cli_passes_coordinate_filters_and_columns():
    wf = Mock(spec=Workflow)
    command = _make_workflow_CLI(hf).commands["show-runs"]
    result = CliRunner().invoke(
        command,
        [
            "--task",
            "0,2",
            "--task-id",
            "2",
            "--task-name",
            "analysis",
            "--element",
            "3-4",
            "--iteration",
            "4",
            "--action-idx",
            "5",
            "--run-id",
            "10,11",
            "--run-idx",
            "0",
            "-c",
            "run_id,run_host,array_idx,duration",
        ],
        obj=wf,
    )
    assert result.exit_code == 0, result.output
    wf.show_runs.assert_called_once_with(
        state=None,
        task_idx="0,2",
        task_id="2",
        task_name="analysis",
        element_idx="3-4",
        iteration_idx="4",
        action_idx="5",
        run_id="10,11",
        run_idx="0",
        columns=("run_id", "host", "array_idx", "duration"),
    )


@pytest.mark.parametrize(
    "args",
    [
        ["--element", "5-2"],
        ["--run-id", "1,,2"],
        ["--task-name", ","],
        ["-c", "unknown"],
        ["-c", "run_idx,attempt"],
    ],
)
def test_invalid_cli_selections_do_not_inspect_runs(args):
    wf = Mock(spec=Workflow)
    result = CliRunner().invoke(
        _make_workflow_CLI(hf).commands["show-runs"], args, obj=wf
    )
    assert result.exit_code == 2
    wf.show_runs.assert_not_called()
