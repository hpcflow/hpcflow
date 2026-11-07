from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from types import SimpleNamespace

import psutil
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_workflow_to_run_command
from hpcflow.sdk.persistence.zarr import ZarrStoreEAR


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_executor_tracks_memory(enabled, exit_code):
    exe = hf.Executor(
        [
            sys.executable,
            "-c",
            f"import time; x=bytearray(20_000_000); time.sleep(0.25); raise SystemExit({exit_code})",
        ],
        dict(os.environ),
        hf.package_name,
        track_peak_memory=enabled,
        peak_memory_sampling_interval=0.05,
    )
    asyncio.run(exe._subprocess_runner())
    assert exe.return_code == exit_code
    if enabled:
        assert exe.peak_memory_bytes >= 20_000_000
    else:
        assert exe.peak_memory_bytes is None


def test_memory_samples_sum_descendants_and_keep_peak():
    exe = hf.Executor([], {}, hf.package_name, track_peak_memory=True)
    child = SimpleNamespace(memory_info=lambda: SimpleNamespace(rss=200))
    vanished = SimpleNamespace(
        memory_info=lambda: (_ for _ in ()).throw(psutil.NoSuchProcess(1))
    )
    denied = SimpleNamespace(
        memory_info=lambda: (_ for _ in ()).throw(psutil.AccessDenied(2))
    )
    root = SimpleNamespace(
        children=lambda recursive: [child, vanished, denied],
        memory_info=lambda: SimpleNamespace(rss=100),
    )
    exe._sample_memory(root)
    assert exe.peak_memory_bytes == 300
    root.children = lambda recursive: []
    exe._sample_memory(root)
    assert exe.peak_memory_bytes == 300


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("peak", [None, 0, 123456789])
def test_peak_memory_persists(tmp_path: Path, store, peak):
    wk = make_workflow_to_run_command(
        "echo ready",
        path=tmp_path,
        store=store,
        resources={"any": {"track_peak_memory": True}},
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    assert run.resources.track_peak_memory is True
    assert run.peak_memory_bytes is None
    wk.set_EAR_end((0, 0, 0), run, 0, peak_memory_bytes=peak)
    loaded = hf.Workflow(wk.path).get_all_EARs()[0]
    assert loaded.peak_memory_bytes == peak
    assert loaded.success


def test_combined_scripts_warn_and_disable_tracking():
    with pytest.warns(UserWarning, match="track_peak_memory will be ignored"):
        resources = hf.ElementResources(track_peak_memory=True, combine_scripts=True)
    assert resources.combine_scripts is True
    assert resources.track_peak_memory is False


def test_combined_scripts_without_tracking_do_not_warn():
    import warnings

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        resources = hf.ElementResources(track_peak_memory=False, combine_scripts=True)
    assert resources.combine_scripts is True
    assert resources.track_peak_memory is False
    assert not caught


def test_legacy_zarr_run_has_no_peak_memory():
    old_runtime = [0, True, None, None, None, None, 0, "host", None]
    stored = ZarrStoreEAR.decode(
        [0, 0, 0, [], []],
        [0, 0, 0, 0],
        old_runtime,
        "%Y-%m-%d",
        {"parameter_paths": []},
    )
    assert stored.peak_memory_bytes is None


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("exit_code", [0, 7])
def test_run_records_memory(tmp_path, monkeypatch, store, enabled, exit_code):
    command = f'"{sys.executable}" -c "import time; x=bytearray(20000000); time.sleep(0.25); raise SystemExit({exit_code})"'
    if os.name == "nt":
        command = "& " + command
    wk = make_workflow_to_run_command(
        command,
        path=tmp_path,
        store=store,
        resources={
            "any": {
                "track_peak_memory": enabled,
                "peak_memory_sampling_interval": 0.05,
                "shell_args": (
                    {"executable_args": ["-ExecutionPolicy", "Bypass"]}
                    if os.name == "nt"
                    else {}
                ),
            }
        },
    )
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    monkeypatch.chdir(tmp_path)
    wk._execute_run(0, (0, 0, 0), run.id_)
    loaded = hf.Workflow(wk.path).get_all_EARs()[0]
    assert loaded.exit_code == exit_code
    assert loaded.success is (exit_code == 0)
    if enabled:
        assert loaded.peak_memory_bytes >= 20_000_000
    else:
        assert loaded.peak_memory_bytes is None


def test_cancelled_executor_retains_samples():
    exe = hf.Executor(
        [sys.executable, "-c", "import time; time.sleep(30)"],
        dict(os.environ),
        hf.package_name,
        track_peak_memory=True,
    )

    async def run():
        task = asyncio.create_task(exe._subprocess_runner())
        try:
            for _ in range(200):
                if exe.peak_memory_bytes is not None:
                    break
                await asyncio.sleep(0.01)
            assert exe.peak_memory_bytes > 0
        finally:
            task.cancel()
            await task
        assert not [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]

    asyncio.run(run())


def test_legacy_zarr_record_can_be_updated(tmp_path):
    import msgpack
    from hpcflow.sdk.submission.run_file_resolver import get_run_multi_chunk_path

    wk = make_workflow_to_run_command("echo ready", path=tmp_path)
    wk.add_submission()
    run = wk.get_all_EARs()[0]
    wk.set_EAR_start(run.id_, None, None)
    path = get_run_multi_chunk_path(
        idx=run.run_file_ID,
        prefix=wk._store._get_run_multi_dir_path(0),
    )
    records = msgpack.unpackb(path.read_bytes())
    records[run._run_file_idx] = records[run._run_file_idx][:9]
    path.write_bytes(msgpack.packb(records))
    wk.set_EAR_end((0, 0, 0), run, 0, peak_memory_bytes=123456)
    assert hf.Workflow(wk.path).get_all_EARs()[0].peak_memory_bytes == 123456


@pytest.mark.parametrize("task_setting", [None, False, True])
@pytest.mark.parametrize("scope", ["any", "main"])
def test_tracking_resource_inheritance(tmp_path, task_setting, scope):
    schema = hf.TaskSchema(
        objective="memory_resources",
        actions=[hf.Action(commands=[hf.Command("echo ready")])],
    )
    task_resources = (
        None if task_setting is None else {scope: {"track_peak_memory": task_setting}}
    )
    wk = hf.Workflow.from_template_data(
        template_name="memory_resources",
        path=tmp_path,
        resources={"any": {"track_peak_memory": True, "combine_scripts": True}},
        tasks=[hf.Task(schema=schema, resources=task_resources)],
    )
    wk.add_submission()
    resources = hf.Workflow(wk.path).get_all_EARs()[0].resources
    assert resources.track_peak_memory is (task_setting is not False)
    # combine_scripts does not apply to this command action.
    assert not resources.combine_scripts


def test_tracking_does_not_split_uncombined_jobscripts():
    assert (
        hf.ElementResources(track_peak_memory=True).get_jobscript_hash()
        == hf.ElementResources(track_peak_memory=False).get_jobscript_hash()
    )


def test_combined_run_resources_warn_and_preserve_combining(tmp_path):
    schema = hf.TaskSchema(
        objective="combined_memory",
        actions=[hf.Action(script="<<script:main_script_test_direct_in.py>>")],
    )
    wk = hf.Workflow.from_template_data(
        template_name="combined_memory",
        path=tmp_path,
        resources={"any": {"track_peak_memory": True, "combine_scripts": True}},
        tasks=[hf.Task(schema=schema)],
    )
    iteration = wk.tasks[0].elements[0].iterations[0]
    action = wk.tasks[0].template.schemas[0].actions[0]
    with pytest.warns(UserWarning, match="track_peak_memory will be ignored"):
        resources = iteration.get_resources_obj(action)
    assert resources.combine_scripts is True
    assert resources.track_peak_memory is False


@pytest.mark.parametrize("interval", [None, 0.05, 1.0])
def test_monitor_uses_sampling_interval(monkeypatch, interval):
    import hpcflow.sdk.core.execute as execution

    kwargs = {} if interval is None else {"peak_memory_sampling_interval": interval}
    exe = hf.Executor([], {}, hf.package_name, track_peak_memory=True, **kwargs)
    process = SimpleNamespace(pid=os.getpid(), returncode=None)
    delays = []

    async def sleep(delay):
        delays.append(delay)
        process.returncode = 0

    monkeypatch.setattr(execution.asyncio, "sleep", sleep)
    asyncio.run(exe._monitor_memory(process))
    assert delays == [0.5 if interval is None else interval]
    assert exe.peak_memory_bytes > 0


@pytest.mark.parametrize("interval", [0, -1, float("inf"), float("nan"), True, "0.5"])
@pytest.mark.parametrize("kind", ["spec", "resolved", "executor"])
def test_sampling_interval_must_be_positive_and_finite(interval, kind):
    with pytest.raises(ValueError, match="finite positive number of seconds"):
        if kind == "spec":
            hf.ResourceSpec(peak_memory_sampling_interval=interval)
        elif kind == "resolved":
            hf.ElementResources(peak_memory_sampling_interval=interval)
        else:
            hf.Executor([], {}, hf.package_name, peak_memory_sampling_interval=interval)


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("task_interval", [None, 0.05])
def test_sampling_interval_inherits_and_persists(tmp_path, store, task_interval):
    schema = hf.TaskSchema(
        objective="memory_interval",
        actions=[hf.Action(commands=[hf.Command("echo ready")])],
    )
    wk = hf.Workflow.from_template_data(
        template_name="memory_interval",
        path=tmp_path,
        store=store,
        resources={
            "any": {"track_peak_memory": True, "peak_memory_sampling_interval": 1.0}
        },
        tasks=[
            hf.Task(
                schema=schema,
                resources={"main": {"peak_memory_sampling_interval": task_interval}},
            )
        ],
    )
    wk.add_submission()
    resources = hf.Workflow(wk.path).get_all_EARs()[0].resources
    assert resources.peak_memory_sampling_interval == (
        1.0 if task_interval is None else task_interval
    )
    assert resources.track_peak_memory is True


def test_sampling_interval_defaults_and_jobscript_hash():
    assert hf.ElementResources().peak_memory_sampling_interval == 0.5
    assert hf.ResourceSpec().peak_memory_sampling_interval is None
    assert (
        hf.ElementResources(peak_memory_sampling_interval=0.05).get_jobscript_hash()
        == hf.ElementResources().get_jobscript_hash()
    )
