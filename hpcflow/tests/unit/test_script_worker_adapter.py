import asyncio
import contextvars
import json
import os
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import numpy as np
import psutil
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core import ABORT_EXIT_CODE, NO_COMMANDS_EXIT_CODE
from hpcflow.sdk.core.enums import EARStatus
from hpcflow.sdk.core.errors import UnsetParameterDataError
from hpcflow.sdk.execution.client import JobscriptClient
from hpcflow.sdk.execution.protocol import StatusRequest
from hpcflow.sdk.execution.run_executor import RunExecutor
from hpcflow.sdk.execution.script_worker import adapter as adapter_module
from hpcflow.sdk.execution.script_worker.adapter import ScriptWorkerInvocation
from hpcflow.sdk.execution.script_worker.client import ScriptWorkerClient
from hpcflow.sdk.execution.script_worker.protocol import (
    ExecuteScriptRequest,
    ScriptExecutionResult,
)
from hpcflow.sdk.execution.script_worker.run import ScriptWorkerRun
from hpcflow.sdk.execution.script_worker.service import (
    ScriptWorkerService,
    ScriptWorkerServiceError,
)
from hpcflow.sdk.instrumentation import TimeIt
from hpcflow.sdk.scripting import ScriptWriter
from hpcflow.sdk.scripting.writer import ScriptWorkerArgs
from hpcflow.sdk.submission.shells import get_shell
from hpcflow.sdk.wait.run_wait import RunWaitEvent


def make_run(
    tmp_path: Path,
    *,
    data_in="direct",
    data_out="direct",
    store="json",
    repeats=1,
    enable_script_workers=False,
    script="<<script:main_script_test_direct_in_direct_out.py>>",
    abortable=False,
    resources=None,
    add_submission=True,
):
    schema = hf.TaskSchema(
        objective="worker_adapter",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                script=script,
                script_exe="python_script",
                script_data_in=data_in,
                script_data_out=data_out,
                environments=[hf.ActionEnvironment(environment="python_env")],
                requires_dir=True,
                abortable=abortable,
            )
        ],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="worker_adapter",
        path=tmp_path,
        store=store,
        tasks=[hf.Task(schema, inputs={"p1": 101}, repeats=repeats)],
        resources=resources,
    )
    if add_submission:
        workflow.add_submission(enable_script_workers=enable_script_workers)
    run = workflow.get_all_EARs()[0]
    if add_submission:
        run.prepare_directory()
    return workflow, run


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("enable_script_workers", [False, True])
def test_retired_combined_scripts_rejected_before_submission(
    tmp_path, store, enable_script_workers
):
    workflow, run = make_run(
        tmp_path,
        store=store,
        resources={"any": {"combine_scripts": True}},
        add_submission=False,
    )
    artifacts_before = set(workflow.submissions_path.rglob("*"))
    assignment_before = (run.submission_idx, run.jobscript_idx)
    with pytest.raises(ValueError, match="combine_scripts=True is no longer supported"):
        workflow.add_submission(enable_script_workers=enable_script_workers)
    assert workflow.num_submissions == 0
    assert set(workflow.submissions_path.rglob("*")) == artifacts_before
    assert (run.submission_idx, run.jobscript_idx) == assignment_before
    fresh = hf.Workflow(workflow.path)
    assert fresh.num_submissions == 0
    fresh_run = fresh.get_all_EARs()[0]
    assert (fresh_run.submission_idx, fresh_run.jobscript_idx) == assignment_before


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_old_combined_submission_readable_but_cannot_execute(tmp_path, store):
    workflow, _ = make_run(tmp_path, store=store)
    data = workflow.submissions[0].to_json_like()[0]
    data["jobscripts"][0]["resources"]["combine_scripts"] = True
    old_submission = hf.Submission.from_json_like({"index": 0, **data})
    old_submission.workflow = workflow
    workflow._submissions = [old_submission]
    assert old_submission.jobscripts[0].resources.combine_scripts is True
    with pytest.raises(ValueError, match="combine_scripts=True is no longer supported"):
        old_submission.prepare_submit(ignore_errors=True)
    with pytest.raises(ValueError, match="combine_scripts=True is no longer supported"):
        old_submission.jobscripts[0].write_jobscript()
    with pytest.raises(ValueError, match="combine_scripts=True is no longer supported"):
        hf.JobscriptExecutor(workflow, 0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("enable_script_workers", "fail"),
    [(False, False), (True, False), (False, True), (True, True)],
)
async def test_jobscript_executor_runs_snippets_on_legacy_or_worker_path(
    tmp_path, monkeypatch, capfd, enable_script_workers, fail
):
    if getattr(sys, "frozen", False):
        pytest.skip("Worker subprocess tests require Python.")
    workflow, run = make_run(
        tmp_path,
        repeats=2,
        enable_script_workers=enable_script_workers,
        script=(
            "<<script:main_script_test_script_worker_failure.py>>"
            if fail
            else "<<script:main_script_test_direct_in_direct_out.py>>"
        ),
    )
    executor = hf.JobscriptExecutor(
        workflow,
        0,
        0,
        enable_script_workers=enable_script_workers,
    )
    worker_args = run.get_script_worker_args(0)
    worker_script = worker_args.get_script_path(workflow.submissions[0].scripts_path)
    worker_commands = worker_args.get_commands_path(workflow.submissions[0].commands_path)
    shell = executor.jobscript.shell
    if enable_script_workers:
        assert worker_script.is_file()
        assert worker_commands.is_file()
    else:
        assert not worker_script.exists()
        assert not worker_commands.exists()
    executor.submission.prepare_submit()
    monkeypatch.setenv(
        f"{executor.app_caps}_JS_FUNCS_PATH",
        str(executor.jobscript.jobscript_functions_path),
    )
    monkeypatch.setenv(f"{executor.app_caps}_WK_PATH_ARG", str(workflow.path))
    jobscript_text = executor.jobscript.compose_jobscript(
        shell=shell,
        os_name=executor.jobscript.os_name,
    )
    assert ("--enable-script-workers" in jobscript_text) is enable_script_workers

    await executor._execute()

    finalized_runs = workflow.get_all_EARs()
    assert all(run.end_time is not None for run in finalized_runs)
    assert all(run.success is not fail for run in finalized_runs)
    if fail and enable_script_workers:
        assert all(
            "script worker action failed" in run.get_app_std_path().read_text()
            for run in finalized_runs
        )
    elif fail:
        captured = capfd.readouterr()
        assert "script worker action failed" in captured.out + captured.err
    else:
        assert [element.outputs.p2.value for element in workflow.tasks[0].elements] == [
            201,
            201,
        ]
    if enable_script_workers:
        assert executor.script_workers == {}
    else:
        assert not executor.script_workers


@pytest.mark.asyncio
async def test_worker_preparation_failure_finalizes_and_reuses_worker(
    tmp_path, monkeypatch
):
    if getattr(sys, "frozen", False):
        pytest.skip("Worker subprocess tests require Python.")
    monkeypatch.chdir(tmp_path)
    workflow, _ = make_run(
        tmp_path,
        repeats=3,
        enable_script_workers=True,
        script="<<script:main_script_test_script_worker_process.py>>",
    )
    runs = workflow.get_all_EARs()
    executor = hf.JobscriptExecutor(workflow, 0, 0, enable_script_workers=True)
    executor.submission.prepare_submit()
    monkeypatch.setenv(
        "HPCFLOW_JS_FUNCS_PATH", str(executor.jobscript.jobscript_functions_path)
    )
    monkeypatch.setenv("HPCFLOW_WK_PATH_ARG", str(workflow.path))
    prepare = executor._prepare_run_files

    def fail_middle_preparation(action, run, run_dir):
        if run.id_ == runs[1].id_:
            raise UnsetParameterDataError("inputs.p1", "inputs.p1")
        return prepare(action, run, run_dir)

    monkeypatch.setattr(executor, "_prepare_run_files", fail_middle_preparation)
    finalize = Mock(wraps=executor._finalize_run)
    monkeypatch.setattr(executor, "_finalize_run", finalize)
    monkeypatch.setattr(workflow, "WAIT_STATE_CHECK_INTERVAL", 60)
    waiter = asyncio.create_task(workflow._wait_for_runs([runs[1].id_], quiet=True))
    task = None
    try:

        async def registered():
            while not executor.run_wait_state.get_waiter_ids(
                runs[1].id_, RunWaitEvent.END
            ):
                if waiter.done():
                    await waiter
                    pytest.fail("Run waiter ended before registration.")
                await asyncio.sleep(0.01)

        await asyncio.wait_for(registered(), timeout=5)
        task = asyncio.create_task(executor._execute())
        await asyncio.wait_for(task, timeout=45)
        await asyncio.wait_for(waiter, timeout=5)

        fresh = hf.Workflow(workflow.path)
        first, failed, last = fresh.get_all_EARs()
        assert [run.status for run in (first, failed, last)] == [
            EARStatus.success,
            EARStatus.error,
            EARStatus.success,
        ]
        assert [run.exit_code for run in (first, failed, last)] == [
            0,
            NO_COMMANDS_EXIT_CODE,
            0,
        ]
        assert all(run.start_time and run.end_time for run in (first, failed, last))
        assert failed.get("outputs.p2") is None
        assert "Could not prepare run" in failed.get_app_std_path().read_text()
        assert [call.kwargs["run"].id_ for call in finalize.call_args_list] == [
            run.id_ for run in runs
        ]
        worker_pid = fresh.tasks[0].elements[0].outputs.p2.value
        assert isinstance(worker_pid, int)
        assert worker_pid > 0
        assert worker_pid != os.getpid()
        assert worker_pid == fresh.tasks[0].elements[2].outputs.p2.value
        assert executor.completion.is_complete()
        assert not executor._get_wait_endpoints()
        assert executor.script_workers == {}
    finally:
        if task is not None:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        if not waiter.done():
            waiter.cancel()
        await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("enable_script_workers", [False, True])
async def test_unexpected_preparation_error_exits_nonzero(
    tmp_path, monkeypatch, enable_script_workers
):
    monkeypatch.chdir(tmp_path)
    workflow, run = make_run(tmp_path, enable_script_workers=enable_script_workers)
    executor = hf.JobscriptExecutor(
        workflow, 0, 0, enable_script_workers=enable_script_workers
    )
    executor.submission.prepare_submit()
    monkeypatch.setattr(
        executor, "_prepare_run_files", Mock(side_effect=OSError("unexpected failure"))
    )

    with pytest.raises(SystemExit) as exc_info:
        await executor._execute()

    assert exc_info.value.code == 1
    assert "OSError: unexpected failure" in run.get_app_std_path().read_text()
    assert executor.completion.is_complete()


def generated_functions(action):
    args = ScriptWorkerArgs(0, hf.Environment("python"), "python", "python")
    writer = ScriptWriter.create(args, {(0, 0): action})
    namespace = {"__name__": "worker_test"}
    exec(compile(writer.compose_worker(), "<worker>", "exec"), namespace)
    return namespace["ACTION_SCRIPTS"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel_jobscript", [False, True])
async def test_active_worker_abort_or_cancellation_reaps_process(
    tmp_path, monkeypatch, cancel_jobscript
):
    if getattr(sys, "frozen", False):
        pytest.skip("Worker subprocess tests require Python.")
    marker = tmp_path / "worker_started.json"
    monkeypatch.setenv("WORKER_CONTROL_PATH", str(marker))
    workflow, _ = make_run(
        tmp_path,
        repeats=2,
        enable_script_workers=True,
        script="<<script:main_script_test_script_worker_abort.py>>",
        abortable=True,
    )
    executor = hf.JobscriptExecutor(workflow, 0, 0, enable_script_workers=True)
    executor.submission.prepare_submit()
    monkeypatch.setenv(
        "HPCFLOW_JS_FUNCS_PATH", str(executor.jobscript.jobscript_functions_path)
    )
    monkeypatch.setenv("HPCFLOW_WK_PATH_ARG", str(workflow.path))
    task = asyncio.create_task(executor._execute())
    waiter = None
    finalize = Mock(wraps=executor._finalize_run)
    monkeypatch.setattr(executor, "_finalize_run", finalize)
    try:

        async def started():
            while not marker.exists():
                if task.done():
                    await task
                    pytest.fail("Jobscript ended before its snippet started.")
                await asyncio.sleep(0.02)

        await asyncio.wait_for(started(), timeout=30)
        pid = json.loads(marker.read_text())["pid"]
        client = next(iter(executor.script_workers.values()))
        process = client.worker.process
        assert process is not None
        server = executor.jobscript_server
        assert server is not None
        assert server._run_task is not None
        if server._run_task.done():
            server._run_task.result()
        status = server._handle_request(StatusRequest()).to_dict()
        assert status["runs"][0]["run_id"] == 0
        assert status["runs"][0]["running"] is True
        if cancel_jobscript:
            task.cancel()
            await asyncio.sleep(0)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, timeout=15)
        else:
            executor.jobscript._set_process_ID(os.getpid())
            monkeypatch.setattr(workflow, "WAIT_STATE_CHECK_INTERVAL", 60)
            waiter = asyncio.create_task(workflow._wait(sub_js={0: [0]}, quiet=True))

            async def registered():
                while not executor._get_wait_endpoints():
                    if waiter.done():
                        await waiter
                        pytest.fail("Workflow waiter ended before registration.")
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(registered(), timeout=5)
            assert server.port_number is not None
            response = await asyncio.to_thread(
                JobscriptClient.send_abort, "127.0.0.1", server.port_number, 0
            )
            assert response == {"ok": True}
            await asyncio.wait_for(task, timeout=30)
            fresh = hf.Workflow(workflow.path)
            first, second = fresh.get_all_EARs()
            assert first.status is EARStatus.aborted
            assert first.exit_code == ABORT_EXIT_CODE
            assert first.end_time is not None
            assert second.success and second.end_time is not None
            assert fresh.tasks[0].elements[1].outputs.p2.value != pid
            await asyncio.wait_for(waiter, timeout=5)
            assert [call.kwargs["run"].id_ for call in finalize.call_args_list] == [0, 1]
            assert not executor._get_wait_endpoints()
        assert not psutil.pid_exists(pid)
        assert process.returncode is not None
        assert client.worker.process is None
        assert client.endpoint is None
        assert executor.script_workers == {}
        assert executor.active_executors == {}
        assert executor.jobscript_server is None
        assert executor.completion.is_complete()
    finally:
        if not task.done():
            task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        if waiter is not None:
            if not waiter.done():
                waiter.cancel()
            await asyncio.gather(waiter, return_exceptions=True)


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("enable_script_workers", [False, True])
def test_submission_reload_preserves_worker_opt_in(
    tmp_path, store, enable_script_workers
):
    workflow, run = make_run(
        tmp_path, store=store, enable_script_workers=enable_script_workers
    )
    args = run.get_script_worker_args(0)
    fresh = hf.Workflow(workflow.path)
    submission = fresh.submissions[0]
    assert submission.enable_script_workers is enable_script_workers
    assert fresh.get_all_EARs()[0].get_script_worker_args(0).id == args.id
    for path in (
        args.get_script_path(submission.scripts_path),
        args.get_manifest_path(submission.scripts_path),
        args.get_commands_path(submission.commands_path),
    ):
        assert path.is_file() is enable_script_workers
    submission.prepare_submit()
    jobscript = submission.jobscripts[0]
    path = jobscript.write_jobscript()
    assert ("--enable-script-workers" in path.read_text()) is enable_script_workers
    original = path.read_bytes()
    assert original == jobscript.write_jobscript().read_bytes()


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_old_submission_without_worker_option_defaults_to_disabled(tmp_path, store):
    workflow, _ = make_run(tmp_path, store=store)
    data = dict(workflow._store.get_submissions()[0])
    assert data.pop("enable_script_workers") is False
    submission = hf.Submission.from_json_like({"index": 0, **data})
    submission.workflow = workflow
    assert submission.enable_script_workers is False
    submission.prepare_submit()
    assert "--enable-script-workers" not in (
        submission.jobscripts[0].write_jobscript().read_text()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("store", ["json", "zarr"])
async def test_reloaded_submission_executes_generated_jobscript(tmp_path, store):
    if getattr(sys, "frozen", False):
        pytest.skip("Worker subprocess tests require Python.")
    workflow, _ = make_run(
        tmp_path,
        store=store,
        repeats=2,
        enable_script_workers=True,
        script="<<script:main_script_test_script_worker_process.py>>",
    )
    fresh = hf.Workflow(workflow.path)
    submission = fresh.submissions[0]
    assert submission.enable_script_workers is True
    submission.prepare_submit()
    jobscript = submission.jobscripts[0]
    path = jobscript.write_jobscript()
    process = await asyncio.create_subprocess_exec(
        *jobscript.shell.get_command_file_launch_command(str(path)),
        cwd=fresh.path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=90)
        assert process.returncode == 0, (stdout.decode(), stderr.decode())
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    completed = hf.Workflow(workflow.path)
    assert all(run.success and run.end_time for run in completed.get_all_EARs())
    worker_pids = [element.outputs.p2.value for element in completed.tasks[0].elements]
    assert worker_pids[0] == worker_pids[1]
    assert worker_pids[0] > 0
    assert worker_pids[0] not in (os.getpid(), process.pid)
    assert jobscript.completion_obj.is_complete()


@pytest.mark.asyncio
@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("different_interpreters", [False, True])
async def test_reloaded_jobscript_isolates_and_reuses_environment_workers(
    tmp_path, store, different_interpreters, reload_template_components, monkeypatch
):
    if getattr(sys, "frozen", False):
        pytest.skip("Worker subprocess tests require Python.")
    second_python = getattr(sys, "_base_executable", sys.executable)
    if different_interpreters:
        if Path(second_python).samefile(sys.executable):
            pytest.skip("No distinct base Python interpreter is available.")
        probe = subprocess.run(
            [
                second_python,
                "-c",
                "from hpcflow.sdk.execution.script_worker.bootstrap "
                "import run_generated_script_worker",
            ],
            capture_output=True,
            text=True,
            timeout=15,
        )
        if probe.returncode:
            pytest.skip(
                "The distinct base interpreter cannot import the worker runtime: "
                + probe.stderr
            )
    monkeypatch.setenv("WORKER_GROUP", "parent")
    monkeypatch.delenv("WORKER_LEAK", raising=False)
    for group in ("one", "two"):
        environment = hf.envs.python_env.copy(
            name="worker_python", specifiers={"worker_group": group}
        )
        setup = (
            f"$env:WORKER_GROUP = '{group}'"
            if os.name == "nt"
            else f"export WORKER_GROUP='{group}'"
        )
        environment.setup = (*(environment.setup or ()), setup)
        if different_interpreters and group == "two":
            shell = get_shell(
                "powershell" if os.name == "nt" else "bash",
                os_name="nt" if os.name == "nt" else "posix",
            )
            prefix = "& " if os.name == "nt" else ""
            executable = environment.executables.get("python_script")
            for instance in executable.instances:
                instance.command = (
                    f"{prefix}{shell.quote_arg(second_python)} "
                    f"{shell.quote_arg('<<script_path>>')} <<args>>"
                )
        hf.envs.add_object(environment)
    schema = hf.TaskSchema(
        objective="worker_environments",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                script="<<script:main_script_test_script_worker_environment.py>>",
                script_exe="python_script",
                script_data_in="direct",
                script_data_out="direct",
                script_pass_env_spec=True,
                environments=[hf.ActionEnvironment(environment="worker_python")],
                requires_dir=True,
            )
        ],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="worker_environments",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schema,
                inputs={"p1": 101},
                environments={"worker_python": {"worker_group": "one"}},
                sequences=[
                    hf.ValueSequence(
                        path="environments.worker_python.worker_group",
                        values=["one", "two", "one", "two"],
                    )
                ],
            )
        ],
    )
    workflow.add_submission(enable_script_workers=True)
    fresh = hf.Workflow(workflow.path)
    submission = fresh.submissions[0]
    assert len(submission.jobscripts) == 1
    runs = fresh.get_all_EARs()
    identities = [run.script_worker_id for run in runs]
    assert identities[0] == identities[2]
    assert identities[1] == identities[3]
    assert identities[0] != identities[1]
    for run in runs:
        args = run.get_script_worker_args(0)
        assert args.get_script_path(submission.scripts_path).is_file()
        assert args.get_manifest_path(submission.scripts_path).is_file()
        assert args.get_commands_path(submission.commands_path).is_file()
    submission.prepare_submit()
    jobscript = submission.jobscripts[0]
    process = await asyncio.create_subprocess_exec(
        *jobscript.shell.get_command_file_launch_command(
            str(jobscript.write_jobscript())
        ),
        cwd=fresh.path,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=90)
        assert process.returncode == 0, (stdout.decode(), stderr.decode())
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    completed = hf.Workflow(workflow.path)
    finalized = completed.get_all_EARs()
    assert all(run.success and run.end_time for run in finalized), (
        stdout.decode(),
        stderr.decode(),
    )
    results = [element.outputs.p2.value for element in completed.tasks[0].elements]
    assert results[0]["pid"] == results[2]["pid"]
    assert results[1]["pid"] == results[3]["pid"]
    assert results[0]["pid"] != results[1]["pid"]
    for index, (run, result) in enumerate(zip(finalized, results)):
        group = "one" if index % 2 == 0 else "two"
        assert result["group"] == result["setup_group"] == group
        assert result["run_id"] == str(run.id_)
        assert result["element_idx"] == str(index)
        assert result["leaked"] is None
        expected_python = (
            second_python if different_interpreters and group == "two" else sys.executable
        )
        assert Path(result["executable"]).samefile(expected_python)
        assert not psutil.pid_exists(result["pid"])
    assert jobscript.completion_obj.is_complete()


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_generated_adapter_persists_direct_outputs_without_finalizing(tmp_path, store):
    workflow, run = make_run(tmp_path, store=store)
    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        generated_functions(run.action)[(0, 0)],
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    result = invocation()
    assert isinstance(result, ScriptExecutionResult)
    assert result.exit_code == 0
    assert result.work_time >= 0 and result.orchestration_time >= 0
    assert workflow.tasks[0].elements[0].outputs.p2.value == 201
    assert workflow.get_all_EARs()[0].end_time is None
    assert not run.use_script_worker


def test_generated_registry_preserves_same_action_index_in_different_blocks(tmp_path):
    _, run = make_run(tmp_path)
    args = ScriptWorkerArgs(0, hf.Environment("python"), "python", "python")
    writer = ScriptWriter.create(args, {(0, 0): run.action, (1, 0): run.action})
    namespace = {"__name__": "worker_test"}
    exec(compile(writer.compose_worker(), "<worker>", "exec"), namespace)
    functions = namespace["ACTION_SCRIPTS"]
    assert set(functions) == {(0, 0), (1, 0)}
    assert functions[(0, 0)](p1=1) == {"p2": 101}
    assert functions[(1, 0)](p1=2) == {"p2": 102}
    assert functions[(0, 0)] is not functions[(1, 0)]


@pytest.mark.parametrize("fail", [False, True])
def test_context_restored_before_service_reply(tmp_path, fail):
    workflow, run = make_run(tmp_path)
    baseline_cwd = Path.cwd()
    baseline_env = dict(os.environ)
    baseline_streams = (sys.stdout, sys.stderr)
    baseline_handler = hf.log.file_handler
    variable = contextvars.ContextVar("worker_test", default="outside")
    work_dir = run.get_directory()
    assert work_dir is not None
    other_dir = tmp_path / "other"
    other_dir.mkdir()
    env = {**os.environ, "WORKER_TEST": "inside"}
    observed = []

    def function(p1):
        observed.append((Path.cwd(), os.environ["WORKER_TEST"], TimeIt.current()))
        variable.set("changed")
        os.environ["LEAKED_WORKER_VALUE"] = "bad"
        os.environ.pop("WORKER_TEST")
        os.chdir(other_dir)
        print("worker stdout")
        print("worker stderr", file=sys.stderr)
        if fail:
            raise ValueError("script failed")
        return {"p2": p1 + 1}

    request = ExecuteScriptRequest(run.id_, 0, 0, 0)
    invocation = ScriptWorkerInvocation(
        request,
        run,
        function,
        environment=env,
        std_path=tmp_path / "run.std",
        log_path=tmp_path / "run.log",
    )
    with TimeIt().activate() as outer:
        response = ScriptWorkerService("unused", {request: invocation})._handle(request)
        assert TimeIt.current() is outer
        assert observed[0][2] is not outer
    assert isinstance(response, ScriptExecutionResult)
    assert response.exit_code == int(fail)
    assert observed[0][:2] == (work_dir, "inside")
    assert variable.get() == "outside"
    assert Path.cwd() == baseline_cwd
    assert dict(os.environ) == baseline_env
    assert (sys.stdout, sys.stderr) == baseline_streams
    assert hf.log.file_handler is baseline_handler
    text = (tmp_path / "run.std").read_text()
    assert "worker stdout" in text and "worker stderr" in text
    if fail:
        assert "ValueError: script failed" in text
    # No worker cwd or log handles may survive a result.
    work_dir.rmdir()
    (tmp_path / "run.log").unlink()


def test_adapter_uses_script_file_arguments_and_existing_json_input_writer(tmp_path):
    _, run = make_run(tmp_path, data_in="json", data_out="json")
    seen = {}

    def function(_input_files, _output_files):
        import json

        seen.update(json.loads(_input_files["json"].read_text()))
        _output_files["json"].write_text('{"p2": 301}')

    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        function,
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    # File preparation stays with the orchestrator.
    previous = Path.cwd()
    try:
        os.chdir(run.get_directory())
        run.write_script_data_in_files((0, 0, 0))
    finally:
        os.chdir(previous)
    assert invocation().exit_code == 0
    assert seen == {"p1": 101}
    path = run.action.get_input_output_file_paths(
        "script", (0, 0, 0), directory=run.get_directory()
    )["outputs"]["json"]
    assert path.read_text() == '{"p2": 301}'


def test_adapter_validates_run_identity_before_invocation(tmp_path):
    _, run = make_run(tmp_path)
    function = Mock()
    with pytest.raises(ValueError, match="run"):
        ScriptWorkerInvocation(
            ExecuteScriptRequest(run.id_ + 1, 0, 0, 0),
            run,
            function,
            environment=dict(os.environ),
            std_path=tmp_path / "run.std",
        )
    function.assert_not_called()


@pytest.mark.parametrize("data_in", ["direct", "json"])
def test_script_environment_argument_remains_a_plain_dictionary(tmp_path, data_in):
    _, run = make_run(tmp_path, data_in=data_in)
    run.action.script_pass_env_spec = True
    values = run.get_data_in_values()
    assert type(values["env_spec"]) is dict
    assert values["env_spec"] == dict(run.env_spec)
    if data_in == "direct":
        kwargs = run.get_py_script_func_kwargs()
        assert type(kwargs["env_spec"]) is dict


def test_output_persistence_error_is_nonzero_and_diagnostic(tmp_path, monkeypatch):
    workflow, run = make_run(tmp_path)
    monkeypatch.setattr(
        workflow, "save_parameter", Mock(side_effect=OSError("save failed"))
    )
    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        lambda p1: {"p2": p1},
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    assert invocation().exit_code == 1
    assert "save failed" in (tmp_path / "run.std").read_text()


@pytest.mark.parametrize(
    "code",
    [
        None,
        0,
        1,
        7,
        64,
        98,
        -1,
        -123124,
        256,
        -256,
        2**31,
        2**32,
        2**63,
        -(2**63) - 1,
        False,
        True,
        "exit message",
        1.5,
    ],
)
def test_system_exit_returns_process_status_and_restores_context(
    tmp_path, monkeypatch, code
):
    if getattr(sys, "frozen", False):
        pytest.skip("Comparing interpreter exit codes requires Python.")
    process = subprocess.run(
        [sys.executable, "-c", f"import sys; sys.exit({code!r})"],
        capture_output=True,
        text=True,
        timeout=10,
    )
    expected_exit_code = RunExecutor._normalise_return_code(process.returncode)
    workflow, run = make_run(tmp_path)
    save_parameter = Mock()
    monkeypatch.setattr(workflow, "save_parameter", save_parameter)
    cwd = Path.cwd()
    env = dict(os.environ)
    streams = (sys.stdout, sys.stderr)
    handler = hf.log.file_handler
    variable = contextvars.ContextVar("worker_exit", default="outside")

    def function(p1):
        variable.set("changed")
        os.environ["WORKER_BAD"] = "bad"
        os.chdir(tmp_path)
        raise SystemExit(code)

    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        function,
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
        log_path=tmp_path / "run.log",
    )
    with TimeIt().activate() as outer:
        result = invocation()
        assert TimeIt.current() is outer
    assert result.exit_code == expected_exit_code
    assert result.work_time >= 0 and result.orchestration_time >= 0
    save_parameter.assert_not_called()
    if process.stderr:
        assert (tmp_path / "run.std").read_text().endswith(process.stderr)
    else:
        assert not (tmp_path / "run.std").exists()
    assert Path.cwd() == cwd and dict(os.environ) == env
    assert (sys.stdout, sys.stderr) == streams
    assert hf.log.file_handler is handler
    assert variable.get() == "outside"


@pytest.mark.parametrize("error_type", [KeyboardInterrupt, GeneratorExit])
def test_other_base_exceptions_propagate_after_context_restoration(tmp_path, error_type):
    _, run = make_run(tmp_path)
    cwd = Path.cwd()
    env = dict(os.environ)

    def function(p1):
        os.environ["WORKER_BAD"] = "bad"
        raise error_type()

    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        function,
        environment=env,
        std_path=tmp_path / "run.std",
    )
    with pytest.raises(error_type):
        invocation()
    assert Path.cwd() == cwd and dict(os.environ) == env


def test_system_exit_during_argument_setup_is_not_a_script_result(tmp_path, monkeypatch):
    _, run = make_run(tmp_path)
    monkeypatch.setattr(run, "get_py_script_func_kwargs", Mock(side_effect=SystemExit(7)))
    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        Mock(),
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    with pytest.raises(SystemExit) as error:
        invocation()
    assert error.value.code == 7


def test_system_exit_during_output_persistence_is_not_a_script_result(
    tmp_path, monkeypatch
):
    workflow, run = make_run(tmp_path)
    monkeypatch.setattr(workflow, "save_parameter", Mock(side_effect=SystemExit(7)))
    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        lambda p1: {"p2": p1},
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    with pytest.raises(SystemExit) as error:
        invocation()
    assert error.value.code == 7


def test_system_exit_during_timing_is_not_a_script_result(tmp_path, monkeypatch):
    _, run = make_run(tmp_path)
    clock = Mock()
    clock.perf_counter.side_effect = [0.0, 1.0, SystemExit(7)]
    monkeypatch.setattr(adapter_module, "time", clock)
    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        lambda p1: {"p2": p1},
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    with pytest.raises(SystemExit) as error:
        invocation()
    assert error.value.code == 7


@pytest.mark.parametrize("system_exit", [False, True])
def test_failed_context_restoration_is_fatal_to_service(
    tmp_path, monkeypatch, system_exit
):
    _, run = make_run(tmp_path)
    cwd = Path.cwd()
    env = dict(os.environ)
    chdir = os.chdir

    def fail_restore(path):
        if path == cwd:
            raise OSError("restore failed")
        chdir(path)

    monkeypatch.setattr(os, "chdir", fail_restore)
    request = ExecuteScriptRequest(run.id_, 0, 0, 0)

    def function(p1):
        if system_exit:
            raise SystemExit(0)
        return {"p2": p1}

    invocation = ScriptWorkerInvocation(
        request,
        run,
        function,
        environment={**env, "TEMP_WORKER": "yes"},
        std_path=tmp_path / "run.std",
    )
    try:
        with pytest.raises(ScriptWorkerServiceError, match="restore context"):
            ScriptWorkerService("unused", {request: invocation})._handle(request)
        assert dict(os.environ) == env
    finally:
        chdir(cwd)


def test_service_rejects_mismatched_adapter_result_identity():
    request = ExecuteScriptRequest(1, 0, 0, 0)
    result = ScriptExecutionResult(2, 0, 0, 0, 0, 0.0, 0.0)
    with pytest.raises(ScriptWorkerServiceError, match="mismatched"):
        ScriptWorkerService("unused", {request: lambda: result})._handle(request)


def test_generated_wrapper_rebuilds_function_defaults_each_invocation(
    tmp_path, monkeypatch
):
    _, run = make_run(tmp_path)
    snippet = tmp_path / "counter.py"
    snippet.write_text(
        "def counter(values=[]):\n    values.append(1)\n    return len(values)\n",
        encoding="utf-8",
        newline="\n",
    )
    monkeypatch.setattr(
        run.action,
        "get_script_artifact_name",
        lambda **kwargs: ("counter_wrapper", snippet),
    )
    function = generated_functions(run.action)[(0, 0)]
    assert function() == 1
    assert function() == 1


@pytest.mark.parametrize("disabled", [False, True])
def test_adapter_keeps_run_specific_array_output_policy(tmp_path, disabled):
    workflow, run = make_run(tmp_path, store="zarr")
    run.task.template.disable_array_outputs = ["p2"] if disabled else []
    output = np.arange(6).reshape((2, 3))
    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        lambda p1: {"p2": output},
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    assert invocation().exit_code == 0
    np.testing.assert_array_equal(workflow.tasks[0].elements[0].outputs.p2.value, output)
    assert bool(workflow.list_array_outputs()) is not disabled


@pytest.mark.parametrize("system_exit", [False, True])
def test_output_file_parser_scalar_uses_declared_parameter(tmp_path, system_exit):
    file = hf.FileSpec(label="result", name="result.txt")
    schema = hf.TaskSchema(
        objective="worker_parser",
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                output_file_parsers=[
                    hf.OutputFileParser(
                        output_files=[file],
                        output=hf.Parameter("p2"),
                        script="<<script:output_file_parser_basic.py>>",
                    )
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="worker_parser",
        path=tmp_path,
        tasks=[hf.Task(schema=schema)],
    )
    workflow.add_submission()
    run = next(run for run in workflow.get_all_EARs() if run.action.is_OFP)
    run.prepare_directory()
    block = workflow.submissions[0].jobscripts[0].blocks[0]
    action_idx, element_idx = np.argwhere(block.EAR_ID == run.id_)[0]

    def function(**kwargs):
        if system_exit:
            raise SystemExit(0)
        return 321

    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, int(action_idx), int(element_idx)),
        run,
        function,
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    assert invocation().exit_code == 0
    if system_exit:
        assert run.get("outputs.p2") is None
    else:
        assert workflow.tasks[0].elements[0].outputs.p2.value == 321


@pytest.mark.parametrize("index", ["block_idx", "action_idx", "element_idx"])
def test_adapter_validates_full_identity_against_jobscript(tmp_path, index):
    _, run = make_run(tmp_path)
    identity = {"run_id": run.id_, "block_idx": 0, "action_idx": 0, "element_idx": 0}
    identity[index] = 99
    with pytest.raises(ValueError, match="jobscript"):
        ScriptWorkerInvocation(
            ExecuteScriptRequest(**identity),
            run,
            Mock(),
            environment=dict(os.environ),
            std_path=tmp_path / "run.std",
        )


def test_adapter_rejects_another_elements_in_bounds_identity(tmp_path):
    _, run = make_run(tmp_path, repeats=2)
    with pytest.raises(ValueError, match="does not match"):
        ScriptWorkerInvocation(
            ExecuteScriptRequest(run.id_, 0, 0, 1),
            run,
            Mock(),
            environment=dict(os.environ),
            std_path=tmp_path / "run.std",
        )


@pytest.mark.parametrize("produces_file", [False, True])
@pytest.mark.parametrize("system_exit", [False, True])
def test_input_file_generator_leaves_promised_file_validation_to_parent(
    tmp_path, monkeypatch, produces_file, system_exit
):
    schema = hf.TaskSchema(
        objective="worker_ifg",
        inputs=[hf.SchemaInput("p1")],
        actions=[
            hf.Action(
                commands=[hf.Command("echo main")],
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=hf.FileSpec(label="input", name="input.txt"),
                        inputs=[hf.Parameter("p1")],
                        script="<<script:input_file_generator_basic.py>>",
                    )
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="worker_ifg",
        path=tmp_path,
        tasks=[hf.Task(schema=schema, inputs={"p1": 123})],
    )
    workflow.add_submission()
    run = next(run for run in workflow.get_all_EARs() if run.action.is_IFG)
    run.action.script_pass_env_spec = True
    directory = run.prepare_directory()
    assert directory is not None

    def function(path, p1, env_spec):
        assert type(env_spec) is dict
        assert env_spec == dict(run.env_spec)
        if produces_file:
            path.write_text(str(p1))
        if system_exit:
            raise SystemExit(0)

    invocation = ScriptWorkerInvocation(
        ExecuteScriptRequest(run.id_, 0, 0, 0),
        run,
        function,
        environment=dict(os.environ),
        std_path=tmp_path / "run.std",
    )
    result = invocation()
    assert result.exit_code == 0
    assert workflow.get_EARs_from_IDs([run.id_])[0].end_time is None
    monkeypatch.chdir(directory)
    workflow.set_EAR_start(run.id_, directory, None)
    workflow.set_EAR_end((0, 0, 0), run, result.exit_code)
    assert workflow.get_EARs_from_IDs([run.id_])[0].success is produces_file


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("exit_codes", "system_exit", "exit_message"),
    [
        ((0, 0), False, None),
        ((1, 1), False, None),
        ((1, 0), False, None),
        ((0, 0), True, None),
        ((7, 0), True, None),
        ((ABORT_EXIT_CODE, 0), True, None),
        ((1, 0), True, "worker exit message"),
    ],
)
async def test_generated_adapter_over_real_transport_persists_and_reuses_worker(
    tmp_path, monkeypatch, exit_codes, system_exit, exit_message
):
    if getattr(sys, "frozen", False):
        pytest.skip("This worker subprocess test requires Python.")
    workflow, run = make_run(tmp_path, repeats=2)
    if any(exit_codes) or system_exit:
        snippet = tmp_path / "worker_action.py"
        snippet.write_text(
            "def worker_action(p1):\n"
            "    import os\n"
            "    if os.environ['WORKER_EXIT_NOW'] == '1':\n"
            "        message = os.environ['WORKER_EXIT_MESSAGE']\n"
            "        raise SystemExit(\n"
            "            message if message else int(os.environ['WORKER_FAIL'])\n"
            "        )\n"
            "    if os.environ['WORKER_FAIL'] == '1':\n"
            "        raise RuntimeError('worker action failed')\n"
            "    return {'p2': p1 + 100}\n",
            encoding="utf-8",
            newline="\n",
        )
        monkeypatch.setattr(
            run.action,
            "get_script_artifact_name",
            lambda **kwargs: ("worker_action_wrapper", snippet),
        )
    runs = workflow.get_all_EARs()
    for bound_run in runs:
        bound_run.prepare_directory()
    args = run.get_script_worker_args(0)
    writer = ScriptWriter.create(args, {(0, 0): run.action})
    writer.write_worker(tmp_path)
    writer.write_worker_manifest(
        tmp_path,
        workflow_path=workflow.path,
        submission_idx=0,
        config_directory=hf.config.config_directory,
        config_key=hf.config.config_key,
    )
    script = args.get_script_path(tmp_path)
    shell = get_shell(
        "powershell" if os.name == "nt" else "bash",
        os_name="nt" if os.name == "nt" else "posix",
        executable_args=["-NoProfile"] if os.name == "nt" else None,
    )
    writer.write_worker_commands_file(
        tmp_path,
        script_path=script,
        shell=shell,
    )
    commands_path = args.get_commands_path(tmp_path)
    if os.name == "nt":
        command = (
            "Invoke-Expression (Get-Content -Raw " f"{shell.quote_arg(commands_path)})"
        )
        worker_command = [*shell.executable, "-Command", command]
    else:
        worker_command = shell.get_command_file_launch_command(str(commands_path))
    client = ScriptWorkerClient(worker_command, dict(os.environ))
    try:
        await client.start()
        process = client.worker.process
        for element_idx, bound_run in enumerate(runs):
            invocation = ScriptWorkerRun(
                client,
                ExecuteScriptRequest(bound_run.id_, 0, 0, element_idx),
                {
                    **os.environ,
                    "WORKER_FAIL": str(exit_codes[element_idx]),
                    "WORKER_EXIT_NOW": str(int(system_exit and element_idx == 0)),
                    "WORKER_EXIT_MESSAGE": exit_message or "",
                },
            )
            assert await invocation.run() == exit_codes[element_idx]
            assert invocation.child_work_times[0] >= 0
            assert invocation.child_orchestration_times[0] >= 0
            assert client.worker.process is process
        await client.stop()
        assert process is not None and process.returncode == 0
    finally:
        await client.abort()
    fresh = hf.Workflow(workflow.path)
    for index, (bound_run, exit_code) in enumerate(zip(fresh.get_all_EARs(), exit_codes)):
        if system_exit and index == 0:
            assert bound_run.get("outputs.p2") is None
            if exit_message is not None:
                assert (
                    bound_run.get_app_std_path().read_text().endswith(f"{exit_message}\n")
                )
        elif exit_code:
            assert "worker action failed" in bound_run.get_app_std_path().read_text()
        else:
            assert bound_run.element.outputs.p2.value == 201
    assert all(bound_run.end_time is None for bound_run in fresh.get_all_EARs())
    for directory in {bound_run.get_directory() for bound_run in runs}:
        directory.rmdir()
