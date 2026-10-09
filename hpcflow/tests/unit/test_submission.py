from __future__ import annotations
from datetime import timedelta
import os
from pathlib import Path
from unittest.mock import Mock
from typing import Any
from typing_extensions import TypedDict
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    MissingEnvironmentError,
    MissingEnvironmentExecutableError,
    MissingEnvironmentExecutableInstanceError,
)
from hpcflow.sdk.core.utils import timedelta_format, timedelta_parse
from hpcflow.sdk.submission.jobscript import group_resource_map_into_jobscripts
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix
from hpcflow.sdk.submission.schedulers.sge import SGEPosix


@pytest.mark.parametrize("scheduler", ["direct", "slurm", "sge"])
@pytest.mark.parametrize("parallelism", [False, True])
def test_containerised_preparation(
    tmp_path, modifiable_config, monkeypatch, scheduler, parallelism
):
    if scheduler != "direct":
        if os.name != "posix":
            pytest.skip("Queued scheduler resource validation requires a POSIX host")
        hf.config.add_scheduler(scheduler)
        if scheduler == "sge":
            hf.config.set(
                "schedulers.sge.parallel_environments",
                {"test": {"num_cores": [1, 2]}},
            )
    resources = (
        {}
        if scheduler == "direct"
        else {"scheduler": scheduler, "os_name": "posix", "shell": "bash"}
    )
    tasks = [
        hf.Task(
            schema=hf.task_schemas.test_t1_conditional_OS,
            inputs={"p1": 1},
            resources={"any": {**resources, "num_cores": cores}},
        )
        for cores in (1, 2)
    ]
    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(name="host plan", tasks=tasks), path=tmp_path
    )
    sub = workflow.add_submission(
        JS_parallelism=parallelism, min_jobscripts=False, status=False
    )
    assert sub is not None
    assert len(sub.jobscripts) == 2
    launch = Mock(side_effect=AssertionError("Must not submit inside the container"))
    monkeypatch.setattr(hf.Jobscript, "submit", launch)
    known_submission = Mock()
    monkeypatch.setattr(hf, "_add_to_known_submissions", known_submission)
    for _, sched in sub.get_unique_schedulers().items():
        monkeypatch.setattr(
            sched, "get_version_info", Mock(side_effect=AssertionError("Host-only probe"))
        )
    for _, shell in sub.get_unique_shells():
        monkeypatch.setattr(
            shell, "get_version_info", Mock(side_effect=AssertionError("Host-only probe"))
        )
    plan = workflow.submit(containerised=True, status=False)
    assert plan["schema_version"] == 1
    assert plan["workflow_path"] == str(Path(workflow.path).resolve())
    assert len(plan["jobscripts"]) == 2
    for job in plan["jobscripts"]:
        path = Path(workflow.path) / job["path"]
        assert path.is_file()
        assert b"\r\n" not in path.read_bytes()
        assert "__HPCFLOW_JOBSCRIPT_PATH__" in job["submit_command"]
        assert job["submission_index"] == sub.index
        assert job["scheduler"] == scheduler
    dependencies = plan["jobscripts"][1]["dependencies"]
    if parallelism:
        assert dependencies == []
    else:
        assert dependencies == [
            {
                "submission_index": 0,
                "jobscript_index": 0,
                "is_array": False,
                "reference": None,
                "placeholder": "__HPCFLOW_JOB_0_0__",
            }
        ]
        if scheduler != "direct":
            assert "__HPCFLOW_JOB_0_0__" in " ".join(
                plan["jobscripts"][1]["submit_command"]
            )
        else:
            assert (
                '--jobscripts "0:0"'
                in (Path(workflow.path) / plan["jobscripts"][1]["path"]).read_text()
            )
    launch.assert_not_called()
    known_submission.assert_not_called()
    assert not sub.submitted_jobscripts
    for js in sub.jobscripts:
        assert js.scheduler_job_ID is None
        assert js.process_ID is None
        assert js.submit_time is None
        assert js.EAR_ID_file_path.is_file()
    assert workflow.submit(containerised=True, status=False) == plan
    reloaded = hf.Workflow(workflow.path)
    assert len(reloaded.submissions) == 1
    assert not reloaded.submissions[0].submitted_jobscripts


@pytest.mark.parametrize("scheduler_cls", [SlurmPosix, SGEPosix])
@pytest.mark.parametrize("already_submitted", [False, True])
@pytest.mark.parametrize("array_dependency", [False, True])
def test_containerised_scheduler_dependency_commands(
    tmp_path, scheduler_cls, already_submitted, array_dependency
):
    sub = Mock(spec=hf.Submission)
    sub.index = 0
    sub.JS_parallelism = True
    sub.workflow = Mock(path=str(tmp_path))
    sub.get_unique_shells.return_value = []
    sub.outstanding_jobscripts = [1] if already_submitted else [0, 1]
    scheduler = scheduler_cls()
    jobscripts = []
    for idx in (0, 1):
        js = Mock(spec=hf.Jobscript)
        js.index = idx
        js.submission = sub
        js.is_scheduled = True
        js.is_array = array_dependency
        js.scheduler_job_ID = "12345" if already_submitted and idx == 0 else None
        js.scheduler = scheduler
        js.scheduler_name = "slurm" if scheduler_cls is SlurmPosix else "sge"
        js.shell_name = "bash"
        js.dependencies = {(0, 0): {"is_array": array_dependency}} if idx else {}
        js._prepare_jobscript.return_value = tmp_path / f"js_{idx}.sh"
        jobscripts.append(js)
    sub.jobscripts = jobscripts
    for js in jobscripts:
        js._get_submit_dependencies.side_effect = (
            lambda refs, job=js: hf.Jobscript._get_submit_dependencies(job, refs)
        )
    plan = hf.Submission._prepare_containerised(sub)
    dependency = plan[-1]["dependencies"][0]
    assert dependency["is_array"] is array_dependency
    assert dependency["reference"] == ("12345" if already_submitted else None)
    ref = "12345" if already_submitted else "__HPCFLOW_JOB_0_0__"
    command = plan[-1]["submit_command"]
    if scheduler_cls is SlurmPosix:
        assert command == [
            "sbatch",
            "--parsable",
            "--dependency",
            f"{'aftercorr' if array_dependency else 'afterany'}:{ref}",
            "__HPCFLOW_JOBSCRIPT_PATH__",
        ]
    else:
        assert command == [
            "qsub",
            "-terse",
            "-hold_jid_ad" if array_dependency else "-hold_jid",
            ref,
            "__HPCFLOW_JOBSCRIPT_PATH__",
        ]
    assert len(plan) == (1 if already_submitted else 2)


def test_containerised_natural_dependencies(tmp_path):
    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(
            name="dependencies",
            tasks=[
                hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": 1}),
                hf.Task(
                    schema=(
                        hf.task_schemas.test_t2_ps
                        if os.name == "nt"
                        else hf.task_schemas.test_t2_bash
                    ),
                    resources={"any": {"num_cores": 2}},
                ),
            ],
        ),
        path=tmp_path,
    )
    plan = workflow.submit(
        containerised=True, status=False, JS_parallelism=True, min_jobscripts=False
    )
    assert len(plan["jobscripts"]) == 2
    assert plan["jobscripts"][1]["dependencies"][0]["jobscript_index"] == 0
    assert not workflow.submissions[0].submitted_jobscripts


@pytest.mark.parametrize("modify_js", [False, True])
def test_containerised_multiple_submissions(tmp_path, monkeypatch, capsys, modify_js):
    workflow = hf.Workflow.from_template(
        hf.WorkflowTemplate(
            name="multiple",
            tasks=[
                hf.Task(schema=hf.task_schemas.test_t1_conditional_OS, inputs={"p1": i})
                for i in (1, 2)
            ],
        ),
        path=tmp_path,
    )
    workflow.add_submission(tasks=[0], status=False)
    workflow.add_submission(tasks=[1], status=False)
    edited_paths = []

    def confirm():
        for sub in workflow.submissions:
            for js in sub.jobscripts:
                assert js.jobscript_path.is_file()
                with js.jobscript_path.open("a", newline="\n") as handle:
                    handle.write("\n# edited before returning the plan\n")
                edited_paths.append(js.jobscript_path)
        return "y"

    prompt = Mock(side_effect=confirm)
    monkeypatch.setattr("builtins.input", prompt)
    plan = workflow.submit(containerised=True, status=False, modify_js=modify_js)
    assert [job["submission_index"] for job in plan["jobscripts"]] == [0, 1]
    assert all(not sub.submitted_jobscripts for sub in workflow.submissions)
    assert prompt.call_count == (1 if modify_js else 0)
    for path in edited_paths:
        assert path.read_text().endswith("# edited before returning the plan\n")
    output = capsys.readouterr()
    assert not output.out
    for path in edited_paths:
        assert str(path) in output.err


def test_containerised_modify_js_direct_submission(tmp_path, monkeypatch):
    workflow = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
    sub = workflow.add_submission(status=False)
    prompt = Mock(return_value="y")
    monkeypatch.setattr("builtins.input", prompt)
    prepared = sub.submit(status=None, containerised=True, modify_js=True)
    assert len(prepared) == 1
    prompt.assert_called_once()
    assert not sub.submitted_jobscripts


@pytest.mark.parametrize("error", [EOFError, KeyboardInterrupt])
def test_containerised_modify_js_interrupted(tmp_path, cli_runner, monkeypatch, error):
    monkeypatch.setattr("builtins.input", Mock(side_effect=error))
    result = cli_runner(
        [
            "demo-workflow",
            "go",
            "workflow_1",
            "--path",
            str(tmp_path),
            "--containerised",
            "--modify-js",
        ],
        input="",
    )
    assert result.exit_code != 0
    assert not result.stdout


class _Example(TypedDict):
    resources: list[list[int]]
    expected: list[dict[str, Any]]


def test_group_resource_map_into_jobscripts() -> None:
    # x-axis corresponds to elements; y-axis corresponds to actions:
    examples: tuple[_Example, ...] = (
        {
            "resources": [
                [1, 1, 1, 2, -1, 2, 4, -1, 1],
                [1, 3, 1, 2, 2, 2, 4, 4, 1],
                [1, 1, 3, 2, 2, 2, 4, -1, 1],
            ],
            "expected": [
                {
                    "resources": 1,
                    "elements": {0: [0, 1, 2], 1: [0], 2: [0, 1], 8: [0, 1, 2]},
                },
                {"resources": 2, "elements": {3: [0, 1, 2], 4: [1, 2], 5: [0, 1, 2]}},
                {"resources": 4, "elements": {6: [0, 1, 2], 7: [1]}},
                {"resources": 3, "elements": {1: [1]}},
                {"resources": 1, "elements": {1: [2]}},
                {"resources": 3, "elements": {2: [2]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [8, 8, 1],
                [4, 4, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 2]}},
                {"resources": 8, "elements": {0: [1], 1: [1]}},
                {"resources": 4, "elements": {0: [2], 1: [2]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [2, 2, 1],
                [4, 4, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0, 1], 1: [0, 1]}},
                {"resources": 1, "elements": {2: [1, 2]}},
                {"resources": 4, "elements": {0: [2], 1: [2]}},
            ],
        },
        {
            "resources": [
                [2, 1, 2],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "elements": {1: [0, 1, 2]}},
                {"resources": 2, "elements": {0: [0], 2: [0]}},
                {"resources": 1, "elements": {0: [1, 2], 2: [1, 2]}},
            ],
        },
        {
            "resources": [
                [2, -1, 2],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 2: [0]}},
                {"resources": 1, "elements": {0: [1, 2], 1: [1, 2], 2: [1, 2]}},
            ],
        },
        {
            "resources": [
                [1, 1],
                [1, 1],
                [1, 1],
            ],
            "expected": [{"resources": 1, "elements": {0: [0, 1, 2], 1: [0, 1, 2]}}],
        },
        {
            "resources": [
                [1, 1, 1],
                [1, 1, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "elements": {0: [0, 1, 2], 1: [0, 1, 2], 2: [0, 2]}}
            ],
        },
        {
            "resources": [
                [1, 1, -1],
                [1, 1, 1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 1, "elements": {0: [0, 1, 2], 1: [0, 1, 2], 2: [1, 2]}}
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 4, -1],
                [2, 2, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1, 2]}},
                {"resources": 2, "elements": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 4, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1, 2]}},
                {"resources": 1, "elements": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, 8, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1]}},
                {"resources": 8, "elements": {1: [2]}},
                {"resources": 1, "elements": {0: [3], 1: [3]}},
            ],
        },
        {
            "resources": [
                [2, 2, -1],
                [4, 4, 1],
                [4, -1, -1],
                [1, 1, 1],
            ],
            "expected": [
                {"resources": 2, "elements": {0: [0], 1: [0]}},
                {"resources": 1, "elements": {2: [1, 3]}},
                {"resources": 4, "elements": {0: [1, 2], 1: [1]}},
                {"resources": 1, "elements": {0: [3], 1: [3]}},
            ],
        },
    )
    for i in examples:
        jobscripts_i, _ = group_resource_map_into_jobscripts(i["resources"])
        assert jobscripts_i == i["expected"]


def test_timedelta_parse_format_round_trip() -> None:
    td = timedelta(days=2, hours=25, minutes=92, seconds=77)
    td_str = timedelta_format(td)
    assert td_str == timedelta_format(timedelta_parse(td_str))


def test_raise_missing_env_executable(tmp_path) -> None:
    exec_name = (
        "my_executable"  # null_env (the default) has no executable "my_executable"
    )
    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[hf.Action(commands=[hf.Command(command=f"<<executable:{exec_name}>>")])],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    with pytest.raises(MissingEnvironmentExecutableError):
        wk.add_submission()


def test_raise_missing_matching_env_executable(
    tmp_path, reload_template_components
) -> None:
    """The executable label exists, but no a matching instance."""
    env_name = "my_hpcflow_env"
    exec_label = "my_exec_name"
    env = hf.Environment(
        name=env_name,
        executables=[
            hf.Executable(
                label=exec_label,
                instances=[
                    hf.ExecutableInstance(
                        command="command", num_cores=1, parallel_mode=None
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(env, skip_duplicates=True)

    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment(environment=env_name)],
                commands=[hf.Command(command=f"<<executable:{exec_label}>>")],
            )
        ],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
        resources={"any": {"num_cores": 2}},
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    with pytest.raises(MissingEnvironmentExecutableInstanceError):
        wk.add_submission()


def test_no_raise_matching_env_executable(tmp_path, reload_template_components) -> None:
    env_name = "my_hpcflow_env"
    exec_label = "my_exec_name"
    env = hf.Environment(
        name=env_name,
        executables=[
            hf.Executable(
                label=exec_label,
                instances=[
                    hf.ExecutableInstance(
                        command="command", num_cores=2, parallel_mode=None
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(env, skip_duplicates=True)

    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment(environment=env_name)],
                commands=[hf.Command(command=f"<<executable:{exec_label}>>")],
            )
        ],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
        resources={"any": {"num_cores": 2}},
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    wk.add_submission()


def test_raise_missing_env(tmp_path) -> None:
    env_name = "my_hpcflow_env"
    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[hf.Action(environments=[hf.ActionEnvironment(environment=env_name)])],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    with pytest.raises(MissingEnvironmentError):
        wk.add_submission()


def test_custom_env_and_executable(tmp_path, reload_template_components) -> None:
    env_name = "my_hpcflow_env"
    exec_label = "my_exec_name"
    env = hf.Environment(
        name=env_name,
        executables=[
            hf.Executable(
                label=exec_label,
                instances=[
                    hf.ExecutableInstance(
                        command="command", num_cores=1, parallel_mode=None
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(env, skip_duplicates=True)

    ts = hf.TaskSchema(
        objective="test_sub",
        actions=[
            hf.Action(
                environments=[hf.ActionEnvironment(environment=env_name)],
                commands=[hf.Command(command=f"<<executable:{exec_label}>>")],
            )
        ],
    )
    t1 = hf.Task(schema=ts)
    wkt = hf.WorkflowTemplate(
        name="test_sub",
        tasks=[t1],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    wk.add_submission()


def test_unique_schedulers_one_direct(tmp_path) -> None:
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
    )
    wkt = hf.WorkflowTemplate(name="temp", tasks=[t1, t2])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert sub is not None
    scheds = sub.get_unique_schedulers()

    assert len(scheds) == 1


def test_unique_schedulers_one_direct_distinct_resources(tmp_path) -> None:
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"num_cores": 1}},
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"num_cores": 2}},
    )
    wkt = hf.WorkflowTemplate(name="temp", tasks=[t1, t2])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert sub is not None
    scheds = sub.get_unique_schedulers()

    assert len(scheds) == 1


@pytest.mark.slurm
def test_unique_schedulers_one_SLURM(modifiable_config, tmp_path) -> None:
    hf.config.add_scheduler("slurm")
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"scheduler": "slurm"}},
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"scheduler": "slurm"}},
    )
    wkt = hf.WorkflowTemplate(name="temp", tasks=[t1, t2])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert sub is not None
    scheds = sub.get_unique_schedulers()

    assert len(scheds) == 1


@pytest.mark.slurm
def test_unique_schedulers_one_SLURM_distinct_resources(
    modifiable_config, tmp_path
) -> None:
    hf.config.add_scheduler("slurm")
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"scheduler": "slurm", "num_cores": 1}},
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"scheduler": "slurm", "num_cores": 2}},
    )
    wkt = hf.WorkflowTemplate(name="temp", tasks=[t1, t2])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert sub is not None
    scheds = sub.get_unique_schedulers()

    assert len(scheds) == 1


@pytest.mark.slurm
def test_unique_schedulers_two_direct_and_SLURM(modifiable_config, tmp_path) -> None:
    hf.config.add_scheduler("slurm")
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"scheduler": "direct"}},
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        inputs={"p1": 1},
        resources={"any": {"scheduler": "slurm"}},
    )
    wkt = hf.WorkflowTemplate(name="temp", tasks=[t1, t2])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert sub is not None
    scheds = sub.get_unique_schedulers()

    assert len(scheds) == 2


def test_scheduler_config_defaults(modifiable_config, tmp_path) -> None:
    """Check default options defined in the config are merged into jobscript resources."""

    # note we use the `shebang_executable` for this test. On Windows, this will not be
    # included in the jobscript, so it is effectively ignored, but the test is still
    # valid.
    hf.config.set("schedulers.direct.defaults.shebang_executable", ["/bin/bash"])

    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        inputs={"p1": 1},
        resources={"any": {"scheduler": "direct"}},
    )
    t2 = hf.Task(
        schema=hf.task_schemas.test_t1_ps,
        inputs={"p1": 1},
        resources={
            "any": {
                "scheduler": "direct",
                "scheduler_args": {"shebang_executable": ["bash"]},
            }
        },
    )
    wkt = hf.WorkflowTemplate(name="temp", tasks=[t1, t2])
    wk = hf.Workflow.from_template(
        template=wkt,
        path=tmp_path,
    )
    sub = wk.add_submission()
    assert sub is not None
    assert sub.jobscripts[0].resources.scheduler_args == {
        "shebang_executable": ["/bin/bash"]
    }
    assert sub.jobscripts[1].resources.scheduler_args == {"shebang_executable": ["bash"]}
