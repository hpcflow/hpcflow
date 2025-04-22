from __future__ import annotations
from datetime import timedelta
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


@pytest.fixture
def null_config(tmp_path):
    if not hf.is_config_loaded:
        hf.load_config(config_dir=tmp_path)


class _Example(TypedDict):
    resources: list[list[int]]
    expected: list[dict[str, Any]]


def test_group_resource_map_into_jobscripts(null_config) -> None:
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


def test_timedelta_parse_format_round_trip(null_config) -> None:
    td = timedelta(days=2, hours=25, minutes=92, seconds=77)
    td_str = timedelta_format(td)
    assert td_str == timedelta_format(timedelta_parse(td_str))


def test_raise_missing_env_executable(new_null_config, tmp_path) -> None:
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


def test_raise_missing_matching_env_executable(new_null_config, tmp_path) -> None:
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

    hf.reload_template_components()  # remove extra envs


def test_no_raise_matching_env_executable(new_null_config, tmp_path) -> None:
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

    hf.reload_template_components()  # remove extra envs


def test_raise_missing_env(new_null_config, tmp_path) -> None:
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


def test_custom_env_and_executable(new_null_config, tmp_path) -> None:
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

    hf.reload_template_components()  # remove extra envs


def test_unique_schedulers_one_direct(new_null_config, tmp_path) -> None:
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


def test_unique_schedulers_one_direct_distinct_resources(
    new_null_config, tmp_path
) -> None:
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
def test_unique_schedulers_one_SLURM(new_null_config, tmp_path) -> None:
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
    new_null_config, tmp_path
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
def test_unique_schedulers_two_direct_and_SLURM(new_null_config, tmp_path) -> None:
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


def test_scheduler_config_defaults(new_null_config, tmp_path) -> None:
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
