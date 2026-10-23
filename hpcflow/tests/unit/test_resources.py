from __future__ import annotations
import os
import copy
from pathlib import Path
from types import SimpleNamespace
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import UnsupportedSchedulerError
from hpcflow.sdk.core.json_like import JSONed
from hpcflow.sdk.core.types import ResourceSpecArgs
from hpcflow.sdk.submission.schedulers.slurm import SlurmPosix


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("level", ["workflow", "task", "both", "scoped"])
@pytest.mark.parametrize("clear", [False, True])
@pytest.mark.parametrize("inherit", [False, True])
def test_scheduler_directives_merge_with_defaults(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    store: str,
    level: str,
    clear: bool,
    inherit: bool,
) -> None:
    defaults: dict[str, dict[str, str | list[str] | None]] = {
        "directives": {
            "--account": "default-account",
            "--time": "00:10:00",
            "--export": ["ALL", "DEFAULT=1"],
        }
    }
    workflow_args: dict[str, dict[str, str | list[str] | None]] = {
        "directives": {"--time": "00:20:00", "--qos": "workflow-qos"}
    }
    task_args: dict[str, dict[str, str | list[str] | None]] = {
        "directives": {
            "--time": "00:30:00",
            "--exclusive": None,
            "--export": ["ALL", "TASK=1"],
        }
    }
    if clear:
        if level == "workflow":
            workflow_args["directives"] = {}
        else:
            task_args["directives"] = {}
    workflow_resources: dict[str, ResourceSpecArgs] | None = (
        {"any": {"scheduler_args": workflow_args}}
        if level in ("workflow", "both", "scoped")
        else None
    )
    task_resources: dict[str, ResourceSpecArgs] | None = (
        {"main" if level == "scoped" else "any": {"scheduler_args": task_args}}
        if level in ("task", "both", "scoped")
        else None
    )
    if level == "workflow":
        assert workflow_resources is not None
        workflow_resources["any"]["inherit_directives"] = inherit
    else:
        assert task_resources is not None
        task_resources["main" if level == "scoped" else "any"][
            "inherit_directives"
        ] = inherit
    template = hf.WorkflowTemplate(
        name="directive_merge",
        resources=workflow_resources,
        tasks=[
            hf.Task(
                schema=hf.task_schemas.test_t1_bash,
                inputs={"p1": 101},
                resources=task_resources,
            )
        ],
    )
    workflow = hf.Workflow.from_template(template, path=tmp_path, store=store)
    workflow = hf.Workflow(workflow.path)
    action = workflow.tasks[0].template.schemas[0].actions[0]
    resolved = workflow.tasks[0].elements[0].iterations[0].get_resources(action)
    resources = hf.ElementResources(**{**resolved, "scheduler": "slurm"})
    config = SimpleNamespace(
        shells=hf.config.shells,
        schedulers={"slurm": {"defaults": defaults}},
        default_shell=hf.config.default_shell,
    )
    monkeypatch.setattr(
        hf.ElementResources,
        "_app",
        SimpleNamespace(config=config, run_time_info=hf.run_time_info),
    )
    resources.set_defaults()
    SlurmPosix.process_resources(
        resources,
        {"defaults": {}, "partitions": {"test-partition": {"num_cores": [1, 1, 1]}}},
    )
    expected = copy.deepcopy(defaults["directives"])
    if workflow_resources:
        expected.update(workflow_args["directives"])
    if task_resources:
        expected.update(task_args["directives"])
    if not inherit:
        expected = copy.deepcopy(
            (workflow_args if level == "workflow" else task_args)["directives"]
        )
    assert resources.scheduler_args["directives"] == expected

    rendered = SlurmPosix(**resources.scheduler_args).format_directives(
        resources, num_elements=1, is_array=False, sub_idx=0, js_idx=0
    )
    assert "#SBATCH --partition test-partition" in rendered.splitlines()
    assert "#SBATCH --ntasks 1" in rendered.splitlines()
    if not inherit and clear:
        assert not any(
            line.startswith(("#SBATCH --account", "#SBATCH --time", "#SBATCH --qos"))
            for line in rendered.splitlines()
        )
    else:
        assert ("#SBATCH --account default-account" in rendered.splitlines()) == inherit
        if "--time" in expected:
            assert f"#SBATCH --time {expected['--time']}" in rendered.splitlines()
        if task_resources and not clear:
            assert "#SBATCH --exclusive" in rendered.splitlines()
            assert "#SBATCH --export TASK=1" in rendered.splitlines()
            assert "#SBATCH --export DEFAULT=1" not in rendered.splitlines()
    assert defaults["directives"]["--time"] == "00:10:00"
    if not (clear and level == "workflow"):
        assert workflow_args["directives"]["--time"] == "00:20:00"


@pytest.mark.parametrize("method", ["merge_other", "merge_one"])
def test_scheduler_args_merge_preserves_inputs_and_empty_directives(method: str) -> None:
    lower_args: dict[str, JSONed] = {
        "directives": {"--account": "account", "--time": "00:10:00"},
        "options": {"--legacy": "value"},
        "submit_cmd": "custom-sbatch",
    }
    lower = hf.ResourceList.from_json_like({"any": {"scheduler_args": lower_args}})
    higher = hf.ResourceList.from_json_like(
        {"any": {"scheduler_args": {"directives": {}}}}
    )
    if method == "merge_other":
        higher.merge_other(lower)
    else:
        higher.merge_one(lower[0])
    args = higher[0].scheduler_args
    assert args["directives"] == lower_args["directives"]
    assert args["submit_cmd"] == "custom-sbatch"
    args["options"]["--legacy"] = "changed"
    assert lower[0].scheduler_args == lower_args


@pytest.mark.parametrize("key", ["options", "directives"])
def test_scheduler_args_defaults_merge(monkeypatch: pytest.MonkeyPatch, key: str) -> None:
    defaults = {key: {"--account": "account", "--time": "00:10:00"}}
    original = copy.deepcopy(defaults)
    config = SimpleNamespace(
        shells=hf.config.shells,
        schedulers={"slurm": {"defaults": defaults}},
        default_shell=hf.config.default_shell,
    )
    monkeypatch.setattr(
        hf.ElementResources,
        "_app",
        SimpleNamespace(config=config, run_time_info=hf.run_time_info),
    )
    resources = hf.ElementResources(
        scheduler="slurm", scheduler_args={key: {"--time": "00:20:00"}}
    )
    resources.set_defaults()
    resources.set_defaults()
    assert resources.scheduler_args[key] == {
        "--account": "account",
        "--time": "00:20:00",
    }
    assert defaults == original

    empty = hf.ElementResources(scheduler="slurm", scheduler_args={key: {}})
    empty.set_defaults()
    assert empty.scheduler_args[key] == defaults[key]


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("scope", ["any", "main"])
@pytest.mark.parametrize("key", ["directives", "options"])
def test_directive_inheritance_resumes_after_barrier(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: str, scope: str, key: str
) -> None:
    task_resources: dict[str, ResourceSpecArgs] = {
        scope: {
            "inherit_directives": True,
            "scheduler_args": {key: {"--time": "00:20:00"}},
        }
    }
    template = hf.WorkflowTemplate(
        name="inheritance_barrier",
        resources={
            "any": {
                "inherit_directives": False,
                "scheduler_args": {key: {"--qos": "new-baseline"}},
            }
        },
        tasks=[
            hf.Task(
                schema=hf.task_schemas.test_t1_bash,
                inputs={"p1": 101},
                resources=task_resources,
            )
        ],
    )
    workflow = hf.Workflow.from_template(template, path=tmp_path, store=store)
    workflow = hf.Workflow(workflow.path)
    action = workflow.tasks[0].template.schemas[0].actions[0]
    resources = workflow.tasks[0].elements[0].iterations[0].get_resources_obj(action)
    resources.scheduler = "slurm"
    config = SimpleNamespace(
        shells=hf.config.shells,
        default_shell=hf.config.default_shell,
        schedulers={"slurm": {"defaults": {key: {"--account": "discarded"}}}},
    )
    monkeypatch.setattr(
        hf.ElementResources,
        "_app",
        SimpleNamespace(config=config, run_time_info=hf.run_time_info),
    )
    resources.set_defaults()
    resources.set_defaults()
    assert resources.scheduler_args[key] == {
        "--qos": "new-baseline",
        "--time": "00:20:00",
    }


@pytest.mark.parametrize("key", ["directives", "options"])
def test_disable_inheritance_without_local_directives(
    monkeypatch: pytest.MonkeyPatch, key: str
) -> None:
    lower = hf.ResourceList.from_json_like(
        {"any": {"scheduler_args": {key: {"--account": "workflow"}}}}
    )
    higher = hf.ResourceList.from_json_like({"any": {"inherit_directives": False}})
    higher.merge_other(lower)
    resources = hf.ElementResources(scheduler="slurm", **higher[0]._get_members())
    config = SimpleNamespace(
        shells=hf.config.shells,
        default_shell=hf.config.default_shell,
        schedulers={"slurm": {"defaults": {key: {"--account": "config"}}}},
    )
    monkeypatch.setattr(
        hf.ElementResources,
        "_app",
        SimpleNamespace(config=config, run_time_info=hf.run_time_info),
    )
    resources.set_defaults()
    assert resources.scheduler_args == {}


def test_init_scope_equivalence_simple() -> None:
    rs1 = hf.ResourceSpec(scope=hf.ActionScope.any(), num_cores=1)
    rs2 = hf.ResourceSpec(scope="any", num_cores=1)
    assert rs1 == rs2


def test_init_scope_equivalence_with_kwargs() -> None:
    rs1 = hf.ResourceSpec(
        scope=hf.ActionScope.input_file_generator(file="my_file"), num_cores=1
    )
    rs2 = hf.ResourceSpec(scope="input_file_generator[file=my_file]", num_cores=1)
    assert rs1 == rs2


def test_init_no_args() -> None:
    rs1 = hf.ResourceSpec()
    rs2 = hf.ResourceSpec(scope="any")
    assert rs1 == rs2


def test_resource_list_raise_on_identical_scopes() -> None:
    with pytest.raises(ValueError):
        hf.ResourceList.normalise([{"scope": "any"}, {"scope": "any"}])


def test_merge_other_same_scope() -> None:
    res_lst_1 = hf.ResourceList.from_json_like({"any": {"num_cores": 1}})
    res_lst_2 = hf.ResourceList.from_json_like({"any": {}})
    res_lst_2.merge_other(res_lst_1)
    assert res_lst_2 == hf.ResourceList.from_json_like({"any": {"num_cores": 1}})


def test_merge_other_same_scope_no_overwrite() -> None:
    res_lst_1 = hf.ResourceList.from_json_like({"any": {"num_cores": 1}})
    res_lst_2 = hf.ResourceList.from_json_like({"any": {"num_cores": 2}})
    res_lst_2.merge_other(res_lst_1)
    assert res_lst_2 == hf.ResourceList.from_json_like({"any": {"num_cores": 2}})


def test_merge_other_multi_scope() -> None:
    res_lst_1 = hf.ResourceList.from_json_like({"any": {"num_cores": 1}})
    res_lst_2 = hf.ResourceList.from_json_like({"any": {}, "main": {"num_cores": 3}})
    res_lst_2.merge_other(res_lst_1)
    assert res_lst_2 == hf.ResourceList.from_json_like(
        {"any": {"num_cores": 1}, "main": {"num_cores": 3}}
    )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_merge_other_persistent_workflow_reload(tmp_path: Path, store: str):
    wkt = hf.WorkflowTemplate(
        name="test_load",
        resources={"any": {"num_cores": 2}},
        tasks=[
            hf.Task(
                schema=hf.task_schemas.test_t1_ps,
                inputs={"p1": 101},
            ),
        ],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path, store=store)
    wk = hf.Workflow(wk.path)
    assert wk.template.tasks[0].element_sets[0].resources[0].num_cores == 2


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_use_persistent_resource_spec(tmp_path: Path, store: str):
    # create a workflow from which we can use a resource spec in a new workflow:
    num_cores_check = 2
    wk_base = hf.Workflow.from_template_data(
        template_name="wk_base",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schema=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
                resources={"any": {"num_cores": num_cores_check}},
            )
        ],
    )
    resource_spec = wk_base.tasks[0].template.element_sets[0].resources[0]

    wk = hf.Workflow.from_template_data(
        template_name="wk",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schema=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
            ),
        ],
        resources=[resource_spec],
    )

    assert wk.tasks[0].template.element_sets[0].resources[0].num_cores == num_cores_check


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_use_persistent_resource_list(tmp_path: Path, store: str):
    # create a workflow from which we can use the resource list in a new workflow:
    num_cores_check = 2
    wk_base = hf.Workflow.from_template_data(
        template_name="wk_base",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schema=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
                resources={"any": {"num_cores": num_cores_check}},
            )
        ],
    )
    resource_list = wk_base.tasks[0].template.element_sets[0].resources

    wk = hf.Workflow.from_template_data(
        template_name="wk",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schema=[hf.task_schemas.test_t1_ps],
                inputs=[hf.InputValue("p1", 101)],
            ),
        ],
        resources=resource_list[:],  # must pass a list!
    )

    assert wk.tasks[0].template.element_sets[0].resources[0].num_cores == num_cores_check


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_default_scheduler_set(tmp_path: Path, store: str):
    wk = hf.Workflow.from_template_data(
        template_name="wk",
        path=tmp_path,
        store=store,
        tasks=[
            hf.Task(
                schema=[hf.task_schemas.test_t1_bash],
                inputs=[hf.InputValue("p1", 101)],
            ),
        ],
    )
    wk.add_submission()
    assert wk.submissions[0].jobscripts[0].scheduler_name == hf.config.default_scheduler


def test_scheduler_case_insensitive() -> None:
    rs1 = hf.ResourceSpec(scheduler="direct")
    rs2 = hf.ResourceSpec(scheduler="dIrEcT")
    assert rs1 == rs2
    assert rs1.scheduler == rs2.scheduler == "direct"


def test_scheduler_strip() -> None:
    rs1 = hf.ResourceSpec(scheduler="  direct ")
    rs2 = hf.ResourceSpec(scheduler="direct")
    assert rs1 == rs2
    assert rs1.scheduler == rs2.scheduler == "direct"


def test_shell_case_insensitive() -> None:
    shell_name = "bash" if os.name == "posix" else "powershell"
    shell_name_title = shell_name
    n = shell_name_title[0]
    shell_name_title = shell_name_title.replace(n, n.upper())
    assert shell_name != shell_name_title
    rs1 = hf.ResourceSpec(shell=shell_name)
    rs2 = hf.ResourceSpec(shell=shell_name_title)
    assert rs1 == rs2
    assert rs1.shell == rs2.shell == shell_name


def test_shell_strip() -> None:
    shell_name = "bash" if os.name == "posix" else "powershell"
    rs1 = hf.ResourceSpec(shell=f"  {shell_name} ")
    rs2 = hf.ResourceSpec(shell=shell_name)
    assert rs1 == rs2
    assert rs1.shell == rs2.shell == shell_name


def test_os_name_case_insensitive():
    rs1 = hf.ResourceSpec(os_name="nt")
    rs2 = hf.ResourceSpec(os_name="NT")
    assert rs1 == rs2
    assert rs1.os_name == rs2.os_name == "nt"


def test_os_name_strip() -> None:
    rs1 = hf.ResourceSpec(os_name="  nt ")
    rs2 = hf.ResourceSpec(os_name="nt")
    assert rs1 == rs2
    assert rs1.os_name == rs2.os_name == "nt"


def test_raise_on_unsupported_scheduler(tmp_path: Path):
    # slurm not supported by default config file:
    wk = hf.Workflow.from_template_data(
        template_name="wk1",
        path=tmp_path,
        tasks=[
            hf.Task(
                schema=[hf.task_schemas.test_t1_bash],
                inputs=[hf.InputValue("p1", 101)],
                resources=[hf.ResourceSpec(scheduler="slurm")],
            )
        ],
    )
    with pytest.raises(UnsupportedSchedulerError):
        wk.add_submission()


def test_can_use_non_default_scheduler(modifiable_config, tmp_path: Path):
    # for either OS choose a compatible scheduler not set by default:
    if os.name == "nt":
        opt_scheduler = "direct_posix"  # i.e for WSL
    else:
        opt_scheduler = "slurm"
    hf.config.add_scheduler(opt_scheduler)

    wk = hf.Workflow.from_template_data(
        template_name="wk1",
        path=tmp_path,
        tasks=[
            hf.Task(
                schema=[hf.task_schemas.test_t1_bash],
                inputs=[hf.InputValue("p1", 101)],
                resources=[hf.ResourceSpec(scheduler=opt_scheduler)],
            )
        ],
    )
    wk.add_submission()


@pytest.mark.integration
def test_random_seed_env_var_set_and_read(tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="random",
        actions=[hf.Action(commands=[hf.Command("echo $HPCFLOW_RUN_RANDOM_SEED")])],
    )
    wk = hf.Workflow.from_template_data(
        template_name="random_seed",
        path=tmp_path,
        tasks=[hf.Task(schema=s1)],
    )
    wk.submit(wait=True)
    assert int(
        wk.submissions[0].jobscripts[0].get_stdout().strip()
    )  # check an integer is printed


def test_specified_random_seed(tmp_path: Path):
    seed = 1234
    s1 = hf.TaskSchema(
        objective="random",
        actions=[hf.Action(commands=[hf.Command("echo $HPCFLOW_RUN_RANDOM_SEED")])],
    )
    wk = hf.Workflow.from_template_data(
        template_name="random_seed",
        path=tmp_path,
        resources={"any": {"random_seed": seed}},
        tasks=[hf.Task(schema=s1)],
    )
    run = wk.get_all_EARs()[0]
    assert run.resources.random_seed == seed


def test_task_specified_random_seed(tmp_path: Path):
    seed = 1234
    s1 = hf.TaskSchema(
        objective="random",
        actions=[hf.Action(commands=[hf.Command("echo $HPCFLOW_RUN_RANDOM_SEED")])],
    )
    wk = hf.Workflow.from_template_data(
        template_name="random_seed",
        path=tmp_path,
        tasks=[hf.Task(schema=s1, resources={"main": {"random_seed": seed}})],
    )
    run = wk.get_all_EARs()[0]
    assert run.resources.random_seed == seed


def test_random_seeds_are_different(tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="random",
        actions=[hf.Action(commands=[hf.Command("echo $HPCFLOW_RUN_RANDOM_SEED")])],
    )
    wk1 = hf.Workflow.from_template_data(
        template_name="random_seed_1",
        path=tmp_path,
        tasks=[hf.Task(schema=s1)],
    )
    wk2 = hf.Workflow.from_template_data(
        template_name="random_seed_2",
        path=tmp_path,
        tasks=[hf.Task(schema=s1)],
    )
    run_1 = wk1.get_all_EARs()[0]
    run_2 = wk2.get_all_EARs()[0]
    assert run_1.resources.random_seed != run_2.resources.random_seed


def test_repeats_generates_sequence(tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="random",
        actions=[hf.Action(commands=[hf.Command("echo $HPCFLOW_RUN_RANDOM_SEED")])],
    )
    wk = hf.Workflow.from_template_data(
        template_name="random_seed",
        path=tmp_path,
        tasks=[hf.Task(schema=s1, repeats=2)],
    )
    elem_set = wk.tasks[0].template.element_sets[0]
    reps = elem_set.repeats
    seqs = elem_set.sequences
    assert reps
    assert seqs
    assert len(seqs) == 1
    assert len(reps) == 1
    assert seqs[0].path == "resources.any.random_seed"
    assert seqs[0].values
    assert len(seqs[0].values) == reps[0].number == 2
    assert seqs[0]._values_method_args
    assert seqs[0]._values_method_args["master_seed"] == reps[0].master_seed


def test_repeats_generates_sequence_overriden_master_seed(tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="random",
        actions=[hf.Action(commands=[hf.Command("echo $HPCFLOW_RUN_RANDOM_SEED")])],
    )
    seed = 1234
    wk = hf.Workflow.from_template_data(
        template_name="random_seed",
        path=tmp_path,
        tasks=[
            hf.Task(
                schema=s1,
                repeats={"number": 3, "master_seed": seed, "nesting_order": 2.5},
            )
        ],
    )
    elem_set = wk.tasks[0].template.element_sets[0]
    reps = elem_set.repeats
    seqs = elem_set.sequences
    assert reps
    assert seqs
    assert len(seqs) == 1
    assert len(reps) == 1
    assert seqs[0].path == "resources.any.random_seed"
    assert seqs[0].nesting_order == reps[0].nesting_order == 2.5
    assert seqs[0].values
    assert len(seqs[0].values) == reps[0].number == 3
    assert seqs[0]._values_method_args
    assert seqs[0]._values_method_args["master_seed"] == reps[0].master_seed == seed
