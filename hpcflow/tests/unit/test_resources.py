from __future__ import annotations
import os
from pathlib import Path
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import UnsupportedSchedulerError


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
def test_merge_other_persistent_workflow_reload(null_config, tmp_path: Path, store: str):
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
def test_use_persistent_resource_spec(null_config, tmp_path: Path, store: str):
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
def test_use_persistent_resource_list(null_config, tmp_path: Path, store: str):
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
def test_default_scheduler_set(new_null_config, tmp_path: Path, store: str):
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


def test_scheduler_case_insensitive(null_config) -> None:
    rs1 = hf.ResourceSpec(scheduler="direct")
    rs2 = hf.ResourceSpec(scheduler="dIrEcT")
    assert rs1 == rs2
    assert rs1.scheduler == rs2.scheduler == "direct"


def test_scheduler_strip(null_config) -> None:
    rs1 = hf.ResourceSpec(scheduler="  direct ")
    rs2 = hf.ResourceSpec(scheduler="direct")
    assert rs1 == rs2
    assert rs1.scheduler == rs2.scheduler == "direct"


def test_shell_case_insensitive(null_config) -> None:
    shell_name = "bash" if os.name == "posix" else "powershell"
    shell_name_title = shell_name
    n = shell_name_title[0]
    shell_name_title = shell_name_title.replace(n, n.upper())
    assert shell_name != shell_name_title
    rs1 = hf.ResourceSpec(shell=shell_name)
    rs2 = hf.ResourceSpec(shell=shell_name_title)
    assert rs1 == rs2
    assert rs1.shell == rs2.shell == shell_name


def test_shell_strip(null_config) -> None:
    shell_name = "bash" if os.name == "posix" else "powershell"
    rs1 = hf.ResourceSpec(shell=f"  {shell_name} ")
    rs2 = hf.ResourceSpec(shell=shell_name)
    assert rs1 == rs2
    assert rs1.shell == rs2.shell == shell_name


def test_os_name_case_insensitive(null_config):
    rs1 = hf.ResourceSpec(os_name="nt")
    rs2 = hf.ResourceSpec(os_name="NT")
    assert rs1 == rs2
    assert rs1.os_name == rs2.os_name == "nt"


def test_os_name_strip(null_config) -> None:
    rs1 = hf.ResourceSpec(os_name="  nt ")
    rs2 = hf.ResourceSpec(os_name="nt")
    assert rs1 == rs2
    assert rs1.os_name == rs2.os_name == "nt"


def test_raise_on_unsupported_scheduler(new_null_config, tmp_path: Path):
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


def test_can_use_non_default_scheduler(new_null_config, tmp_path: Path):
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
