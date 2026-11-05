from pathlib import Path
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_workflow
from hpcflow.sdk.core.types import ResourceSpecArgs


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_run_directories(tmp_path, store):
    wk = make_workflow(
        schemas_spec=[
            [{"p1": None}, ("p1",), "t1"],
            [{"p2": None}, ("p2",), "t2", {"requires_dir": True}],
        ],
        local_inputs={0: ("p1",)},
        local_sequences={1: [("inputs.p2", 2, 0)]},
        path=tmp_path,
        store=store,
    )
    lp_0 = hf.Loop(name="my_loop", tasks=[1], num_iterations=2)
    wk.add_loop(lp_0)
    sub = wk.add_submission()  # populates run directories

    run_dirs = wk.get_run_directories()

    assert run_dirs[0] is None
    assert str(run_dirs[1]) == str(Path(wk.path).joinpath("execute/t_1/e_0/i_0"))
    assert str(run_dirs[2]) == str(Path(wk.path).joinpath("execute/t_1/e_1/i_0"))
    assert str(run_dirs[3]) == str(Path(wk.path).joinpath("execute/t_1/e_0/i_1"))
    assert str(run_dirs[4]) == str(Path(wk.path).joinpath("execute/t_1/e_1/i_1"))


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize("level", ["workflow", "task", "scoped"])
@pytest.mark.parametrize("relative", [False, True])
def test_scratch_run_directories(
    tmp_path: Path, store: str, level: str, relative: bool
) -> None:
    scratch = "scratch" if relative else str(tmp_path / "scratch space")
    resource: ResourceSpecArgs = {"scratch": scratch}
    schema = hf.TaskSchema(
        objective="scratch",
        actions=[hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)],
    )
    workflows = []
    for index in range(2):
        wk = hf.Workflow.from_template_data(
            template_name="scratch",
            path=tmp_path / f"workflow_{index}",
            store=store,
            resources={"any": resource} if level == "workflow" else None,
            tasks=[
                hf.Task(
                    schema=schema,
                    repeats=2,
                    resources=(
                        {"main" if level == "scoped" else "any": resource}
                        if level != "workflow"
                        else None
                    ),
                )
            ],
            loops=[hf.Loop(tasks=[0], num_iterations=2)],
        )
        wk.add_submission()
        wk = hf.Workflow(wk.path)
        workflows.append(wk)
        parent = Path(wk.path) / scratch if relative else Path(scratch)
        root = parent / f"{hf.name}_{wk.id_}" / "execute"
        directories = wk.get_run_directories()
        assert directories == [
            root / "e_0" / "i_0",
            root / "e_1" / "i_0",
            root / "e_0" / "i_1",
            root / "e_1" / "i_1",
        ]
        assert wk.get_run_directories(run_ids=[3, 1]) == [
            directories[3],
            directories[1],
        ]
        indices = wk._store.get_dirs_array()[[3, 1]]
        assert wk.get_run_directories(run_ids=[3, 1], dir_indices_arr=indices) == [
            directories[3],
            directories[1],
        ]
        assert not parent.exists()
    assert not set(workflows[0].get_run_directories()).intersection(
        workflows[1].get_run_directories()
    )


@pytest.mark.parametrize("scratch", ["", "   "])
def test_empty_scratch_path(tmp_path: Path, scratch: str) -> None:
    schema = hf.TaskSchema(
        objective="empty_scratch",
        actions=[hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)],
    )
    wk = hf.Workflow.from_template_data(
        template_name="empty_scratch",
        path=tmp_path,
        resources={"any": {"scratch": scratch}},
        tasks=[hf.Task(schema=schema)],
    )
    wk.add_submission()
    with pytest.raises(ValueError, match="non-empty directory path"):
        wk.get_run_directories()


def test_scratch_task_override_and_directoryless_run(tmp_path: Path) -> None:
    schema = hf.TaskSchema(
        objective="mixed_scratch",
        actions=[hf.Action(commands=[hf.Command("echo ready")], requires_dir=True)],
    )
    no_dir_schema = hf.TaskSchema(
        objective="no_directory",
        actions=[hf.Action(commands=[hf.Command("echo ready")], requires_dir=False)],
    )
    parent = tmp_path / "scratch"
    override = tmp_path / "other scratch"
    wk = hf.Workflow.from_template_data(
        template_name="mixed_scratch",
        path=tmp_path,
        resources={"any": {"scratch": str(parent)}},
        tasks=[
            hf.Task(schema=schema, resources={"main": {"scratch": str(override)}}),
            hf.Task(schema=no_dir_schema),
            hf.Task(schema=schema),
        ],
    )
    wk.add_submission()
    subdirectory = Path(f"{hf.name}_{wk.id_}") / "execute"
    assert wk.get_run_directories() == [
        override / subdirectory / "t_0",
        None,
        parent / subdirectory / "t_2",
    ]
    assert not parent.exists()
    assert not override.exists()
