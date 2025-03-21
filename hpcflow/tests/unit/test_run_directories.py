from pathlib import Path
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import make_workflow


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_run_directories(null_config, tmp_path, store):
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
