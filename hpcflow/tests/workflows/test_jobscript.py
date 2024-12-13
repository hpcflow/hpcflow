import os
from pathlib import Path
import pytest

from hpcflow.app import app as hf


@pytest.mark.integration
@pytest.mark.parametrize("exit_code", [0, 1, 98, -1, -123124])
def test_action_exit_code_parsing(null_config, tmp_path: Path, exit_code: int):
    act = hf.Action(commands=[hf.Command(command=f"exit {exit_code}")])
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[act],
    )
    t1 = hf.Task(schema=[s1])
    wk = hf.Workflow.from_template_data(tasks=[t1], template_name="test", path=tmp_path)
    wk.submit(wait=True, add_to_known=False)
    recorded_exit = wk.get_EARs_from_IDs([0])[0].exit_code
    if os.name == "posix":
        # exit code from bash wraps around:
        exit_code %= 256
    assert recorded_exit == exit_code
