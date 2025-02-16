import os
import time

import pytest

from hpcflow.app import app as hf


@pytest.mark.integration
def test_non_snippet_script_execution(null_config, tmp_path):
    test_str = "non-snippet script!"
    script_name = "my_script.py"
    script_contents = f'print("{test_str}")'

    if os.name == "nt":
        cmd = f"Set-Content -Path {script_name} -Value '{script_contents}'"
    else:
        cmd = f"echo '{script_contents}' > {script_name}"

    act_1 = hf.Action(
        commands=[hf.Command(cmd)],
    )
    act_2 = hf.Action(
        script="my_script.py",
        script_exe="python_script",
        script_data_in="direct",
        environments=[hf.ActionEnvironment(environment="python_env")],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[act_1, act_2],
    )

    t1 = hf.Task(schema=s1, inputs={"p1": 101})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="non_snippet_script_test",
        workflow_name="non_snippet_script_test",
        overwrite=True,
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text().strip()
    assert std_out.endswith(test_str)
