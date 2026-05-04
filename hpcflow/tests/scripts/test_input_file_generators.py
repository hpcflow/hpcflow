import os
import time

import pytest
from hpcflow.app import app as hf


@pytest.mark.integration
def test_input_file_generator_creates_file(tmp_path):

    hf.config._show(metadata=True)
    hf.print_envs()

    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")

    if os.name == "nt":
        cmd = "Get-Content <<file:my_input_file>>"
    else:
        cmd = "cat <<file:my_input_file>>"

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[hf.Command(cmd)],
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=inp_file,
                        inputs=[hf.Parameter("p1")],
                        script="<<script:input_file_generator_basic.py>>",
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="input_file_generator_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False)

    # check the input file is written
    run_0 = wk.get_all_EARs()[0]
    exec_path = run_0.get_directory()
    inp_file_path = exec_path.joinpath(inp_file.name.name)
    inp_file_contents = inp_file_path.read_text()
    assert inp_file_contents.strip() == str(p1_val)

    # check the command successfully printed the file contents to stdout:
    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text()
    assert std_out.strip() == str(p1_val)


@pytest.mark.integration
def test_IFG_std_stream_redirect_on_exception(tmp_path, reload_template_components):
    """Test exceptions raised by the app during execution of a IFG script are printed to the
    std-stream redirect file (and not the jobscript's standard error file)."""

    # define a custom python environment which redefines the `WK_PATH` shell variable to
    # a nonsense value so the app cannot load the workflow and thus raises an exception
    app_caps = hf.package_name.upper()
    if os.name == "nt":
        env_cmd = f'$env:{app_caps}_WK_PATH = "nonsense_path"'
    else:
        env_cmd = f'export {app_caps}_WK_PATH="nonsense_path"'

    bad_env = hf.envs.python_env.copy(name="bad_python_env")
    inst = bad_env.executables[0].instances[0]
    inst.command = env_cmd + "; " + inst.command
    hf.envs.add_object(bad_env, skip_duplicates=True)

    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=inp_file,
                        inputs=[hf.Parameter("p1")],
                        script="<<script:input_file_generator_basic.py>>",
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="bad_python_env")],
            )
        ],
    )

    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="input_file_generator_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    # jobscript stderr should be empty
    assert not wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()

    # std stream file has workflow not found traceback
    run = wk.get_all_EARs()[0]
    std_stream_path = run.get_app_std_path()
    assert std_stream_path.is_file()
    assert "WorkflowNotFoundError" in std_stream_path.read_text()


@pytest.mark.integration
def test_IFG_std_out_std_err_not_redirected(tmp_path):
    """Test that standard error and output streams from an IFG script are written to the jobscript
    standard error and output files."""
    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=inp_file,
                        inputs=[hf.Parameter("p1")],
                        script="<<script:input_file_generator_test_stdout_stderr.py>>",
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    stdout_msg = str(p1_val)
    stderr_msg = str(p1_val)
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="input_file_generator_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    if wk.submissions[0].jobscripts[0].resources.combine_jobscript_std:
        std_out_err = wk.submissions[0].jobscripts[0].direct_std_out_err_path.read_text()
        assert std_out_err.strip() == f"{stdout_msg}\n{stderr_msg}"
    else:
        std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text()
        std_err = wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()
        assert std_out.strip() == stdout_msg
        assert std_err.strip() == stderr_msg


@pytest.mark.integration
def test_IFG_pass_env_spec(tmp_path):
    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")

    if os.name == "nt":
        cmd = "Get-Content <<file:my_input_file>>"
    else:
        cmd = "cat <<file:my_input_file>>"

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[hf.Command(cmd)],
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=inp_file,
                        inputs=[hf.Parameter("p1")],
                        script="<<script:env_specifier_test/input_file_generator_pass_env_spec.py>>",
                        script_pass_env_spec=True,
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="input_file_generator_pass_env_spec",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    # check the command successfully printed the env spec and file contents to stdout:
    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text()
    assert std_out.strip() == f"{{'name': 'python_env'}}\n{str(p1_val)}"


@pytest.mark.integration
def test_env_specifier_in_input_file_generator_script_path(
    tmp_path, reload_template_components
):

    py_env = hf.envs.python_env.copy(specifiers={"version": "v1"})
    hf.envs.add_object(py_env, skip_duplicates=True)

    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")

    if os.name == "nt":
        cmd = "Get-Content <<file:my_input_file>>"
    else:
        cmd = "cat <<file:my_input_file>>"

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[hf.Command(cmd)],
                input_file_generators=[
                    hf.InputFileGenerator(
                        input_file=inp_file,
                        inputs=[hf.Parameter("p1")],
                        script="<<script:env_specifier_test/<<env:version>>/input_file_generator_basic.py>>",
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": p1_val},
        environments={"python_env": {"version": "v1"}},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="input_file_generator_test_env_specifier",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    # check the input file is written
    run_0 = wk.get_all_EARs()[0]
    exec_path = run_0.get_directory()
    inp_file_path = exec_path.joinpath(inp_file.name.name)
    inp_file_contents = inp_file_path.read_text()
    assert inp_file_contents.strip() == str(p1_val)

    # check the command successfully printed the file contents to stdout:
    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text()
    assert std_out.strip() == str(p1_val)
