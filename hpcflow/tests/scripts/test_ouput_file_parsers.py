import os
import time

import pytest
from hpcflow.app import app as hf


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_output_file_parser_parses_file(null_config, tmp_path):
    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                script="<<script:output_file_parser_basic.py>>",
            ),
        ],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )

    p1_val = 101
    p2_val_expected = p1_val + 100
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_test",
        path=tmp_path,
    )

    wk.submit(wait=True, add_to_known=False)

    # check the command successfully generated the output file:
    run_0 = wk.get_all_EARs()[0]
    exec_path = run_0.get_directory()
    out_file_path = exec_path.joinpath(out_file.name.name)
    out_file_contents = out_file_path.read_text()
    assert out_file_contents.strip() == str(p2_val_expected)

    # check the output is parsed correctly:
    assert wk.tasks[0].elements[0].outputs.p2.value == p2_val_expected


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_OFP_std_stream_redirect_on_exception(new_null_config, tmp_path):
    """Test exceptions raised by the app during execution of an OFP script are printed to the
    std-stream redirect file (and not the jobscript's standard error file)."""

    # define a custom python environment which redefines the `WK_PATH` shell variable to
    # a nonsense value so the app cannot load the workflow and thus raises an exception

    app_caps = hf.package_name.upper()
    if os.name == "nt":
        env_cmd = f'$env:{app_caps}_WK_PATH = "nonsense_path"'
    else:
        env_cmd = f'export {app_caps}_WK_PATH="nonsense_path"'

    env_cmd += "; python <<script_path>> <<args>>"
    bad_env = hf.Environment(
        name="bad_python_env",
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command=env_cmd,
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(bad_env, skip_duplicates=True)

    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                script="<<script:output_file_parser_basic.py>>",
            ),
        ],
        environments=[hf.ActionEnvironment(environment="bad_python_env")],
    )

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )

    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    # jobscript stderr should be empty
    assert not wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()

    # std stream file has workflow not found traceback
    run = wk.get_all_EARs()[1]
    std_stream_path = run.get_app_std_path()
    assert std_stream_path.is_file()
    assert "WorkflowNotFoundError" in std_stream_path.read_text()

    hf.reload_template_components()  # remove extra envs


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_OFP_std_out_std_err_not_redirected(null_config, tmp_path):
    """Test that standard error and output streams from an OFP script are written to the jobscript
    standard error and output files."""
    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                inputs=["p1"],
                script="<<script:output_file_parser_test_stdout_stderr.py>>",
            ),
        ],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )
    p1_val = 101
    stdout_msg = str(p1_val)
    stderr_msg = str(p1_val)
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="ouput_file_parser_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False)

    if wk.submissions[0].jobscripts[0].resources.combine_jobscript_std:
        std_out_err = wk.submissions[0].jobscripts[0].direct_std_out_err_path.read_text()
        assert std_out_err.strip() == f"{stdout_msg}\n{stderr_msg}"
    else:
        std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text()
        std_err = wk.submissions[0].jobscripts[0].direct_stderr_path.read_text()
        assert std_out.strip() == stdout_msg
        assert std_err.strip() == stderr_msg


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_output_file_parser_pass_env_spec(null_config, tmp_path):
    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                script="<<script:env_specifier_test/output_file_parser_pass_env_spec.py>>",
                script_pass_env_spec=True,
            ),
        ],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )

    t1 = hf.Task(schema=s1, inputs={"p1": 101})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_pass_env_spec",
        path=tmp_path,
    )

    wk.submit(wait=True, add_to_known=False, status=False)

    std_out = wk.submissions[0].jobscripts[0].direct_stdout_path.read_text().strip()
    assert std_out == "{'name': 'python_env'}"


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_env_specifier_in_output_file_parser_script_path(new_null_config, tmp_path):

    py_env = hf.Environment(
        name="python_env",
        specifiers={"version": "v1"},
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command="python <<script_path>> <<args>>",
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(py_env, skip_duplicates=True)

    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                script="<<script:env_specifier_test/<<env:version>>/output_file_parser_basic.py>>",
            ),
        ],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
    )

    p1_val = 101
    p2_val_expected = p1_val + 100
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": p1_val},
        environments={"python_env": {"version": "v1"}},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_test_env_specifier",
        path=tmp_path,
    )

    wk.submit(wait=True, add_to_known=False, status=False)

    # check the command successfully generated the output file:
    run_0 = wk.get_all_EARs()[0]
    exec_path = run_0.get_directory()
    out_file_path = exec_path.joinpath(out_file.name.name)
    out_file_contents = out_file_path.read_text()
    assert out_file_contents.strip() == str(p2_val_expected)

    # check the output is parsed correctly:
    assert wk.tasks[0].elements[0].outputs.p2.value == p2_val_expected

    hf.reload_template_components()  # remove extra envs


@pytest.mark.integration
def test_no_script_no_output_saves_files(null_config, tmp_path):
    """Check we can use an output file parser with no script or output to save files."""
    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[hf.OutputFileParser(output_files=[out_file])],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[act],
    )

    p1_val = 101
    p2_val_expected = p1_val + 100
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_test_no_output_no_script",
        path=tmp_path,
    )

    wk.submit(wait=True, add_to_known=False, status=False)

    # check the output file is saved to artifacts:
    run_0 = wk.get_all_EARs()[0]
    exec_path = run_0.get_directory()
    out_file_path = exec_path.joinpath(out_file.name.name)
    out_file_contents = out_file_path.read_text()
    assert out_file_contents.strip() == str(p2_val_expected)

    # check no scripts generated
    assert not any(wk.submissions[0].scripts_path.iterdir())
