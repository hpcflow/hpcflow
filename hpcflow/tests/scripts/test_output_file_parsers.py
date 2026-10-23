import os
from pathlib import Path
import time

import pytest
from hpcflow.app import app as hf


@pytest.mark.integration
@pytest.mark.parametrize(
    ("schema_save_files", "saved"),
    [
        (None, False),
        (["my_output_file"], True),
    ],
)
def test_output_file_parser_parses_file(tmp_path, schema_save_files, saved):
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
        save_files=schema_save_files,
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

    # Check the parser produced its output and retained its source only as configured.
    run_0 = wk.get_all_EARs()[0]
    output_file_param_id = run_0.data_idx["output_files.my_output_file"]
    assert wk.is_parameter_set(output_file_param_id) is saved
    artifact_paths = list(wk.task_artifacts_path.rglob(out_file_name))
    assert bool(artifact_paths) is saved
    if saved:
        out_file_path = Path(run_0.get("output_files.my_output_file"))
        out_file_contents = out_file_path.read_text()
        assert out_file_contents.strip() == str(p2_val_expected)

    # check the output is parsed correctly:
    assert wk.tasks[0].elements[0].outputs.p2.value == p2_val_expected


@pytest.mark.integration
def test_OFP_std_stream_redirect_on_exception(tmp_path, reload_template_components):
    """Test exceptions raised by the app during execution of an OFP script are printed to the
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


@pytest.mark.integration
def test_OFP_std_out_std_err_not_redirected(tmp_path):
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
def test_output_file_parser_pass_env_spec(tmp_path):
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
def test_env_specifier_in_output_file_parser_script_path(
    tmp_path, reload_template_components
):

    py_env = hf.envs.python_env.copy(specifiers={"version": "v1"})
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
        save_files=["my_output_file"],
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

    # Check the output file was saved before execution cleanup.
    run_0 = wk.get_all_EARs()[0]
    out_file_path = Path(run_0.get("output_files.my_output_file"))
    out_file_contents = out_file_path.read_text()
    assert out_file_contents.strip() == str(p2_val_expected)

    # check the output is parsed correctly:
    assert wk.tasks[0].elements[0].outputs.p2.value == p2_val_expected


@pytest.mark.integration
def test_no_script_no_output_saves_files(tmp_path):
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
        save_files=["my_output_file"],
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
    runs = wk.get_all_EARs()
    assert all(run.success for run in runs)
    out_file_path = Path(runs[-1].get("output_files.my_output_file"))
    out_file_contents = out_file_path.read_text()
    assert out_file_contents.strip() == str(p2_val_expected)
    assert not wk.execution_path.exists()

    # check no scripts generated
    assert not any(wk.submissions[0].scripts_path.iterdir())


@pytest.mark.integration
@pytest.mark.parametrize(
    ("schema_save_files", "expected_files"),
    [
        (None, set()),
        (["generated_file"], {"generated.txt"}),
        (True, {"my_output_file.txt", "generated.txt"}),
    ],
)
def test_output_file_parser_generates_files(tmp_path, schema_save_files, expected_files):
    """Files produced by a parser script are declared with `generates_files` and
    retained according to the task schema policy."""
    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)
    gen_file = hf.FileSpec(label="generated_file", name="generated.txt")

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
                script="<<script:output_file_parser_generates_file.py>>",
                generates_files=[gen_file],
            ),
        ],
        environments=[hf.ActionEnvironment(environment="python_env")],
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[act],
        save_files=schema_save_files,
    )
    p1_val = 101
    p2_val_expected = p1_val + 100
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="output_file_parser_generates_files",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_all_EARs()
    assert all(run.success for run in runs)
    assert wk.tasks[0].elements[0].outputs.p2.value == p2_val_expected

    saved = {path.name for path in wk.task_artifacts_path.rglob("*.txt")}
    assert saved == expected_files

    # the parser run has a parameter for the generated file:
    parser_run = runs[-1]
    gen_param_id = parser_run.data_idx["output_files.generated_file"]
    assert wk.is_parameter_set(gen_param_id) is ("generated.txt" in expected_files)
    if "generated.txt" in expected_files:
        gen_path = Path(parser_run.get("output_files.generated_file"))
        assert gen_path.read_text() == f"generated from {p2_val_expected}"
