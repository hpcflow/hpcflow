import os
from textwrap import dedent

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.actions import EARStatus


@pytest.mark.integration
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_run_status_fail_when_missing_script_output_data_file(
    null_config, tmp_path, combine_scripts
):

    s1 = hf.TaskSchema(
        objective="t1",
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_out_FAIL.py>>",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
                requires_dir=True,
            )
        ],
    )

    tasks = [
        hf.Task(s1),  # will fail due to not generaing an output data file
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_run_status_fail_missing_script_output_file",
        path=tmp_path,
        tasks=tasks,
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": False,
                "combine_scripts": combine_scripts,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert runs[0].status is EARStatus.error


@pytest.mark.integration
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_run_status_fail_when_missing_script_output_data_file_OFP_fail(
    null_config, tmp_path, combine_scripts
):

    out_file_name = "my_output_file.txt"
    out_file = hf.FileSpec(label="my_output_file", name=out_file_name)

    if os.name == "nt":
        cmd = f"Set-Content -Path {out_file_name} -Value (<<parameter:p1>> + 100)"
    else:
        cmd = f"echo $(( <<parameter:p1>> + 100 )) > {out_file_name}"

    # this script parses the output file but then deletes this file so it can't be saved!
    act = hf.Action(
        commands=[hf.Command(cmd)],
        output_file_parsers=[
            hf.OutputFileParser(
                output_files=[out_file],
                output=hf.Parameter("p2"),
                script="<<script:output_file_parser_basic_FAIL.py>>",
                save_files=True,
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
    t1 = hf.Task(schema=s1, inputs={"p1": 100})

    wk = hf.Workflow.from_template_data(
        template_name="test_run_status_fail_missing_OFP_save_file",
        path=tmp_path,
        tasks=[t1],
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": False,
                "combine_scripts": combine_scripts,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert runs[0].status is EARStatus.success
    assert runs[1].status is EARStatus.error


@pytest.mark.integration
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_run_status_fail_when_missing_IFG_input_file(
    null_config, tmp_path, combine_scripts
):

    inp_file = hf.FileSpec(label="my_input_file", name="my_input_file.txt")

    if os.name == "nt":
        cmd = dedent(
            """\
            try {
                Get-Content "<<file:my_input_file>>" -ErrorAction Stop
            } catch {
                Write-Host "File does not exist."
                exit 1 
            }
        """
        )
    else:
        cmd = "cat <<file:my_input_file>>"

    # this script silently fails to generate the input file!
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
                        script="<<script:input_file_generator_basic_FAIL.py>>",
                    ),
                ],
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    t1 = hf.Task(schema=s1, inputs={"p1": 100})
    wk = hf.Workflow.from_template_data(
        template_name="test_run_status_fail_missing_IFG_save_file",
        path=tmp_path,
        tasks=[t1],
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": False,
                "combine_scripts": combine_scripts,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert runs[0].status is EARStatus.error  # no input file to save
    assert runs[1].status is EARStatus.error  # no input file to consume


@pytest.mark.integration
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_run_status_fail_when_action_save_file(null_config, tmp_path, combine_scripts):

    my_file = hf.FileSpec(label="my_file", name="my_file.txt")

    # this script does not generate a file that can be saved:
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[
            hf.Action(
                script="<<script:do_nothing.py>>",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
                requires_dir=True,
                save_files=[my_file],
            )
        ],
    )
    t1 = hf.Task(schema=s1)
    wk = hf.Workflow.from_template_data(
        template_name="test_run_status_fail_missing_action_save_file",
        path=tmp_path,
        tasks=[t1],
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": False,
                "combine_scripts": combine_scripts,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert runs[0].status is EARStatus.error  # no file to save
