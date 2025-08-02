import os
import pytest

import hpcflow.app as hf


@pytest.mark.integration
def test_builtin_program_no_args_resource_var(new_null_config, tmp_path):
    # run a builtin program
    env_cmd = ("& " if os.name == "nt" else "") + "<<program_path>> <<args>>"
    env = hf.Environment(
        name="program_env",
        executables=[
            hf.Executable(
                label="hello_world",
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
    hf.envs.add_object(env, skip_duplicates=True)

    act = hf.Action(
        program="hello_world/<<resource:platform>>/hello_world<<resource:executable_extension>>",
        program_exe="hello_world",
        environments=[hf.ActionEnvironment("program_env")],
    )
    s1 = hf.TaskSchema(objective="hello", actions=[act])
    tasks = [hf.Task(s1)]
    wk = hf.Workflow.from_template_data(
        template_name="test_program_no_args",
        tasks=tasks,
        path=tmp_path,
    )
    wk.submit(wait=True, status=False)
    assert wk.submissions[0].jobscripts[0].get_stdout().strip() == "hello, world"

    hf.reload_template_components()  # remove extra env


@pytest.mark.integration
def test_builtin_program_no_args_env_var(new_null_config, tmp_path):
    # run a builtin program
    env_cmd = ("& " if os.name == "nt" else "") + "<<program_path>> <<args>>"
    default_platform = hf.ElementResources.get_default_platform()
    env = hf.Environment(
        name="program_env",
        specifiers={
            "platform": default_platform
        },  # reference this specifier in the program path
        executables=[
            hf.Executable(
                label="hello_world",
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
    hf.envs.add_object(env, skip_duplicates=True)

    act = hf.Action(
        program="hello_world/<<env:platform>>/hello_world<<resource:executable_extension>>",
        program_exe="hello_world",
        environments=[hf.ActionEnvironment("program_env")],
    )
    s1 = hf.TaskSchema(objective="hello", actions=[act])
    tasks = [hf.Task(s1)]
    wk = hf.Workflow.from_template_data(
        template_name="test_program_no_args",
        environments={"program_env": {"platform": default_platform}},
        tasks=tasks,
        path=tmp_path,
    )
    wk.submit(wait=True, status=False)
    assert wk.submissions[0].jobscripts[0].get_stdout().strip() == "hello, world"

    hf.reload_template_components()  # remove extra env


@pytest.mark.integration
def test_builtin_program_no_args_param_var(new_null_config, tmp_path):
    # run a builtin program
    env_cmd = ("& " if os.name == "nt" else "") + "<<program_path>> <<args>>"
    default_platform = hf.ElementResources.get_default_platform()
    env = hf.Environment(
        name="program_env",
        executables=[
            hf.Executable(
                label="hello_world",
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
    hf.envs.add_object(env, skip_duplicates=True)

    act = hf.Action(
        program="hello_world/<<parameter:platform>>/hello_world<<resource:executable_extension>>",
        program_exe="hello_world",
        environments=[hf.ActionEnvironment("program_env")],
    )
    s1 = hf.TaskSchema(
        objective="hello",
        actions=[act],
        inputs=[hf.SchemaInput("platform")],
    )
    tasks = [hf.Task(s1, inputs={"platform": default_platform})]
    wk = hf.Workflow.from_template_data(
        template_name="test_program_no_args",
        tasks=tasks,
        path=tmp_path,
    )
    wk.submit(wait=True, status=False)
    assert wk.submissions[0].jobscripts[0].get_stdout().strip() == "hello, world"

    hf.reload_template_components()  # remove extra env


@pytest.mark.integration
def test_builtin_program_input_output_JSON_resource_var(new_null_config, tmp_path):
    # run a builtin program that expects input and output JSON file paths as a cmdline arguments
    env_cmd = ("& " if os.name == "nt" else "") + "<<program_path>> <<args>>"
    env = hf.Environment(
        name="program_env",
        executables=[
            hf.Executable(
                label="hello_world",
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
    hf.envs.add_object(env, skip_duplicates=True)

    act = hf.Action(
        program="hello_world/<<resource:platform>>/hello_world<<resource:executable_extension>>",
        program_exe="hello_world",
        program_data_in="json",
        program_data_out="json",
        requires_dir=True,
        environments=[hf.ActionEnvironment("program_env")],
    )
    s1 = hf.TaskSchema(
        objective="hello",
        inputs=[hf.SchemaInput("p1"), hf.SchemaInput("p2"), hf.SchemaInput("p3")],
        outputs=[hf.SchemaInput("p4")],
        actions=[act],
    )
    p1, p2, p3 = 1, 2, 3
    tasks = [hf.Task(s1, inputs={"p1": p1, "p2": p2, "p3": p3})]
    wk = hf.Workflow.from_template_data(
        template_name="test_program_input_output_files",
        tasks=tasks,
        path=tmp_path,
    )
    wk.submit(wait=True, status=False)
    assert wk.submissions[0].jobscripts[0].get_stdout().strip() == "hello, world"
    assert wk.tasks[0].elements[0].get("outputs.p4") == p1 + p2 + p3

    hf.reload_template_components()  # remove extra env
