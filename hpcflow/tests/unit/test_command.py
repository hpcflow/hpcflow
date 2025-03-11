from __future__ import annotations
from pathlib import Path
import numpy as np
import pytest
from hpcflow.app import app as hf
from hpcflow.sdk.submission.shells import ALL_SHELLS
from hpcflow.sdk.core.test_utils import (
    P1_parameter_cls as P1,
    P1_sub_parameter_cls as P1_sub,
)


def test_get_command_line(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = 1
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value} + 100)"


@pytest.mark.parametrize("shell_args", [("powershell", "nt"), ("bash", "posix")])
def test_get_command_line_with_stdout(
    null_config, tmp_path: Path, shell_args: tuple[str, str]
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1>> + 100)",
                        stdout="<<parameter:p2>>",
                    ),
                ],
            )
        ],
    )
    p1_value = 1
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS[shell_args[0]][shell_args[1]]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    if shell_args == ("powershell", "nt"):
        assert cmd_str == f"$parameter_p2 = Write-Output ({p1_value} + 100)"

    elif shell_args == ("bash", "posix"):
        assert cmd_str == f"parameter_p2=`Write-Output ({p1_value} + 100)`"


def test_get_command_line_single_labelled_input(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"), labels={"one": {}})],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1[one]>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = 1
    tasks = [
        hf.Task(schema=s1, inputs=[hf.InputValue("p1", label="one", value=p1_value)])
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value} + 100)"


def test_get_command_line_multiple_labelled_input(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"), multiple=True, labels={"one": {}, "two": {}}
            )
        ],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1[one]>> + <<parameter:p1[two]>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_one_value = 1
    p1_two_value = 2
    tasks = [
        hf.Task(
            schema=s1,
            inputs=[
                hf.InputValue("p1", label="one", value=p1_one_value),
                hf.InputValue("p1", label="two", value=p1_two_value),
            ],
        ),
    ]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_one_value} + {p1_two_value} + 100)"


def test_get_command_line_sub_parameter(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1.a>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = {"a": 1}
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value['a']} + 100)"


def test_get_command_line_sum(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command="Write-Output (<<sum(parameter:p1)>> + 100)"),
                ],
            )
        ],
    )
    p1_value = [1, 2, 3]
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({sum(p1_value)} + 100)"


def test_get_command_line_join(null_config, tmp_path: Path):
    delim = ","
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command=f'Write-Output (<<join[delim="{delim}"](parameter:p1)>> + 100)'
                    ),
                ],
            )
        ],
    )
    p1_value = [1, 2, 3]
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({delim.join(str(i) for i in p1_value)} + 100)"


def test_get_command_line_sum_sub_data(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command="Write-Output (<<sum(parameter:p1.a)>> + 100)"),
                ],
            )
        ],
    )
    p1_value = {"a": [1, 2, 3]}
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({sum(p1_value['a'])} + 100)"


def test_get_command_line_join_sub_data(null_config, tmp_path):
    delim = ","
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command=f'Write-Output (<<join[delim="{delim}"](parameter:p1.a)>> + 100)'
                    ),
                ],
            )
        ],
    )
    p1_value = {"a": [1, 2, 3]}
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({delim.join(str(i) for i in p1_value['a'])} + 100)"


def test_get_command_line_parameter_value(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = P1(a=1)  # has a `CLI_format` method defined which returns `str(a)`
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a} + 100)"


def test_get_command_line_parameter_value_join(null_config, tmp_path: Path):
    delim = ","
    cmd = (
        f"Write-Output "
        f'<<join[delim="{delim}"](parameter:p1c.custom_CLI_format_prep(reps=4))>>'
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command=cmd),
                ],
            )
        ],
    )
    p1_value = P1(a=4)
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
    )
    t1 = wk.tasks.t1
    assert isinstance(t1, hf.WorkflowTask)
    run = t1.elements[0].iterations[0].action_runs[0]
    command = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = command.get_command_line(EAR=run, shell=shell, env=run.get_environment())
    assert cmd_str == f"Write-Output 4,4,4,4"


def test_get_command_line_parameter_value_custom_method(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1c.custom_CLI_format()>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = P1(a=1)
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    run = wk.tasks.t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a + 4} + 100)"


def test_get_command_line_parameter_value_custom_method_with_args(
    null_config, tmp_path: Path
):
    add_val = 35
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command=f"Write-Output (<<parameter:p1c.custom_CLI_format(add={add_val})>> + 100)",
                    ),
                ],
            )
        ],
    )
    p1_value = P1(a=1)
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    t1 = wk.tasks.t1
    assert isinstance(t1, hf.WorkflowTask)
    run = t1.elements[0].iterations[0].action_runs[0]
    cmd = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = cmd.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a + add_val} + 100)"


def test_get_command_line_parameter_value_custom_method_with_two_args(
    null_config, tmp_path: Path
):
    add_val = 35
    sub_val = 10
    cmd = (
        f"Write-Output ("
        f"<<parameter:p1c.custom_CLI_format(add={add_val}, sub={sub_val})>> + 100)"
    )
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[hf.Action(commands=[hf.Command(command=cmd)])],
    )
    p1_value = P1(a=1)
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    t1 = wk.tasks.t1
    assert isinstance(t1, hf.WorkflowTask)
    run = t1.elements[0].iterations[0].action_runs[0]
    command = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = command.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert cmd_str == f"Write-Output ({p1_value.a + add_val - sub_val} + 100)"


def test_get_command_line_parameter_value_sub_object(null_config, tmp_path: Path):
    cmd = f"Write-Output (<<parameter:p1c.sub_param>> + 100)"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[hf.Action(commands=[hf.Command(command=cmd)])],
    )
    p1_value = P1(a=1, sub_param=P1_sub(e=5))
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    t1 = wk.tasks.t1
    assert isinstance(t1, hf.WorkflowTask)
    run = t1.elements[0].iterations[0].action_runs[0]
    command = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = command.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert p1_value.sub_param is not None
    assert cmd_str == f"Write-Output ({p1_value.sub_param.e} + 100)"


def test_get_command_line_parameter_value_sub_object_attr(null_config, tmp_path: Path):
    cmd = f"Write-Output (" f"<<parameter:p1c.sub_param.e>> + 100)"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        actions=[hf.Action(commands=[hf.Command(command=cmd)])],
    )
    p1_value = P1(a=1, sub_param=P1_sub(e=5))
    tasks = [hf.Task(schema=s1, inputs=[hf.InputValue("p1c", value=p1_value)])]
    wk = hf.Workflow.from_template_data(
        tasks=tasks,
        path=tmp_path,
        template_name="wk0",
        overwrite=True,
    )
    t1 = wk.tasks.t1
    assert isinstance(t1, hf.WorkflowTask)
    run = t1.elements[0].iterations[0].action_runs[0]
    command = run.action.commands[0]
    shell = ALL_SHELLS["powershell"]["nt"]()
    cmd_str, _ = command.get_command_line(EAR=run, shell=shell, env=run.get_environment())

    assert p1_value.sub_param is not None
    assert cmd_str == f"Write-Output ({p1_value.sub_param.e} + 100)"


def test_process_std_stream_int(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<int(parameter:p2)>>")
    assert cmd.process_std_stream(name="p2", value="101", stderr=False) == 101


def test_process_std_stream_stderr_int(null_config) -> None:
    cmd = hf.Command(command="", stderr="<<int(parameter:p2)>>")
    assert cmd.process_std_stream(name="p2", value="101", stderr=True) == 101


def test_process_std_stream_float(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<float(parameter:p2)>>")
    assert cmd.process_std_stream(name="p2", value="3.1415", stderr=False) == 3.1415


def test_process_std_stream_bool_true(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<bool(parameter:p2)>>")
    for value in ("true", "True", "1"):
        assert cmd.process_std_stream(name="p2", value=value, stderr=False) == True


def test_process_std_stream_bool_false(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<bool(parameter:p2)>>")
    for value in ("false", "False", "0"):
        assert cmd.process_std_stream(name="p2", value=value, stderr=False) == False


def test_process_std_stream_bool_raise(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<bool(parameter:p2)>>")
    for value in ("hi", "120", "-1"):
        with pytest.raises(ValueError):
            cmd.process_std_stream(name="p2", value=value, stderr=False)


def test_process_std_stream_list(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<list(parameter:p2)>>")
    assert cmd.process_std_stream(name="p2", value="1 2 3", stderr=False) == [
        "1",
        "2",
        "3",
    ]


def test_process_std_stream_list_int(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<list[item_type=int](parameter:p2)>>")
    assert cmd.process_std_stream(name="p2", value="1 2 3", stderr=False) == [1, 2, 3]


def test_process_std_stream_list_delim(null_config) -> None:
    cmd = hf.Command(command="", stdout='<<list[delim=","](parameter:p2)>>')
    assert cmd.process_std_stream(name="p2", value="1,2,3", stderr=False) == [
        "1",
        "2",
        "3",
    ]


def test_process_std_stream_list_int_delim(null_config) -> None:
    cmd = hf.Command(
        command="", stdout='<<list[item_type=int, delim=","](parameter:p2)>>'
    )
    assert cmd.process_std_stream(name="p2", value="1,2,3", stderr=False) == [1, 2, 3]


def test_process_std_stream_list_float_delim_colon(null_config) -> None:
    cmd = hf.Command(
        command="", stdout='<<list[item_type=float, delim=":"](parameter:p2)>>'
    )
    assert cmd.process_std_stream(name="p2", value="1.1:2.2:3.3", stderr=False) == [
        1.1,
        2.2,
        3.3,
    ]


def test_process_std_stream_array(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<array(parameter:p2)>>")
    assert np.allclose(
        cmd.process_std_stream(name="p2", value="1 2 3", stderr=False),
        np.array([1, 2, 3]),
    )


def test_process_std_stream_array_delim(null_config) -> None:
    cmd = hf.Command(command="", stdout='<<array[delim=","](parameter:p2)>>')
    assert np.allclose(
        cmd.process_std_stream(name="p2", value="1,2,3", stderr=False),
        np.array([1, 2, 3]),
    )


def test_process_std_stream_array_dtype_int(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<array[item_type=int](parameter:p2)>>")
    arr = cmd.process_std_stream(name="p2", value="1 2 3", stderr=False)
    assert arr.dtype == np.dtype("int")


def test_process_std_stream_array_dtype_float(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<array[item_type=float](parameter:p2)>>")
    arr = cmd.process_std_stream(name="p2", value="1 2 3", stderr=False)
    assert arr.dtype == np.dtype("float")


def test_process_std_stream_object(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<parameter:p1c>>")
    a_val = 12
    assert cmd.process_std_stream(name="p1c", value=str(a_val), stderr=False) == P1(
        a=a_val
    )


def test_process_std_stream_object_kwargs(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<parameter:p1c.CLI_parse(double=true)>>")
    a_val = 12
    expected = 2 * a_val
    assert cmd.process_std_stream(name="p1c", value=str(a_val), stderr=False) == P1(
        a=expected
    )


def test_get_output_types(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<parameter:p1_test_123>>")
    assert cmd.get_output_types() == {"stdout": "p1_test_123", "stderr": None}


def test_get_output_types_int(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<int(parameter:p1_test_123)>>")
    assert cmd.get_output_types() == {"stdout": "p1_test_123", "stderr": None}


def test_get_output_types_object_with_args(null_config) -> None:
    cmd = hf.Command(
        command="", stdout="<<parameter:p1_test_123.CLI_parse(double=true)>>"
    )
    assert cmd.get_output_types() == {"stdout": "p1_test_123", "stderr": None}


def test_get_output_types_list(null_config) -> None:
    cmd = hf.Command(
        command="", stdout="<<list[item_type=int, delim=" "](parameter:p1_test_123)>>"
    )
    assert cmd.get_output_types() == {"stdout": "p1_test_123", "stderr": None}


def test_get_output_types_no_match(null_config) -> None:
    cmd = hf.Command(command="", stdout="parameter:p1_test_123")
    assert cmd.get_output_types() == {"stdout": None, "stderr": None}


def test_get_output_types_raise_with_extra_substring_start(null_config) -> None:
    cmd = hf.Command(command="", stdout="hello: <<parameter:p1_test_123>>")
    with pytest.raises(ValueError):
        cmd.get_output_types()


def test_get_output_types_raise_with_extra_substring_end(null_config) -> None:
    cmd = hf.Command(command="", stdout="<<parameter:p1_test_123>> hello")
    with pytest.raises(ValueError):
        cmd.get_output_types()


def test_extract_executable_labels(null_config) -> None:
    tests = {
        "<<executable:m1>> and <<executable:12>>": ["m1", "12"],
        "<<executable:m1>> hi": ["m1"],
        "<<executable:m1": [],
    }
    for k, v in tests.items():
        assert hf.Command._extract_executable_labels(k) == v
