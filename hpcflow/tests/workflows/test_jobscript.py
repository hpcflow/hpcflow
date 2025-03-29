import os
import sys
from pathlib import Path
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core import SKIPPED_EXIT_CODE
from hpcflow.sdk.core.skip_reason import SkipReason


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


@pytest.mark.integration
def test_bad_action_py_script_exit_code(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[
            hf.Action(
                script="<<script:bad_script.py>>",  # raises SyntaxError
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    t1 = hf.Task(schema=[s1])
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="bad_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    recorded_exit = wk.get_EARs_from_IDs([0])[0].exit_code
    assert recorded_exit == 1


@pytest.mark.integration
@pytest.mark.parametrize("exit_code", [0, 1, 98, -1, -123124])
def test_action_py_script_specified_exit_code(null_config, tmp_path, exit_code):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("exit_code")],
        actions=[
            hf.Action(
                script="<<script:script_exit_test.py>>",
                script_exe="python_script",
                script_data_in="direct",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    t1 = hf.Task(schema=[s1], inputs={"exit_code": exit_code})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="script_exit_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    recorded_exit = wk.get_EARs_from_IDs([0])[0].exit_code
    if os.name == "posix":
        # exit code from bash wraps around:
        exit_code %= 256
    assert recorded_exit == exit_code


@pytest.mark.integration
def test_skipped_action_same_element(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2"), hf.SchemaOutput("p3")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command=f"echo <<parameter:p1>>", stdout="<<parameter:p2>>"
                    ),
                    hf.Command(command=f"exit 1"),
                ],
            ),
            hf.Action(  # should be skipped
                commands=[
                    hf.Command(
                        command=f"echo <<parameter:p2>>", stdout="<<parameter:p3>>"
                    ),
                    hf.Command(command=f"exit 0"),  # exit code should be ignored
                ],
            ),
        ],
    )
    t1 = hf.Task(schema=s1, inputs={"p1": 101})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="test_skip", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_EARs_from_IDs([0, 1])
    exit_codes = [i.exit_code for i in runs]
    is_skipped = [i.skip for i in runs]

    assert exit_codes == [1, SKIPPED_EXIT_CODE]
    assert is_skipped == [0, 1]


@pytest.mark.integration
def test_two_skipped_actions_same_element(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2"), hf.SchemaOutput("p3"), hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command=f"echo <<parameter:p1>>", stdout="<<parameter:p2>>"
                    ),
                    hf.Command(command=f"exit 1"),
                ],
            ),
            hf.Action(  # should be skipped
                commands=[
                    hf.Command(
                        command=f"echo <<parameter:p2>>", stdout="<<parameter:p3>>"
                    ),
                    hf.Command(command=f"exit 0"),  # exit code should be ignored
                ],
            ),
            hf.Action(  # should be skipped
                commands=[
                    hf.Command(
                        command=f"echo <<parameter:p3>>", stdout="<<parameter:p4>>"
                    ),
                    hf.Command(command=f"exit 0"),  # exit code should be ignored
                ],
            ),
        ],
    )
    t1 = hf.Task(schema=s1, inputs={"p1": 101})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="test_skip_two_actions", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    runs = wk.get_EARs_from_IDs([0, 1, 2])
    exit_codes = [i.exit_code for i in runs]
    skip_reasons = [i.skip_reason for i in runs]

    assert exit_codes == [1, SKIPPED_EXIT_CODE, SKIPPED_EXIT_CODE]
    assert skip_reasons == [
        SkipReason.NOT_SKIPPED,
        SkipReason.UPSTREAM_FAILURE,
        SkipReason.UPSTREAM_FAILURE,
    ]


@pytest.mark.integration
@pytest.mark.skipif(
    condition=sys.platform == "win32",
    reason="`combine_jobscript_std` not implemented on Windows.",
)
def test_combine_jobscript_std_true(null_config, tmp_path):
    out_msg = "hello stdout!"
    err_msg = "hello stderr!"
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command=f'echo "{out_msg}"'),
                    hf.Command(command=f'>&2 echo "{err_msg}"'),
                ],
            )
        ],
    )
    t1 = hf.Task(schema=s1)
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_combine_jobscript_std",
        path=tmp_path,
        resources={"any": {"combine_jobscript_std": True}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    jobscript = wk.submissions[0].jobscripts[0]

    assert jobscript.resources.combine_jobscript_std

    out_err_path = jobscript.get_std_out_err_path()
    out_path = jobscript._get_stdout_path()
    err_path = jobscript._get_stderr_path()

    assert out_err_path.is_file()
    assert not out_path.is_file()
    assert not err_path.is_file()

    assert out_err_path.read_text().strip() == f"{out_msg}\n{err_msg}"


@pytest.mark.integration
def test_combine_jobscript_std_false(null_config, tmp_path):
    out_msg = "hello stdout!"
    err_msg = "hello stderr!"
    s1 = hf.TaskSchema(
        objective="t1",
        actions=[
            hf.Action(
                commands=[
                    hf.Command(command=f'echo "{out_msg}"'),
                    hf.Command(command=f'>&2 echo "{err_msg}"'),
                ],
                rules=[
                    hf.ActionRule(
                        rule=hf.Rule(
                            path="resources.os_name",
                            condition={"value.equal_to": "posix"},
                        )
                    )
                ],
            ),
            hf.Action(
                commands=[
                    hf.Command(command=f'Write-Output "{out_msg}"'),
                    hf.Command(command=f'$host.ui.WriteErrorLine("{err_msg}")'),
                ],
                rules=[
                    hf.ActionRule(
                        rule=hf.Rule(
                            path="resources.os_name",
                            condition={"value.equal_to": "nt"},
                        )
                    )
                ],
            ),
        ],
    )
    t1 = hf.Task(schema=s1)
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_combine_jobscript_std",
        path=tmp_path,
        resources={"any": {"combine_jobscript_std": False}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    jobscript = wk.submissions[0].jobscripts[0]

    assert not jobscript.resources.combine_jobscript_std

    out_err_path = jobscript.direct_std_out_err_path
    out_path = jobscript.direct_stdout_path
    err_path = jobscript.direct_stderr_path

    assert not out_err_path.is_file()
    assert out_path.is_file()
    assert err_path.is_file()

    assert out_path.read_text().strip() == out_msg
    assert err_path.read_text().strip() == err_msg


@pytest.mark.integration
def test_write_app_logs_true(null_config, tmp_path):

    p1_vals = [101, 102]
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        sequences=[hf.ValueSequence("inputs.p1", values=p1_vals)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_write_app_logs",
        path=tmp_path,
        config={
            "log_file_level": "debug"
        },  # ensure there is something to write to the log
        resources={"any": {"write_app_logs": True}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    run_0 = wk.tasks[0].elements[0].action_runs[0]
    run_1 = wk.tasks[0].elements[1].action_runs[0]

    run_0_log_path = run_0.get_app_log_path()
    run_1_log_path = run_1.get_app_log_path()

    assert run_0_log_path.is_file()
    assert run_1_log_path.is_file()


@pytest.mark.integration
def test_write_app_logs_false(null_config, tmp_path):

    p1_vals = [101, 102]
    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        sequences=[hf.ValueSequence("inputs.p1", values=p1_vals)],
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="test_write_app_logs",
        path=tmp_path,
        config={
            "log_file_level": "debug"
        },  # ensure there is something to write to the log
        resources={"any": {"write_app_logs": False}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    run_0 = wk.tasks[0].elements[0].action_runs[0]
    run_1 = wk.tasks[0].elements[1].action_runs[0]

    run_0_log_path = run_0.get_app_log_path()
    run_1_log_path = run_1.get_app_log_path()

    assert not wk.submissions[0].app_log_path.is_dir()
    assert not run_0_log_path.is_file()
    assert not run_1_log_path.is_file()


@pytest.mark.integration
def test_jobscript_start_end_times_equal_to_first_and_last_run_start_end_times(
    null_config, tmp_path
):

    t1 = hf.Task(
        schema=hf.task_schemas.test_t1_conditional_OS,
        sequences=[hf.ValueSequence(path="inputs.p1", values=list(range(2)))],
    )
    wk = hf.Workflow.from_template_data(
        template_name="test_jobscript_start_end_times",
        path=tmp_path,
        tasks=[t1],
    )
    wk.submit(wait=True, add_to_known=False, status=False)

    js = wk.submissions[0].jobscripts[0]
    runs = wk.get_all_EARs()
    assert len(runs) == 2

    # jobsript has two runs, so start time should be start time of first run:
    assert js.start_time == runs[0].start_time

    # ...and end time should be end time of second run:
    assert js.end_time == runs[1].end_time
