import os
from hpcflow.sdk.core.test_utils import make_schemas
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.actions import EARStatus


@pytest.mark.integration
def test_skip_downstream_on_failure_true_combine_scripts(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_2.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p3"), group="my_group")],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p4"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_group_direct_out_3.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    tasks = [
        hf.Task(
            s1,
            sequences=[
                hf.ValueSequence(path="inputs.p1", values=[101, "NONSENSE VALUE"])
            ],
        ),
        hf.Task(s2, groups=[hf.ElementGroup(name="my_group")]),
        hf.Task(s3),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_skip_downstream_on_failure",
        path=tmp_path,
        tasks=tasks,
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": True,
                "combine_scripts": True,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()

    assert runs[0].status is EARStatus.success
    assert runs[1].status is EARStatus.error  # original error
    assert runs[2].status is EARStatus.success
    assert runs[3].status is EARStatus.skipped  # skipped due to run 1 error
    assert runs[4].status is EARStatus.skipped  # skipped due to run 3 skipped


@pytest.mark.integration
def test_skip_downstream_on_failure_false_combine_scripts(null_config, tmp_path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p2"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_2.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    s3 = hf.TaskSchema(
        objective="t3",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p3"), group="my_group")],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p4"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_group_direct_out_3.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    tasks = [
        hf.Task(
            s1,
            sequences=[
                hf.ValueSequence(path="inputs.p1", values=[101, "NONSENSE VALUE"])
            ],
        ),
        hf.Task(s2, groups=[hf.ElementGroup(name="my_group")]),
        hf.Task(s3),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_skip_downstream_on_failure",
        path=tmp_path,
        tasks=tasks,
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": False,
                "combine_scripts": True,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()

    assert runs[0].status is EARStatus.success
    assert runs[1].status is EARStatus.error  # original error
    assert runs[2].status is EARStatus.success
    assert runs[3].status is EARStatus.error  # relies on run 1 output so fails
    assert runs[4].status is EARStatus.error  # relies on run 3 output so fails


@pytest.mark.integration
def test_skip_downstream_on_failure_true(null_config, tmp_path):
    s1, s2 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p3",), "t2"),
    )
    s3 = hf.TaskSchema(
        "t3",
        inputs=[hf.SchemaInput("p3", group="my_group")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<sum(parameter:p3)>> ))",
                        stdout="<<int(parameter:p4)>>",
                    )
                ]
            )
        ],
    )

    tasks = [
        hf.Task(
            s1,
            sequences=[
                hf.ValueSequence(path="inputs.p1", values=[101, "NONSENSE VALUE"])
            ],
        ),
        hf.Task(s2, groups=[hf.ElementGroup(name="my_group")]),
        hf.Task(s3),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_skip_downstream_on_failure",
        path=tmp_path,
        tasks=tasks,
        resources={"any": {"write_app_logs": True, "skip_downstream_on_failure": True}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()

    assert runs[0].status is EARStatus.success
    assert runs[1].status is EARStatus.error  # original error
    assert runs[2].status is EARStatus.success
    assert runs[3].status is EARStatus.skipped  # skipped due to run 1 error
    assert runs[4].status is EARStatus.skipped  # skipped due to run 3 skipped


@pytest.mark.integration
def test_skip_downstream_on_failure_false(null_config, tmp_path):
    s1, s2 = make_schemas(
        ({"p1": None}, ("p2",), "t1"),
        ({"p2": None}, ("p3",), "t2"),
    )
    s3 = hf.TaskSchema(
        "t3",
        inputs=[hf.SchemaInput("p3", group="my_group")],
        outputs=[hf.SchemaOutput("p4")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        "echo $(( <<sum(parameter:p3)>> ))",
                        stdout="<<int(parameter:p4)>>",
                    )
                ]
            )
        ],
    )

    tasks = [
        hf.Task(
            s1,
            sequences=[
                hf.ValueSequence(path="inputs.p1", values=[101, "NONSENSE VALUE"])
            ],
        ),
        hf.Task(s2, groups=[hf.ElementGroup(name="my_group")]),
        hf.Task(s3),
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_skip_downstream_on_failure",
        path=tmp_path,
        tasks=tasks,
        resources={"any": {"write_app_logs": True, "skip_downstream_on_failure": False}},
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()

    assert runs[0].status is EARStatus.success
    assert runs[1].status is EARStatus.error  # original error
    assert runs[2].status is EARStatus.success
    assert runs[3].status is EARStatus.error  # relies on run 1 output so fails
    assert runs[4].status is EARStatus.error  # relies on run 3 output so fails


@pytest.mark.integration
@pytest.mark.parametrize("allow_failed_dependencies", ["UNSET", None, False, 0.0, 0])
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_skip_downstream_on_failure_false_expected_failure(
    null_config, tmp_path, allow_failed_dependencies, combine_scripts
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    sch_inp_args = {"parameter": hf.Parameter("p2")}
    if allow_failed_dependencies != "UNSET":
        sch_inp_args["allow_failed_dependencies"] = allow_failed_dependencies

    # schema with a script that handles missing data (p2):
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(**sch_inp_args)],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_2_fail_allowed.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    tasks = [
        hf.Task(s1, inputs={"p1": "NONSENSE VALUE"}),  # will fail
        hf.Task(s2),  # depends on t1, will fail
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_allowed_failed_dependencies",
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
    assert runs[1].status is EARStatus.error


@pytest.mark.integration
@pytest.mark.parametrize("allow_failed_dependencies", [True, 1.0, 1])
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_skip_downstream_on_failure_false_handled_failure_allow_failed_dependencies(
    null_config, tmp_path, allow_failed_dependencies, combine_scripts
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    # schema with a script that handles missing data (p2):
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p2"),
                allow_failed_dependencies=allow_failed_dependencies,
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_2_fail_allowed.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    tasks = [
        hf.Task(s1, inputs={"p1": "NONSENSE VALUE"}),  # will fail
        hf.Task(s2),  # should succeed
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_allowed_failed_dependencies",
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
    assert runs[1].status is EARStatus.success


@pytest.mark.integration
@pytest.mark.parametrize(
    "allow_failed_dependencies",
    [
        "UNSET",
        None,
        False,
        0.4,
        1,
    ],
)
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_skip_downstream_on_failure_false_expected_failure_group(
    null_config, tmp_path, allow_failed_dependencies, combine_scripts
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    sch_inp_args = {"parameter": hf.Parameter("p2"), "group": "my_group"}
    if allow_failed_dependencies != "UNSET":
        sch_inp_args["allow_failed_dependencies"] = allow_failed_dependencies

    # schema with a script that handles missing data (p2):
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(**sch_inp_args)],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_2_fail_allowed_group.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    tasks = [
        hf.Task(
            s1,
            sequences=[
                hf.ValueSequence(
                    path="inputs.p1", values=[100, "NONSENSE VALUE", "NONSENSE VALUE"]
                )
            ],
            groups=[hf.ElementGroup("my_group")],
        ),  # two thirds will fail
        hf.Task(s2),  # should succeed
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_allowed_failed_dependencies",
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

    assert runs[0].status is EARStatus.success
    assert runs[1].status is EARStatus.error
    assert runs[2].status is EARStatus.error
    assert runs[3].status is EARStatus.error


@pytest.mark.integration
@pytest.mark.parametrize("allow_failed_dependencies", [True, 0.4, 1])
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_skip_downstream_on_failure_false_handled_failure_allow_failed_dependencies_group(
    null_config, tmp_path, allow_failed_dependencies, combine_scripts
):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )

    # schema with a script that handles missing data (p2):
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p2"),
                allow_failed_dependencies=allow_failed_dependencies,
                group="my_group",
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_2_fail_allowed_group.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    tasks = [
        hf.Task(
            s1,
            sequences=[
                hf.ValueSequence(path="inputs.p1", values=[100, 200, "NONSENSE VALUE"])
            ],
            groups=[hf.ElementGroup("my_group")],
        ),  # one third will fail
        hf.Task(s2),  # should succeed
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_allowed_failed_dependencies",
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

    assert runs[0].status is EARStatus.success
    assert runs[1].status is EARStatus.success
    assert runs[2].status is EARStatus.error
    assert runs[3].status is EARStatus.success


@pytest.mark.integration
def test_unset_parameters_found_when_writing_commands(null_config, tmp_path):
    cmd_ps = "echo <<parameter:p1>>; exit 1"
    cmd_bash = "exit; echo <<parameter:p1>>"
    cmd = cmd_ps if os.name == "nt" else cmd_bash
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaInput("p2")],
        actions=[
            hf.Action(commands=[hf.Command(command=cmd, stdout="<<parameter:p2>>")])
        ],  # will fail
    )
    s2 = make_schemas(
        ({"p2": None}, ("p3",), "t2"),  # command-line based action
    )
    tasks = [
        hf.Task(s1, inputs={"p1": 123}),  # will fail, and not set p2 for next task
        hf.Task(s2),  # will fail when writing commands
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_unset_parameters_in_cmdline",
        path=tmp_path,
        tasks=tasks,
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": False,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert runs[0].status is EARStatus.error
    assert runs[1].status is EARStatus.error


@pytest.mark.integration
def test_unset_parameters_found_when_writing_script_input_file(null_config, tmp_path):
    cmd_ps = "echo <<parameter:p0>>; exit 1"
    cmd_bash = "exit; echo <<parameter:p0>>"
    cmd = cmd_ps if os.name == "nt" else cmd_bash
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p0")],
        outputs=[hf.SchemaInput("p1")],
        actions=[
            hf.Action(commands=[hf.Command(command=cmd, stdout="<<parameter:p1>>")])
        ],  # will fail
    )

    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_json_out.py>>",
                script_data_in="json",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
                requires_dir=True,
            )
        ],
    )

    tasks = [
        hf.Task(s1, inputs={"p0": 123}),  # will fail, and not set p2 for next task
        hf.Task(s2),  # will fail when writing input JSON file
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_unset_parameters_in_script_input_file",
        path=tmp_path,
        tasks=tasks,
        resources={
            "any": {
                "write_app_logs": True,
                "skip_downstream_on_failure": False,
            }
        },
    )
    wk.submit(wait=True, add_to_known=False, status=False)
    runs = wk.get_all_EARs()
    assert runs[0].status is EARStatus.error
    assert runs[1].status is EARStatus.error


@pytest.mark.integration
@pytest.mark.parametrize("combine_scripts", [True, False])
def test_unset_parameters_found_when_py_script_gets_direct_inputs(
    null_config, tmp_path, combine_scripts
):
    cmd_ps = "echo <<parameter:p0>>; exit 1"
    cmd_bash = "exit; echo <<parameter:p0>>"
    cmd = cmd_ps if os.name == "nt" else cmd_bash
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput("p0")],
        outputs=[hf.SchemaInput("p1")],
        actions=[
            hf.Action(commands=[hf.Command(command=cmd, stdout="<<parameter:p1>>")])
        ],  # will fail
    )

    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
                requires_dir=True,
            )
        ],
    )

    tasks = [
        hf.Task(s1, inputs={"p0": 123}),  # will fail, and not set p2 for next task
        hf.Task(s2),  # will fail when retrieving input p2 within generated script
    ]

    wk = hf.Workflow.from_template_data(
        template_name="test_unset_parameters_in_py_script",
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
    assert runs[1].status is EARStatus.error
