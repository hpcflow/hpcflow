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
        [
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, ("p3",), "t2"],
        ],
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
        [
            [{"p1": None}, ("p2",), "t1"],
            [{"p2": None}, ("p3",), "t2"],
        ],
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
