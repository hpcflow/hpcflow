from pathlib import Path
import time
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.test_utils import P1_parameter_cls as P1

# note: when testing the frozen app, we might not have MatFlow installed in the built in
# python_env MatFlow environment, so we should skip these tests.


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_direct_out(null_config, tmp_path: Path):
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
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == p1_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_sub_param_in_direct_out(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_sub_param_in_direct_out.py>>",
                script_data_in={"p1.a": "direct"},
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = {"a": 101}
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == p1_val["a"] + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_direct_out_single_label(null_config, tmp_path: Path):
    """This uses the same test script as the `test_script_direct_in_direct_out` test;
    single labels are trivial and need not be referenced in the script."""
    p1_label = "one"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"), labels={p1_label: {}})],
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
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={f"p1[{p1_label}]": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == p1_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_direct_out_labels(null_config, tmp_path: Path):
    p1_label_1 = "one"
    p1_label_2 = "two"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"),
                labels={p1_label_1: {}, p1_label_2: {}},
                multiple=True,
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_labels.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_1_val = 101
    p1_2_val = 201
    t1 = hf.Task(
        schema=s1,
        inputs={
            f"p1[{p1_label_1}]": p1_1_val,
            f"p1[{p1_label_2}]": p1_2_val,
        },
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == p1_1_val + p1_2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_json_out(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_json_out.py>>",
                script_data_in="json",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == p1_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_json_out_labels(null_config, tmp_path: Path):
    p1_label_1 = "one"
    p1_label_2 = "two"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"),
                labels={p1_label_1: {}, p1_label_2: {}},
                multiple=True,
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_json_out_labels.py>>",
                script_data_in="json",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_1_val = 101
    p1_2_val = 201
    t1 = hf.Task(
        schema=s1,
        inputs={
            f"p1[{p1_label_1}]": p1_1_val,
            f"p1[{p1_label_2}]": p1_2_val,
        },
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == p1_1_val + p1_2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_sub_param_in_json_out_labels(null_config, tmp_path: Path):
    p1_label_1 = "one"
    p1_label_2 = "two"
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(
                parameter=hf.Parameter("p1"),
                labels={p1_label_1: {}, p1_label_2: {}},
                multiple=True,
            )
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_sub_param_in_json_out_labels.py>>",
                script_data_in={"p1[one].a": "json", "p1[two]": "json"},
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    a_val = 101
    p1_2_val = 201
    t1 = hf.Task(
        schema=s1,
        inputs={
            f"p1[{p1_label_1}]": {"a": a_val},
            f"p1[{p1_label_2}]": p1_2_val,
        },
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == a_val + p1_2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_and_direct_in_json_out(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[
            hf.SchemaInput(parameter=hf.Parameter("p1")),
            hf.SchemaInput(parameter=hf.Parameter("p2")),
        ],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p3"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_and_direct_in_json_out.py>>",
                script_data_in={"p1": "json", "p2": "direct"},
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    p2_val = 201
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val, "p2": p2_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p3 = wk.tasks[0].elements[0].outputs.p3
    assert isinstance(p3, hf.ElementParameter)
    assert p3.value == p1_val + p2_val


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_json_and_direct_out(null_config, tmp_path: Path):
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[
            hf.SchemaInput(parameter=hf.Parameter("p2")),
            hf.SchemaOutput(parameter=hf.Parameter("p3")),
        ],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_json_and_direct_out.py>>",
                script_data_in="json",
                script_data_out={"p2": "json", "p3": "direct"},
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
    )
    p1_val = 101
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    p3 = wk.tasks[0].elements[0].outputs.p3
    assert isinstance(p3, hf.ElementParameter)
    assert p2.value == p1_val + 100
    assert p3.value == p1_val + 200


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_in_obj(null_config, tmp_path: Path):
    """Use a custom JSON dumper defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_in_obj.py>>",
                script_data_in="json",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    a_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1c": P1(a=a_val)})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == a_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_hdf5_in_obj(null_config, tmp_path: Path):
    """Use a custom HDF5 dumper defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1c"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_hdf5_in_obj.py>>",
                script_data_in="hdf5",
                script_data_out="direct",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    a_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1c": P1(a=a_val)})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == a_val + 100


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_json_out_obj(null_config, tmp_path: Path):
    """Use a custom JSON saver defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_json_out_obj.py>>",
                script_data_in="direct",
                script_data_out="json",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    p1_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p1c = wk.tasks[0].elements[0].outputs.p1c
    assert isinstance(p1c, hf.ElementParameter)
    assert p1c.value == P1(a=p1_val + 100)


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_hdf5_out_obj(null_config, tmp_path: Path):
    """Use a custom HDF5 saver defined in the P1 class."""
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p1c"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_hdf5_out_obj.py>>",
                script_data_in="direct",
                script_data_out="hdf5",
                script_exe="python_script",
                environments=[hf.ActionEnvironment(environment="python_env")],
            )
        ],
        parameter_class_modules=["hpcflow.sdk.core.test_utils"],
    )
    p1_val = 1
    t1 = hf.Task(schema=s1, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1], template_name="main_script_test", path=tmp_path
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p1c = wk.tasks[0].elements[0].outputs.p1c
    assert isinstance(p1c, hf.ElementParameter)
    assert p1c.value == P1(a=p1_val + 100)


@pytest.mark.integration
@pytest.mark.skipif("hf.run_time_info.is_frozen")
def test_script_direct_in_pass_env_spec(new_null_config, tmp_path: Path):
    vers_spec = {"version": "1.2"}
    env = hf.Environment(
        name="python_env_with_specifiers",
        specifiers=vers_spec,
        executables=[
            hf.Executable(
                label="python_script",
                instances=[
                    hf.ExecutableInstance(
                        command="python <<script_name>> <<args>>",
                        num_cores=1,
                        parallel_mode=None,
                    )
                ],
            )
        ],
    )
    hf.envs.add_object(env, skip_duplicates=True)

    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],
        outputs=[hf.SchemaOutput(parameter=hf.Parameter("p2"))],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out_env_spec.py>>",
                script_data_in="direct",
                script_data_out="direct",
                script_exe="python_script",
                script_pass_env_spec=True,
                environments=[
                    hf.ActionEnvironment(environment="python_env_with_specifiers")
                ],
            )
        ],
    )
    t1 = hf.Task(
        schema=s1,
        inputs={"p1": 101},
        environments={"python_env_with_specifiers": vers_spec},
    )
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="main_script_test",
        path=tmp_path,
    )
    wk.submit(wait=True, add_to_known=False)
    # TODO: investigate why the value is not always populated on GHA Ubuntu runners (tends
    # to be later Python versions):
    time.sleep(10)
    p2 = wk.tasks[0].elements[0].outputs.p2
    assert isinstance(p2, hf.ElementParameter)
    assert p2.value == {
        "name": "python_env_with_specifiers",
        **vers_spec,
    }
