from __future__ import annotations
import copy
from typing import TYPE_CHECKING
from typing_extensions import TypedDict
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    ActionInputHasNoSource,
    ActionOutputNotSchemaOutput,
    EnvironmentPresetUnknownEnvironmentError,
    InvalidIdentifier,
    TaskSchemaExtraInputs,
    TaskSchemaMissingActionOutputs,
)
from hpcflow.sdk.core.parameters import NullDefault
from hpcflow.sdk.core.test_utils import make_actions, make_parameters, make_schemas

if TYPE_CHECKING:
    from hpcflow.sdk.core.actions import Action, ActionEnvironment
    from hpcflow.sdk.core.task import TaskObjective


@pytest.fixture
def act_env_1() -> ActionEnvironment:
    return hf.ActionEnvironment("env_1")


@pytest.fixture
def action_a1(act_env_1: ActionEnvironment) -> Action:
    return hf.Action(commands=[hf.Command("ls")], environments=[act_env_1])


class SchemaKwargs(TypedDict):
    objective: TaskObjective
    actions: list[Action]


@pytest.fixture
def schema_s1_kwargs(action_a1: Action) -> SchemaKwargs:
    return {"objective": hf.TaskObjective("t1"), "actions": [action_a1]}


def test_task_schema_equality() -> None:
    t1a = hf.TaskSchema("t1", actions=[])
    t1b = hf.TaskSchema("t1", actions=[])
    assert t1a == t1b


def test_init_with_str_objective(action_a1: Action) -> None:
    obj_str = "t1"
    obj = hf.TaskObjective(obj_str)
    assert hf.TaskSchema(obj_str, actions=[action_a1]) == hf.TaskSchema(
        obj, actions=[action_a1]
    )


def test_init_with_method_with_underscore(schema_s1_kwargs) -> None:
    hf.TaskSchema(method="my_method", **schema_s1_kwargs)


def test_raise_on_invalid_method_digit(schema_s1_kwargs) -> None:
    with pytest.raises(InvalidIdentifier):
        hf.TaskSchema(method="9", **schema_s1_kwargs)


def test_raise_on_invalid_method_space(schema_s1_kwargs) -> None:
    with pytest.raises(InvalidIdentifier):
        hf.TaskSchema(method="my method", **schema_s1_kwargs)


def test_raise_on_invalid_method_non_alpha_numeric(schema_s1_kwargs) -> None:
    with pytest.raises(InvalidIdentifier):
        hf.TaskSchema(method="_mymethod", **schema_s1_kwargs)


def test_schema_action_validate() -> None:
    p1, p2, p3, p4, p5 = make_parameters(5)
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    hf.TaskSchema(
        "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4, p5]
    )


def test_schema_action_validate_raise_on_action_output_not_schema_output() -> None:
    # assert raise ValueError
    p1, p2, p3, p4, p5 = make_parameters(5)
    p7 = hf.Parameter("p7")
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    with pytest.raises(ActionOutputNotSchemaOutput) as exc_info:
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2, p7], outputs=[p3, p4]
        )
    exc = exc_info.value
    assert exc.parameter_type == "p5"


def test_schema_action_validate_raise_on_extra_schema_input() -> None:
    # assert raise ValueError
    p1, p2, p3, p4, p5 = make_parameters(5)
    p7 = hf.Parameter("p7")
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    with pytest.raises(TaskSchemaExtraInputs) as exc_info:
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2, p7], outputs=[p3, p4, p5]
        )
    exc = exc_info.value
    assert exc.extra_inputs == {"p7"}


def test_schema_action_validate_raise_on_extra_schema_output() -> None:
    p7 = hf.Parameter("p7")
    p1, p2, p3, p4, p5 = make_parameters(5)
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    with pytest.raises(TaskSchemaMissingActionOutputs) as exc_info:
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4, p5, p7]
        )
    exc = exc_info.value
    assert exc.missing_outputs == {"p7"}


def test_schema_action_validate_raise_on_extra_action_input() -> None:
    p1, p2, p3, p4, p5 = make_parameters(5)
    act_1, act_2, act_3 = make_actions(
        [(("p1", "p7"), "p5"), (("p2", "p5"), "p3"), ("p3", "p4")]
    )
    with pytest.raises(ActionInputHasNoSource) as exc_info:
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4, p5]
        )
    exc = exc_info.value
    assert exc.parameter_type == "p7"


def test_dot_access_object_list_raise_on_bad_access_attr_name() -> None:
    """Check we can't name a DotAccessObjectList item with a name that collides with a
    method name."""
    ts = hf.TaskSchema("add_object", actions=[])
    with pytest.raises(ValueError):
        hf.TaskSchemasList([ts])


def test_env_preset() -> None:
    p1, p2 = make_parameters(2)
    (act_1,) = make_actions([("p1", "p2")], env="env1")
    hf.TaskSchema(
        "t1",
        inputs=[p1],
        outputs=[p2],
        actions=[act_1],
        environment_presets={"my_preset": {"env1": {"version": 1}}},
    )


def test_env_preset_raise_bad_env() -> None:
    p1, p2 = make_parameters(2)
    (act_1,) = make_actions([("p1", "p2")], env="env1")
    with pytest.raises(EnvironmentPresetUnknownEnvironmentError):
        hf.TaskSchema(
            "t1",
            inputs=[p1],
            outputs=[p2],
            actions=[act_1],
            environment_presets={"my_preset": {"env2": {"version": 1}}},
        )


def test_env_preset_raise_bad_env_no_actions() -> None:
    with pytest.raises(EnvironmentPresetUnknownEnvironmentError):
        hf.TaskSchema(
            "t1",
            environment_presets={"my_preset": {"env1": {"version": 1}}},
        )


def test_validate_schema_input_not_in_jinja_template() -> None:
    # raise on input not in template
    with pytest.raises(TaskSchemaExtraInputs) as exc_info:
        hf.TaskSchema(
            objective="t1",
            inputs=[
                hf.SchemaInput(parameter=hf.Parameter("name")),
                hf.SchemaInput(parameter=hf.Parameter("fruits")),
                hf.SchemaInput(parameter=hf.Parameter("vegetables")),  # not in template
            ],
            actions=[hf.Action(jinja_template="test/test_template.txt")],
        )
    exc = exc_info.value
    assert exc.extra_inputs == {"vegetables"}


def test_validate_jinja_template_input_not_in_schema() -> None:
    # raise on inputs from template not in schema
    with pytest.raises(ActionInputHasNoSource) as exc_info:
        hf.TaskSchema(
            objective="t1",
            inputs=[hf.SchemaInput(parameter=hf.Parameter("name"))],  # missing fruits
            actions=[hf.Action(jinja_template="test/test_template.txt")],
        )
    exc = exc_info.value
    assert exc.parameter_type == "fruits"


def test_schema_add_parameter_dependency():
    sch = hf.TaskSchema(
        objective="my_schema", actions=[hf.Action(commands=[hf.Command("echo 'hello'")])]
    )
    sch.add_parameter_dependency("p1")
    assert sch.inputs[0] == hf.SchemaInput(parameter=hf.Parameter("p1"), multiple=False)
    assert sch.parameter_dependencies == ["p1"]
    assert sch.actions[0].get_input_types() == ("p1",)


def test_schema_add_parameter_dependency_input_source(tmp_path):
    (s1,) = make_schemas(({"p1": NullDefault.NULL}, ("p2",), "t1"))
    s2 = hf.TaskSchema(
        objective="t2", actions=[hf.Action(commands=[hf.Command("echo 'hello'")])]
    )

    # force a dependency on t1 via its output p1, even though the parameter is not
    # required by any of this schema's actions:
    s2_c = copy.deepcopy(s2)
    s2_c.add_parameter_dependency("p2")

    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        workflow_name="test_conditional_tasks",
        overwrite=True,
        path=tmp_path,
        tasks=[
            hf.Task(schema=s1, inputs={"p1": 100}),
            hf.Task(schema=s2_c),
        ],
    )
    # check the input is defined as required and not provided (so must be sourced from the
    # previous task):
    p2_inp_status = wk.tasks.t2.template.get_input_statuses(
        wk.tasks.t2.template.element_sets[0]
    )["p2"]
    assert p2_inp_status.has_default == False
    assert p2_inp_status.is_required == True
    assert p2_inp_status.is_provided == False

    # check source is defined properly:
    p2_inp_source = wk.tasks.t2.template.element_sets[0].input_sources["p2"]
    assert len(p2_inp_source) == 1
    assert p2_inp_source[0] == hf.InputSource.task(
        task_ref=0, task_source_type="output", element_iters=[0]
    )

    # check it's in the data index:
    dat_idx = wk.tasks.t2.elements[0].get_data_idx()
    assert "inputs.p2" in dat_idx


def test_schema_add_parameter_dependency_added_to_all_schema_actions(tmp_path):
    """Check that for multi-action schemas, the parameter dependency is added to all of
    them."""

    (s1,) = make_schemas(({"p1": NullDefault.NULL}, ("p2",), "t1"))
    s2 = hf.TaskSchema(
        objective="t2",
        actions=[
            hf.Action(commands=[hf.Command("echo 'hello!'")]),
            hf.Action(commands=[hf.Command("echo 'hey!'")]),
        ],
    )

    # force a dependency on t1 via its output p1, even though the parameter is not
    # required by any of this schema's actions:
    s2_c = copy.deepcopy(s2)
    s2_c.add_parameter_dependency("p2")

    wk = hf.Workflow.from_template_data(
        template_name="test_conditional_tasks",
        overwrite=True,
        path=tmp_path,
        tasks=[
            hf.Task(schema=s1, inputs={"p1": 100}),
            hf.Task(schema=s2_c),
        ],
    )

    runs = wk.get_all_EARs()
    assert "inputs.p2" in runs[1].get_data_idx()
    assert "inputs.p2" in runs[2].get_data_idx()


def test_schema_add_parameter_dependency_script_action(tmp_path):
    (s1,) = make_schemas(({"p0": NullDefault.NULL}, ("p3",), "t1"))
    s2 = hf.TaskSchema(
        objective="t2",
        inputs=[hf.SchemaInput(parameter=hf.Parameter("p1"))],  # doesn't depend on s1
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

    # force a dependency on s1 via a parameter dependency:
    s2_c = copy.deepcopy(s2)
    s2_c.add_parameter_dependency("p3")

    p1_val = 100
    t1 = hf.Task(s1, inputs={"p0": 100})
    t2 = hf.Task(schema=s2_c, inputs={"p1": p1_val})
    wk = hf.Workflow.from_template_data(
        tasks=[t1, t2],
        template_name="main_script_test_param_deps",
        path=tmp_path,
    )

    runs = wk.get_all_EARs()
    assert runs[0].get_dependent_EARs() == {1}
    assert runs[1].get_EAR_dependencies() == {0}
    assert "inputs.p3" in runs[1].get_data_idx()
    assert "inputs.p3" not in runs[1].get_data_in_values_direct()
