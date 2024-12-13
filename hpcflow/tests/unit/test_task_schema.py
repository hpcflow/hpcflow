from __future__ import annotations
from typing import TYPE_CHECKING
from typing_extensions import TypedDict
import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import (
    EnvironmentPresetUnknownEnvironmentError,
    InvalidIdentifier,
)
from hpcflow.sdk.core.test_utils import make_actions, make_parameters

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
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    hf.TaskSchema("t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4])


def test_schema_action_validate_raise_on_extra_schema_input() -> None:
    # assert raise ValueError
    p1, p2, p3, p4 = make_parameters(4)
    p7 = hf.Parameter("p7")
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    with pytest.raises(ValueError):
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2, p7], outputs=[p3, p4]
        )


def test_schema_action_validate_raise_on_extra_schema_output() -> None:
    p7 = hf.Parameter("p7")
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions([("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4")])
    with pytest.raises(ValueError):
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4, p7]
        )


def test_schema_action_validate_raise_on_extra_action_input() -> None:
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions(
        [(("p1", "p7"), "p5"), (("p2", "p5"), "p3"), ("p3", "p4")]
    )
    with pytest.raises(ValueError):
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4]
        )


def test_schema_action_validate_raise_on_extra_action_output() -> None:
    p1, p2, p3, p4 = make_parameters(4)
    act_1, act_2, act_3 = make_actions(
        [("p1", "p5"), (("p2", "p5"), "p3"), ("p3", "p4", "p7")]
    )
    with pytest.raises(ValueError):
        hf.TaskSchema(
            "t1", actions=[act_1, act_2, act_3], inputs=[p1, p2], outputs=[p3, p4]
        )


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
