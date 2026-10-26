from __future__ import annotations
import copy
import warnings
from typing import TYPE_CHECKING, cast
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
from hpcflow.sdk.core.object_list import ObjectList
from hpcflow.sdk.core.test_utils import make_actions, make_parameters, make_schemas

if TYPE_CHECKING:
    from hpcflow.sdk.core.json_like import JSONable
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


@pytest.mark.parametrize("multiplicity", [1, 3])
def test_schema_output_multiplicity_round_trip(multiplicity: int) -> None:
    output = hf.SchemaOutput("p1", hf.ParameterPropagationMode.EXPLICIT, multiplicity)
    assert output.multiplicity == multiplicity
    assert output.propagation_mode is hf.ParameterPropagationMode.EXPLICIT
    schema = hf.TaskSchema(
        "t1",
        outputs=[output],
        actions=[hf.Action(commands=[hf.Command("echo 1", stdout="<<parameter:p1>>")])],
    )
    data, shared = schema.to_json_like()
    assert isinstance(data, dict)
    assert shared is not None
    output.parameter._hash_value = next(iter(shared["parameters"]))
    parameters: ObjectList[JSONable] = ObjectList([output.parameter])
    restored = hf.TaskSchema.from_json_like(data, shared_data={"parameters": parameters})
    assert restored is not None
    assert restored.outputs[0].multiplicity == multiplicity
    assert restored == schema
    assert copy.deepcopy(output).multiplicity == multiplicity
    if multiplicity != 1:
        assert f"multiplicity={multiplicity}" in repr(output)
        assert output != hf.SchemaOutput("p1", hf.ParameterPropagationMode.EXPLICIT)


def test_schema_output_legacy_multiplicity() -> None:
    parameters: ObjectList[JSONable] = ObjectList([hf.Parameter("p1")])
    output = hf.SchemaOutput.from_json_like(
        {"parameter": "p1"},
        shared_data={"parameters": parameters},
    )
    assert output is not None
    assert output.multiplicity == 1
    assert output == hf.SchemaOutput("p1", multiplicity=1)


@pytest.mark.parametrize("multiplicity", [True, False, 1.0, "2", None, [], {}])
def test_schema_output_invalid_multiplicity_type(multiplicity: object) -> None:
    with pytest.raises(TypeError, match="multiplicity.*positive integer"):
        hf.SchemaOutput("p1", multiplicity=cast("int", multiplicity))


@pytest.mark.parametrize("multiplicity", [0, -1])
def test_schema_output_invalid_multiplicity_value(multiplicity: int) -> None:
    with pytest.raises(ValueError, match="multiplicity.*positive integer"):
        hf.SchemaOutput("p1", multiplicity=multiplicity)


def test_schema_output_multiplicity_reference_requires_schema_input() -> None:
    with pytest.raises(ValueError, match="does not match an input"):
        hf.TaskSchema(
            "t1",
            inputs=[hf.SchemaInput("p1")],
            outputs=[hf.SchemaOutput("p2", multiplicity="<<input:p3>>")],
            actions=[
                hf.Action(commands=[hf.Command("echo 1", stdout="<<parameter:p2>>")])
            ],
        )


@pytest.mark.parametrize(
    ("value", "error"),
    [(None, ValueError), (True, TypeError), (1.5, TypeError), (0, ValueError)],
)
def test_schema_output_resolves_and_validates_dynamic_multiplicity(
    value: object, error: type[Exception]
) -> None:
    output = hf.SchemaOutput("p2", multiplicity="<<input:p1>>")
    with pytest.raises(error, match="multiplicity input 'p1'"):
        output.resolve_multiplicity(value)

    assert output.resolve_multiplicity(3) == 3


def test_disable_array_outputs() -> None:
    schema = hf.TaskSchema("t1", disable_array_outputs=["outputs.foo.bar"])

    assert hf.TaskSchema("t1").disable_array_outputs == []
    assert schema.disable_array_outputs == ["outputs.foo.bar"]
    assert schema != hf.TaskSchema("t1")

    json_like = schema.to_json_like()[0]
    assert isinstance(json_like, dict)
    restored_schema = hf.TaskSchema.from_json_like(json_like)
    assert restored_schema is not None
    assert restored_schema.disable_array_outputs == ["outputs.foo.bar"]


def test_schema_without_array_output_fields() -> None:
    schema = hf.TaskSchema.from_json_like({"objective": "legacy_task", "actions": []})
    assert schema is not None
    assert schema.disable_array_outputs == []
    assert not hasattr(schema, "array_outputs")
    assert "array_outputs" not in schema.to_dict()


def test_task_schema_save_files_policy() -> None:
    file = hf.FileSpec(label="result", name="result.txt")
    schema = hf.TaskSchema(
        "t1",
        actions=[hf.Action(output_files=[file])],
        save_files=["result"],
    )
    assert schema.save_files == ["result"]
    assert schema != hf.TaskSchema(
        "t1", actions=[hf.Action(output_files=[file])], save_files=[]
    )

    with pytest.raises(ValueError, match="Unknown file labels"):
        hf.TaskSchema("t1", actions=[], save_files=["missing"])

    with pytest.raises(TypeError, match="save_files"):
        hf.TaskSchema(
            "t1",
            actions=[],
            save_files=cast("bool | list[str] | None", "result"),
        )


def test_task_schema_save_files_policy_paths() -> None:
    schema = hf.TaskSchema(
        "t1", actions=[], save_files=["path:logs/*.log", "path:sub/output.txt"]
    )
    assert schema.save_files == ["path:logs/*.log", "path:sub/output.txt"]

    for bad in ("path:", "path:/abs/file.txt", "path:C:\\file.txt", "path:../x.txt"):
        with pytest.raises(ValueError, match="path:"):
            hf.TaskSchema("t1", actions=[], save_files=[bad])


def test_task_schema_file_labels_include_parser_generated_files() -> None:
    src = hf.FileSpec(label="src", name="src.txt")
    gen = hf.FileSpec(label="gen", name="gen.png")
    schema = hf.TaskSchema(
        "t1",
        outputs=[hf.SchemaOutput("p1")],
        actions=[
            hf.Action(
                commands=[hf.Command("echo hi")],
                output_file_parsers=[
                    hf.OutputFileParser(
                        output_files=[src],
                        output=hf.Parameter("p1"),
                        generates_files=[gen],
                    )
                ],
            )
        ],
        save_files=["src", "gen"],
    )
    assert schema.get_file_labels() == {"src", "gen"}
    # the parser's expanded action declares the generated file as an output file:
    parser_action = next(act for act in schema.actions if act.output_file_parsers)
    assert {f.label for f in parser_action.output_files} == {"src", "gen"}


def test_action_output_files_preserved_on_expand() -> None:
    declared = hf.FileSpec(label="declared", name="declared.txt")
    schema = hf.TaskSchema(
        "t1", actions=[hf.Action(commands=[hf.Command("ls")], output_files=[declared])]
    )
    assert [f.label for f in schema.actions[0].output_files] == ["declared"]


def test_action_declared_files_imply_requires_dir() -> None:
    file = hf.FileSpec(label="f", name="f.txt")
    assert hf.Action(commands=[hf.Command("ls")]).requires_dir is False
    assert hf.Action(commands=[hf.Command("ls")], output_files=[file]).requires_dir
    assert hf.Action(commands=[hf.Command("ls")], input_files=[file]).requires_dir
    with pytest.raises(ValueError, match="requires a working directory"):
        hf.Action(commands=[hf.Command("ls")], output_files=[file], requires_dir=False)


def test_action_files_from_json_like_labels() -> None:
    """Action `input_files`/`output_files` given as labels (as in YAML task schemas)
    resolve against the shared command files."""
    in_file = hf.FileSpec(label="in_f", name="in.txt")
    out_file = hf.FileSpec(label="out_f", name="out.txt")
    shared = {"command_files": hf.CommandFilesList([in_file, out_file])}
    act = hf.Action.from_json_like(
        {
            "commands": [{"command": "ls"}],
            "input_files": ["in_f"],
            "output_files": ["out_f"],
        },
        shared,
    )
    assert act.input_files == [in_file]
    assert act.output_files == [out_file]


def test_legacy_save_files_settings_migrated(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pre-existing persisted workflows carry action/parser `save_files`; these load
    with a (once-only) deprecation warning and are mapped onto the new declarations."""
    from hpcflow.sdk.core import warnings as hf_warnings

    # each legacy variant warns once per process; start from a clean registry:
    monkeypatch.setattr(hf_warnings, "_warned_once", set())

    f1 = hf.FileSpec(label="f1", name="f1.txt")
    f2 = hf.FileSpec(label="f2", name="f2.txt")
    shared = {"command_files": hf.CommandFilesList([f1, f2])}

    with pytest.warns(DeprecationWarning, match="output_files"):
        act = hf.Action.from_json_like(
            {"commands": [{"command": "ls"}], "save_files": ["f1"]}, shared
        )
    assert act.output_files == [f1]
    assert act.requires_dir

    # an empty legacy list (as serialised for every expanded action) is silent, and the
    # same warning is not re-issued for a second legacy action:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        act = hf.Action.from_json_like(
            {"commands": [{"command": "ls"}], "save_files": []}, shared
        )
        assert act.output_files == []
        act = hf.Action.from_json_like(
            {"commands": [{"command": "ls"}], "save_files": ["f2"]}, shared
        )
        assert act.output_files == [f2]

    # parser: files not in `from_files` were written by the parser script:
    with pytest.warns(DeprecationWarning, match="generates_files"):
        ofp = hf.OutputFileParser.from_json_like(
            {"from_files": ["f1"], "save_files": ["f1", "f2"]}, shared
        )
    assert ofp.output_files == [f1]
    assert ofp.generates_files == [f2]

    # parser: `True`/`[]`/`False` only concerned retention, so are ignored (and share a
    # single warning):
    with pytest.warns(DeprecationWarning, match="ignored"):
        ofp = hf.OutputFileParser.from_json_like(
            {"from_files": ["f1"], "save_files": True}, shared
        )
    assert ofp.generates_files == []
    legacy: bool | list[str]
    for legacy in ([], False):
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            ofp = hf.OutputFileParser.from_json_like(
                {"from_files": ["f1"], "save_files": legacy}, shared
            )
        assert ofp.generates_files == []


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
