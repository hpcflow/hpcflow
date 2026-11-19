from __future__ import annotations

import copy
import json
from pathlib import Path
from textwrap import dedent
from typing import Any

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.core.template_expressions import resolve_workflow_input_references


@pytest.mark.parametrize("list_form", [False, True])
def test_template_demo_data_input_references(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, list_form: bool
) -> None:
    file_path = tmp_path / "posterior.txt"

    def demo_path(name: str) -> Path:
        assert name == "posterior.txt"
        return file_path

    monkeypatch.setattr(hf, "get_demo_data_file_path", demo_path)
    inputs = {
        "p1": "<<input:p2>>",
        "p2": {"files": ["<<demo_data_file:posterior.txt>>"]},
    }
    data = {
        "inputs": (
            [{"parameter": key, "value": value} for key, value in inputs.items()]
            if list_form
            else inputs
        ),
        "value": "<<input:p1>>",
        "path": "<<input:p2.files.0>>",
        "command": "load <<input:p1.files.0>>",
    }
    original = copy.deepcopy(data)
    result = resolve_workflow_input_references(data, input_value_cls=hf.InputValue)
    assert result["value"] == {"files": [str(file_path)]}
    assert result["path"] == str(file_path)
    assert result["command"] == f"load {file_path}"
    assert data == original


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_template_demo_data_input_in_load_txt_sequence(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, store: str
) -> None:
    file_path = tmp_path / "posterior.txt"
    file_path.write_text("1 2 3 4\n5 6 7 8\n9 10 11 12\n", encoding="utf-8", newline="\n")

    def demo_path(name: str) -> Path:
        assert name == "posterior.txt"
        return file_path

    monkeypatch.setattr(hf, "get_demo_data_file_path", demo_path)
    template = hf.WorkflowTemplate.from_YAML_string(dedent("""\
        name: demo_data_input_reference
        inputs:
          material_parameters: <<demo_data_file:posterior.txt>>
          num_simulations: 2
        template_components:
          task_schemas:
            - objective: demo_data_input_reference_consumer
              inputs:
                - parameter: p1
        tasks:
          - schema: demo_data_input_reference_consumer
            inputs:
              p1: {}
            multi_path_sequences:
              - paths: [inputs.p1.a, inputs.p1.b, inputs.p1.c, inputs.p1.d]
                values::from_load_txt:
                  file_path: <<input:material_parameters>>
                  num_samples: <<input:num_simulations>>
                  seed: 2
    """))
    assert template._inputs["material_parameters"].value == str(file_path)
    sequence = template.tasks[0].element_sets[0].multi_path_sequences[0]
    assert sequence._values_method_args is not None
    assert sequence._values_method_args["file_path"] == str(file_path)
    assert sequence._values_method_args["num_samples"] == 2
    wk = hf.Workflow.from_template(template, path=tmp_path, store=store)
    for workflow in (wk, hf.Workflow(wk.path)):
        assert workflow.tasks[0].num_elements == 2
        rows = [
            [element.get(f"inputs.p1.{key}") for key in "abcd"]
            for element in workflow.tasks[0].elements
        ]
        assert len({tuple(row) for row in rows}) == 2
        assert all(row in ([1, 2, 3, 4], [5, 6, 7, 8], [9, 10, 11, 12]) for row in rows)


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("<<input:p1>>", 3),
        ("value=<<input:p1>>", "value=3"),
        ("<<input:p1>> * <<input:p2>>", "3 * 4"),
        ("<<calc(<<input:p1>> * <<input:p2>>)>>", 12),
        ("<<calc((<<input:p1>> + <<input:p2>>) * 2)>>", 14),
        ("<<calc(-<<input:p1>> + +<<input:p2>>)>>", 1),
        ("<<calc(<<input:p1>> / 2)>>", 1.5),
        ("<<calc(<<input:p2>> // <<input:p1>>)>>", 1),
        ("<<calc(<<input:p2>> % <<input:p1>>)>>", 1),
        ("<<calc(<<input:p1>> ** 2)>>", 9),
        ("<<calc(1 + 2)>>", 3),
        ("<<calc(1 + 2)>> and <<calc(3 + 4)>>", "3 and 7"),
        ("n=<<calc(<<input:p1>> * 2)>>, m=<<calc(<<input:p2>> + 1)>>", "n=6, m=5"),
        ("<<input:collection>>", [1, {"nested": None}]),
        ("<<input:collection.1.nested>>", None),
        ("<<input:flag>>", False),
        ("echo <<parameter:p1>> * 2", "echo <<parameter:p1>> * 2"),
        ("<<calc(<<input:negative>> ** 2)>>", 4),
    ],
)
def test_template_expressions(value: str, expected: Any) -> None:
    data = {
        "inputs": {
            "p1": 3,
            "p2": 4,
            "negative": -2,
            "flag": False,
            "collection": [1, {"nested": None}],
        },
        "value": value,
    }
    original = copy.deepcopy(data)
    resolved = resolve_workflow_input_references(data)
    assert resolved["value"] == expected
    assert type(resolved["value"]) is type(expected)
    assert data == original


@pytest.mark.parametrize("list_form", [False, True])
def test_template_input_dependencies(list_form: bool) -> None:
    inputs = {
        "p1": "<<calc(<<input:p2>> * 2)>>",
        "p2": "<<input:p3>>",
        "p3": 3,
    }
    result = resolve_workflow_input_references(
        {
            "inputs": (
                [{"parameter": key, "value": value} for key, value in inputs.items()]
                if list_form
                else inputs
            ),
            "name": "workflow_<<input:p1>>",
        }
    )
    assert result["name"] == "workflow_6"


@pytest.mark.parametrize(
    ("inputs", "value", "message"),
    [
        ({}, "<<input:missing>>", "not defined"),
        ({"p1": 1}, "<<input:p1.missing>>", "not resolvable at build time"),
        ({"p1": "<<input:p2>>", "p2": "<<input:p1>>"}, "<<input:p1>>", "Cyclic"),
        ({"p1": "<<input:p1>>"}, "<<input:p1>>", "Cyclic"),
        ({"p1": "<<resource:num_cores>>"}, "<<input:p1>>", "deferred reference"),
        ({"p1": {"x": "<<parameter:p2>>"}}, "<<input:p1>>", "deferred reference"),
        ({}, "<<input:p1", "Malformed workflow input reference"),
        ({}, "<<calc(1 + 2)", "Malformed calculation"),
        ({}, "<<calc(1 + )>>", "Invalid calculation"),
        ({}, "<<calc(1 / 0)>>", "Calculation failed"),
        ({}, "<<calc(1 // 0)>>", "Calculation failed"),
        ({}, "<<calc(1e300 * 1e300)>>", "finite"),
        ({}, "<<calc(2 ** 10000)>>", "exponent"),
        ({}, "<<calc((2 ** 1000) ** 1000)>>", "exponent"),
        ({}, "<<calc(1 << 2)>>", "support only"),
        ({}, "<<calc(__import__('os').getcwd())>>", "support only"),
        ({}, "<<calc((1).__class__)>>", "support only"),
        ({}, "<<calc([1, 2][0])>>", "support only"),
        ({}, "<<calc(sum([1, 2]))>>", "support only"),
        ({"p1": True}, "<<calc(<<input:p1>> + 1)>>", "must be numbers"),
        ({"p1": "3"}, "<<calc(<<input:p1>> + 1)>>", "must be numbers"),
        ({"p1": [3]}, "<<calc(<<input:p1>> * 2)>>", "must be numbers"),
        ({"p1": None}, "<<calc(<<input:p1>> * 2)>>", "must be numbers"),
        ({"p1": "1 + 2"}, "<<calc(<<input:p1>>)>>", "must be numbers"),
        ({}, "<<calc(" + "+".join(["1"] * 100) + ")>>", "too complex"),
        ({}, "<<calc(" + "1" * 4097 + ")>>", "character limit"),
    ],
)
def test_invalid_template_expressions(
    inputs: dict[str, Any], value: str, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        resolve_workflow_input_references({"inputs": inputs, "value": value})


def test_labelled_and_sub_parameter_references() -> None:
    result = resolve_workflow_input_references(
        {
            "inputs": [
                {"parameter": "p1", "label": "chosen", "value": {"nested": {"x": 3}}},
                {"parameter": "p2[chosen].nested.x", "value": 4},
            ],
            "values": ["<<input:p1[chosen].nested.x>>", "<<input:p2[chosen].nested.x>>"],
        }
    )
    assert result["values"] == [3, 4]


def test_template_reference_copies_values() -> None:
    result = resolve_workflow_input_references(
        {"inputs": {"p1": [1, 2]}, "values": ["<<input:p1>>", "<<input:p1>>"]}
    )
    result["values"][0].append(3)
    assert result["values"][1] == [1, 2]
    assert result["inputs"]["p1"] == [1, 2]


_CREATION_YAML = dedent("""\
    name: creation_info_workflow
    inputs:
      p1: <<var:a>>
      p2: 3
      p3: <<calc(<<input:p1>> * <<input:p2>>)>>
    template_components:
      task_schemas:
        - objective: creation_info_consumer
          inputs:
            - parameter: p3
          actions:
            - commands:
                - command: echo <<parameter:p3>>
    tasks:
      - schema: creation_info_consumer
""")


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_creation_info_variables_and_calculations(tmp_path: Path, store: str) -> None:
    yaml = _CREATION_YAML
    template = hf.WorkflowTemplate.from_YAML_string(
        yaml, variables={"a": "4", "unused": "x"}
    )
    wk = hf.Workflow.from_template(template, path=tmp_path, store=store)
    for workflow in (wk, hf.Workflow(wk.path)):
        info = workflow.creation_info
        assert info["variables"] == {"a": "4", "unused": "x"}
        assert info["calculations"] == {"<<input:p1>> * <<input:p2>>": 12}
        assert info["command_line"] is None


def test_creation_info_default_variable_recorded(tmp_path: Path) -> None:
    yaml = _CREATION_YAML.replace("<<var:a>>", "<<var:a[default=5]>>")
    template = hf.WorkflowTemplate.from_YAML_string(yaml)
    wk = hf.Workflow.from_template(template, path=tmp_path, store="json")
    assert wk.creation_info["variables"] == {"a": "5"}
    assert wk.creation_info["calculations"] == {"<<input:p1>> * <<input:p2>>": 15}


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_creation_info_backwards_compatible(tmp_path: Path, store: str) -> None:
    template = hf.WorkflowTemplate.from_YAML_string(_CREATION_YAML, variables={"a": "1"})
    wk = hf.Workflow.from_template(template, path=tmp_path, store=store)
    # simulate a workflow created before these keys existed:
    info = wk._store.get_creation_info()
    info.pop("variables")
    info.pop("calculations")
    info.pop("command_line")
    assert "variables" not in info
    wk._creation_info = None
    wk._store.get_creation_info = lambda: info  # type: ignore[method-assign]
    assert wk.creation_info["variables"] == {}
    assert wk.creation_info["calculations"] == {}
    assert wk.creation_info["command_line"] is None


def test_creation_info_command_line_from_cli(tmp_path: Path) -> None:
    from click.testing import CliRunner

    path = tmp_path / "wk.yaml"
    path.write_text(_CREATION_YAML, encoding="utf-8", newline="\n")
    result = CliRunner().invoke(
        hf.cli,
        [
            "make",
            str(path),
            "--path",
            str(tmp_path),
            "--var",
            "a",
            "2",
            "--store",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    wk = hf.Workflow(result.output.strip().splitlines()[-1])
    args = [
        "make",
        str(path),
        "--path",
        str(tmp_path),
        "--var",
        "a",
        "2",
        "--store",
        "json",
    ]
    command_line = wk.creation_info["command_line"]
    assert command_line is not None
    assert command_line[-len(args) :] == args
    assert len(command_line) > len(args)  # invocation command precedes the arguments
    assert wk.creation_info["variables"] == {"a": "2"}
    assert hf.run_time_info.command_line is None
    assert hf.run_time_info.cli_args is None


@pytest.mark.parametrize("store", ["json", "zarr"])
@pytest.mark.parametrize(
    "format_", ["yaml_string", "yaml_file", "json_string", "json_file"]
)
def test_build_template_expressions(tmp_path: Path, store: str, format_: str) -> None:
    yaml = dedent("""\
        name: expression_workflow
        inputs:
          p1: 2
          p2: 3
          p3: <<calc(<<input:p1>> * <<input:p2>>)>>
        template_components:
          task_schemas:
            - objective: expression_consumer
              inputs:
                - parameter: p1
                - parameter: p4
              actions:
                - commands:
                    - command: echo <<parameter:p1>> <<parameter:p4>>
        tasks:
          - schema: expression_consumer
            inputs:
              p1: 100
              p4: <<input:p3>>
            repeats:
              number: <<calc(<<input:p1>> + <<input:p2>>)>>
              nesting_order: 2
    """)
    updates: dict[tuple[str | int, ...], Any] = {("inputs", "p1"): 4}
    if format_.startswith("yaml"):
        if format_.endswith("file"):
            path = tmp_path / "workflow.yaml"
            path.write_text(yaml, encoding="utf-8", newline="\n")
            template = hf.WorkflowTemplate.from_YAML_file(path, updates=updates)
        else:
            template = hf.WorkflowTemplate.from_YAML_string(yaml, updates=updates)
    else:
        from hpcflow.sdk.core.utils import read_YAML_str

        text = json.dumps(read_YAML_str(yaml))
        if format_.endswith("file"):
            path = tmp_path / "workflow.json"
            path.write_text(text, encoding="utf-8", newline="\n")
            template = hf.WorkflowTemplate.from_JSON_file(path, updates=updates)
        else:
            template = hf.WorkflowTemplate.from_JSON_string(text, updates=updates)
    assert template.tasks[0].element_sets[0].repeats[0].number == 7
    assert template.tasks[0].element_sets[0].repeats[0].nesting_order == 2
    wk = hf.Workflow.from_template(template, path=tmp_path, store=store)
    for workflow in (wk, hf.Workflow(wk.path)):
        assert workflow.tasks[0].num_elements == 7
        assert [el.get("inputs.p4") for el in workflow.tasks[0].elements] == [12] * 7
        assert [el.get("inputs.p1") for el in workflow.tasks[0].elements] == [100] * 7
        assert workflow.template._inputs["p3"].value == 12


def test_template_reference_not_task_input() -> None:
    yaml = dedent("""\
        name: unavailable_task_reference
        template_components:
          task_schemas:
            - objective: unavailable_task_reference_consumer
              inputs:
                - parameter: p1
                - parameter: p2
        tasks:
          - schema: unavailable_task_reference_consumer
            inputs:
              p1: 2
              p2: <<input:p1>>
    """)
    with pytest.raises(ValueError, match="workflow-level input, not a task input"):
        hf.WorkflowTemplate.from_YAML_string(yaml)


@pytest.mark.parametrize("calc", [False, True])
def test_template_reference_output_multiplicity(calc: bool) -> None:
    reference = "<<calc(<<input:p1>> * 2)>>" if calc else "<<input:p1>>"
    template = hf.WorkflowTemplate.from_YAML_string(dedent("""\
            name: expression_multiplicity
            inputs:
              p1: 2
            template_components:
              task_schemas:
                - objective: expression_multiplicity_CALC
                  inputs:
                    - parameter: p1
                  outputs:
                    - parameter: p2
                      multiplicity: "REFERENCE"
                  actions:
                    - commands:
                        - command: echo <<parameter:p1>>
                          stdout: <<parameter:p2>>
            tasks:
              - schema: expression_multiplicity_CALC
                inputs:
                  p1: 3
        """).replace("CALC", str(calc)).replace("REFERENCE", reference))
    output = template.tasks[0].schema.outputs[0]
    assert output.multiplicity == (4 if calc else "<<input:p1>>")
    assert output.resolve_multiplicity(3) == (4 if calc else 3)


def test_template_references_in_descriptors() -> None:
    template = hf.WorkflowTemplate.from_YAML_string(dedent("""\
        name: descriptor_references
        inputs:
          p1: 3
          p2: 2
        template_components:
          task_schemas:
            - objective: descriptor_references_consumer
              inputs:
                - parameter: p3
        tasks:
          - schema: descriptor_references_consumer
            repeats:
              number: <<input:p2>>
              nesting_order: 2
            sequences:
              - path: inputs.p3
                values::from_range:
                  start: 0
                  stop: <<input:p1>>
            resources:
              any:
                num_cores: <<calc(<<input:p1>> * <<input:p2>>)>>
    """))
    element_set = template.tasks[0].element_sets[0]
    assert element_set.repeats[0].number == 2
    assert element_set.resources.get().num_cores == 6
    assert element_set.sequences[0].values == [0, 1, 2]


def test_deferred_input_references_rejected() -> None:
    with pytest.raises(ValueError, match="not resolvable at build time"):
        resolve_workflow_input_references(
            {
                "inputs": [
                    {"parameter": "p1", "value_class_method": "from_data", "value": {}}
                ],
                "value": "<<input:p1>>",
            }
        )


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_build_time_factory_input_references(tmp_path: Path, store: str) -> None:
    template = hf.WorkflowTemplate.from_YAML_string(dedent("""\
        name: factory_references
        inputs:
          p1::from_linear_space:
            start: 0
            stop: 1
            num: 3
          p2: <<input:p1.1>>
        template_components:
          task_schemas:
            - objective: factory_references_consumer
              inputs:
                - parameter: p3
        tasks:
          - schema: factory_references_consumer
            inputs:
              p3: <<input:p1>>
    """))
    assert template._inputs["p1"].value == [0, 0.5, 1]
    assert template._inputs["p1"]._value_method == "from_linear_space"
    assert template._inputs["p2"].value == 0.5
    assert template.tasks[0].element_sets[0].inputs[0].value == [0, 0.5, 1]
    wk = hf.Workflow.from_template(template, path=tmp_path, store=store)
    assert hf.Workflow(wk.path).tasks[0].elements[0].get("inputs.p3") == [0, 0.5, 1]


def test_random_factory_input_is_generated_once() -> None:
    data = {
        "inputs": {
            "p1::from_normal": {"shape": 5},
            "p2": "<<input:p1>>",
        },
        "value": "<<input:p1>>",
    }
    result = resolve_workflow_input_references(data, input_value_cls=hf.InputValue)
    generated = result["inputs"][0]["_value"]
    assert generated == result["value"]
    assert generated == result["inputs"][1]["value"]
    assert result["inputs"][0]["_value_method"] == "from_normal"
    assert "seed" in result["inputs"][0]["_value_method_args"]


def test_template_variables_before_input_references() -> None:
    template = hf.WorkflowTemplate.from_YAML_string(
        dedent("""\
            name: variable_references_<<input:p1>>
            inputs:
              p1: <<var:N>>
              p2: <<calc(<<input:p1>> * 2)>>
            tasks: []
        """),
        variables={"N": "3"},
    )
    assert template.name == "variable_references_3"
    assert template._inputs["p2"].value == 6
