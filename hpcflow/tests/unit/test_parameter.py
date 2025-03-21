from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import random
import string
import sys
from textwrap import dedent
from typing import ClassVar

import pytest
import requests

from hpcflow.app import app as hf
from hpcflow.sdk.core.errors import ParametersMetadataReadOnlyError
from hpcflow.sdk.typing import hydrate
from hpcflow.sdk.core.parameters import ParameterValue


@dataclass
@hydrate
class MyParameterP1(ParameterValue):
    _typ: ClassVar[str] = "p1_test"
    a: int

    def CLI_format(self):
        return str(self.a)


@pytest.mark.integration
@pytest.mark.parametrize("store", ["json", "zarr"])
def test_submission_with_specified_parameter_class_module(
    null_config, tmp_path: Path, store: str
):
    """Test we can use a ParameterValue subclass that is defined separately from the main
    code (i.e. not automatically imported on app init)."""

    # not sure why this is necessary; without this we end up with two `p1_test`
    # parameters, despite `skip_duplicates=True`:
    hf.reload_template_components()

    hf.parameters.add_object(hf.Parameter("p1_test"), skip_duplicates=True)
    s1 = hf.TaskSchema(
        objective="t1",
        inputs=[hf.SchemaInput(parameter="p1_test")],
        outputs=[hf.SchemaOutput(parameter="p2")],
        actions=[
            hf.Action(
                commands=[
                    hf.Command(
                        command="Write-Output (<<parameter:p1_test>> + 100)",
                        stdout="<<parameter:p2>>",
                    )
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
            hf.Action(
                commands=[
                    hf.Command(
                        command='echo "$((<<parameter:p1_test>> + 100))"',
                        stdout="<<parameter:p2>>",
                    )
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
        ],
        parameter_class_modules=["hpcflow.tests.unit.test_parameter"],
    )
    p1_value = MyParameterP1(a=10)
    t1 = hf.Task(schema=s1, inputs=[hf.InputValue("p1_test", value=p1_value)])
    wk = hf.Workflow.from_template_data(
        tasks=[t1],
        template_name="w1",
        path=tmp_path,
        store=store,
    )
    wk.submit(wait=True, add_to_known=False)
    assert wk.tasks.t1.elements[0].get("outputs.p2") == "110"


@pytest.mark.parametrize("store", ["json", "zarr"])
def test_unseen_parameter(null_config, tmp_path: Path, store: str):
    """Test we can generate a workflow that uses an unseen parameter type."""

    random_str = "".join(random.choice(string.ascii_letters) for _ in range(10))
    p_type = f"p_{random_str}"
    act = hf.Action(
        commands=[
            hf.Command(
                command=f'echo "$((<<parameter:{p_type}>> + 1))"',
                stdout=f"<<int(parameter:{p_type})>>",
            )
        ]
    )
    ts = hf.TaskSchema(
        objective="add_one",
        actions=[act],
        inputs=[hf.SchemaInput(p_type)],
        outputs=[hf.SchemaOutput(p_type)],
    )
    wkt = hf.WorkflowTemplate(
        name="increment_number",
        tasks=[
            hf.Task(
                schema=ts,
                inputs={p_type: 5},
            )
        ],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path, store=store)
    assert wk.tasks[0].elements[0].get(f"inputs.{p_type}") == 5


def test_iter(new_null_config, tmp_path: Path):
    values = [1, 2, 3]
    wkt = hf.WorkflowTemplate(
        name="test",
        tasks=[
            hf.Task(
                schema=hf.task_schemas.test_t1_ps,
                sequences=[hf.ValueSequence(path="inputs.p1", values=values)],
            ),
        ],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    for idx, param_p1_i in enumerate(wk.tasks[0].inputs.p1):
        assert param_p1_i.value == values[idx]


def test_slice(new_null_config, tmp_path: Path):
    values = [1, 2, 3]
    wkt = hf.WorkflowTemplate(
        name="test",
        tasks=[
            hf.Task(
                schema=hf.task_schemas.test_t1_ps,
                sequences=[hf.ValueSequence(path="inputs.p1", values=values)],
            ),
        ],
    )
    wk = hf.Workflow.from_template(wkt, path=tmp_path)
    p1_params = wk.tasks[0].inputs.p1[0::2]
    assert len(p1_params) == 2
    assert p1_params[0].value == values[0]
    assert p1_params[1].value == values[2]


@pytest.mark.xfail(
    condition=sys.platform == "darwin",
    raises=requests.exceptions.HTTPError,
    reason=(
        "GHA MacOS runners use the same IP address, so we get rate limited when "
        "retrieving demo data from GitHub."
    ),
)
def test_demo_data_substitution_param_value_class_method(new_null_config, tmp_path: Path):
    yaml_str = dedent(
        """\
        name: temp
        template_components:
          task_schemas:
            - objective: test
              inputs:
                - parameter: p1c
              parameter_class_modules: [hpcflow.sdk.core.test_utils]
        tasks: 
          - schema: test
            inputs:
              p1c::from_file:
                path: <<demo_data_file:text_file.txt>>
    """
    )
    wk = hf.Workflow.from_YAML_string(YAML_str=yaml_str, path=tmp_path)
    assert wk.tasks[0].template.element_sets[0].inputs[0].value == {
        "path": str(hf.demo_data_cache_dir.joinpath("text_file.txt"))
    }


@pytest.mark.xfail(
    condition=sys.platform == "darwin",
    raises=requests.exceptions.HTTPError,
    reason=(
        "GHA MacOS runners use the same IP address, so we get rate limited when "
        "retrieving demo data from GitHub."
    ),
)
def test_demo_data_substitution_value_sequence_class_method(
    new_null_config, tmp_path: Path
):
    yaml_str = dedent(
        """\
        name: temp
        template_components:
          task_schemas:
            - objective: test
              inputs:
                - parameter: p1
        tasks:
          - schema: test
            sequences:
              - path: inputs.p1
                values::from_file:
                  file_path: <<demo_data_file:text_file.txt>>
    """
    )
    wk = hf.Workflow.from_YAML_string(YAML_str=yaml_str, path=tmp_path)
    assert wk.tasks[0].template.element_sets[0].sequences[0].values == [
        "1",
        "2",
        "3",
        "4",
        "5",
        "6",
        "7",
        "8",
        "9",
        "10",
    ]


def test_json_store_parameters_metadata_cache_raises_on_modify(
    null_config, tmp_path: Path
):
    wk = hf.make_demo_workflow("workflow_1", path=tmp_path, store="json")
    assert isinstance(wk, hf.Workflow)
    num_params = len(wk.get_all_parameters())
    with pytest.raises(ParametersMetadataReadOnlyError):
        with wk._store.parameters_metadata_cache():
            wk._add_unset_parameter_data(source={"type": "UNSET-FOR-THIS-TEST"})
    assert len(wk.get_all_parameters()) == num_params
