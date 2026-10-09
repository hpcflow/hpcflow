from __future__ import annotations
from pathlib import Path
from unittest.mock import Mock
import sys
from typing import TYPE_CHECKING
import pytest
import requests

from hpcflow.app import app as hf

if TYPE_CHECKING:
    from hpcflow.sdk.core.actions import Action, ActionEnvironment


@pytest.mark.parametrize(
    ("containerised", "modify_js"),
    [(False, False), (False, True), (True, False), (True, True)],
)
@pytest.mark.parametrize("return_idx", [False, True])
@pytest.mark.parametrize(
    "api_name",
    ["make_and_submit_workflow", "make_and_submit_demo_workflow", "submit_workflow"],
)
def test_containerised_passed_to_workflow_submit(
    tmp_path, monkeypatch, api_name, return_idx, containerised, modify_js
):
    workflow = hf.Workflow.from_template(hf.WorkflowTemplate(name="w1"), path=tmp_path)
    indices = {0: [0]}
    submit = Mock(return_value=indices)
    monkeypatch.setattr(hf.Workflow, "submit", submit)
    if api_name == "submit_workflow":
        args = {"workflow_path": workflow.path}
    else:
        make_name = (
            "_make_workflow"
            if api_name == "make_and_submit_workflow"
            else "_make_demo_workflow"
        )
        monkeypatch.setattr(hf, make_name, Mock(return_value=workflow))
        args = (
            {"template_file_or_str": "template.yaml"}
            if api_name == "make_and_submit_workflow"
            else {"workflow_name": "workflow_1"}
        )
    result = getattr(hf, api_name)(
        **args, containerised=containerised, return_idx=return_idx, modify_js=modify_js
    )
    submit.assert_called_once()
    assert submit.call_args.kwargs["containerised"] is containerised
    assert submit.call_args.kwargs["modify_js"] is modify_js
    if api_name == "submit_workflow":
        assert result == (indices if return_idx or containerised else None)
    else:
        assert result == (
            (workflow, indices) if return_idx or containerised else workflow
        )


@pytest.fixture
def act_env_1() -> ActionEnvironment:
    return hf.ActionEnvironment(environment="env_1")


@pytest.fixture
def act_1(act_env_1) -> Action:
    return hf.Action(
        commands=[hf.Command("<<parameter:p1>>")],
        environments=[act_env_1],
    )


def test_shared_data_from_json_like_with_shared_data_dependency(act_1: Action):
    """Check we can generate some shared data objects where one depends on another."""

    p1 = hf.Parameter("p1")
    p1._set_hash()
    p1_hash = p1._hash_value
    assert p1_hash is not None

    ts1 = hf.TaskSchema(objective="ts1", actions=[act_1], inputs=[p1])
    ts1._set_hash()
    ts1_hash = ts1._hash_value
    assert ts1_hash is not None

    env_label = ts1.actions[0].environments[0].environment

    shared_data_json: dict[str, dict] = {
        "parameters": {
            p1_hash: {
                "is_file": p1.is_file,
                "sub_parameters": [],
                "type": p1.typ,
            }
        },
        "task_schemas": {
            ts1_hash: {
                "method": ts1.method,
                "implementation": ts1.implementation,
                "version": ts1.version,
                "objective": ts1.objective.name,
                "inputs": [{"parameter": f"hash:{p1_hash}", "labels": {"": {}}}],
                "outputs": [],
                "actions": [
                    {
                        "_from_expand": False,
                        "script": None,
                        "commands": [
                            {
                                "command": "<<parameter:p1>>",
                                "executable": None,
                                "arguments": None,
                                "stdout": None,
                                "stderr": None,
                                "stdin": None,
                            }
                        ],
                        "input_files": [],
                        "output_files": [],
                        "input_file_generators": [],
                        "output_file_parsers": [],
                        "environments": [
                            {
                                "scope": {"kwargs": {}, "type": "ANY"},
                                "environment": env_label,
                            }
                        ],
                        "rules": [],
                    }
                ],
            }
        },
    }

    sh = hf.template_components_from_json_like(shared_data_json)

    assert sh["parameters"] == hf.ParametersList([p1]) and sh[
        "task_schemas"
    ] == hf.TaskSchemasList([ts1])


def test_get_data_manifest() -> None:
    assert hf.get_data_manifest("data")


def test_get_program_manifest() -> None:
    assert hf.get_data_manifest("program")


@pytest.mark.skip(
    reason=(
        "In CI testing, we install the cache from an pre-existing directory for each test"
        "job, to avoid rate-limiting. Since this test clears the data cache, it would "
        "result in test jobs that run after it retrieving data from GitHub, thus causing "
        "rate-limiting."
    ),
)
def test_get_demo_data_cache() -> None:
    hf.clear_data_cache_dir()
    hf.cache_data_file("text_file_1.txt")
    with hf.data_cache_dir.joinpath("text_file_1.txt").open("rt") as fh:
        contents = fh.read()
    assert contents == "\n".join(f"{i}" for i in range(1, 11)) + "\n"


def test_list_demo_workflows():
    # sanity checks
    lst = hf.list_demo_workflows()
    assert isinstance(lst, tuple)
    assert all(isinstance(i, str) and "." not in i for i in lst)  # no extension included


def test_get_demo_workflows():
    # sanity checks
    lst = hf.list_demo_workflows()
    demo_paths = hf._get_demo_workflows()
    # keys should be those in the list:
    assert sorted(list(lst)) == sorted(list(demo_paths.keys()))

    # values should be distinct, absolute paths:
    assert all(isinstance(i, Path) and i.is_absolute() for i in demo_paths.values())
    assert len(set(demo_paths.values())) == len(demo_paths)
