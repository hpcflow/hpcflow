from __future__ import annotations
from pathlib import Path
import sys
from typing import TYPE_CHECKING
import pytest
import requests

from hpcflow.app import app as hf

if TYPE_CHECKING:
    from hpcflow.sdk.core.actions import Action, ActionEnvironment


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


def test_get_demo_data_manifest(null_config) -> None:
    hf.get_demo_data_files_manifest()


@pytest.mark.xfail(
    condition=sys.platform == "darwin",
    raises=requests.exceptions.HTTPError,
    reason=(
        "GHA MacOS runners use the same IP address, so we get rate limited when "
        "retrieving demo data from GitHub."
    ),
)
def test_get_demo_data_cache(null_config) -> None:
    hf.clear_demo_data_cache_dir()
    hf.cache_demo_data_file("text_file.txt")
    with hf.demo_data_cache_dir.joinpath("text_file.txt").open("rt") as fh:
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
