from dataclasses import replace
from pathlib import Path
from typing import Any, Callable, cast

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.execution.script_worker.bootstrap import (
    ScriptWorkerManifest,
    build_script_worker_invocations,
)
from hpcflow.sdk.execution.script_worker.protocol import ExecuteScriptRequest
from hpcflow.sdk.scripting import ScriptWriter


def make_worker_workflow(tmp_path: Path):
    schema = hf.TaskSchema(
        objective="worker_bootstrap",
        inputs=[hf.SchemaInput("p1")],
        outputs=[hf.SchemaOutput("p2")],
        actions=[
            hf.Action(
                script="<<script:main_script_test_direct_in_direct_out.py>>",
                script_exe="python_script",
                script_data_in="direct",
                script_data_out="direct",
                environments=[hf.ActionEnvironment(environment="python_env")],
                requires_dir=True,
            )
        ],
    )
    workflow = hf.Workflow.from_template_data(
        template_name="worker_bootstrap",
        path=tmp_path,
        tasks=[hf.Task(schema, inputs={"p1": 101}, repeats=2)],
    )
    workflow.add_submission()
    return workflow


def test_manifest_round_trip_and_validation(tmp_path: Path) -> None:
    manifest = ScriptWorkerManifest(
        workflow_path=str(tmp_path),
        submission_idx=0,
        jobscript_idx=1,
        worker_id="0123456789",
        action_keys=((0, 1), (1, 0)),
        config_directory=str(tmp_path / "config"),
        config_key="hpcflow",
    )
    manifest_path = tmp_path / "worker.json"

    manifest.write(manifest_path)

    assert ScriptWorkerManifest.read(manifest_path) == manifest
    with pytest.raises(ValueError, match="missing or unknown"):
        ScriptWorkerManifest.from_dict(
            {**manifest.to_dict(), "unexpected": "stale-field"}
        )


def test_bootstrap_binds_each_block_element_to_persisted_run(tmp_path: Path) -> None:
    workflow = make_worker_workflow(tmp_path)
    runs = workflow.get_all_EARs()
    worker_args = runs[0].get_script_worker_args(0)
    writer = ScriptWriter.create(worker_args, {(0, 0): runs[0].action})
    namespace: dict[str, object] = {"__name__": "worker_test"}
    exec(compile(writer.compose_worker(), "<worker>", "exec"), namespace)
    action_scripts = cast(
        dict[tuple[int, int], Callable[..., Any]], namespace["ACTION_SCRIPTS"]
    )

    manifest = ScriptWorkerManifest(
        workflow_path=str(workflow.path),
        submission_idx=0,
        jobscript_idx=0,
        worker_id=worker_args.stable_hash,
        action_keys=((0, 0),),
        config_directory=str(hf.config.config_directory),
        config_key=hf.config.config_key,
    )

    invocations = build_script_worker_invocations(
        manifest,
        action_scripts,
        workflow,
    )

    assert set(invocations) == {
        ExecuteScriptRequest(runs[0].id_, 0, 0, 0),
        ExecuteScriptRequest(runs[1].id_, 0, 0, 1),
    }

    stale_manifest = replace(manifest, worker_id="0000000000")
    with pytest.raises(ValueError, match="worker identity"):
        build_script_worker_invocations(
            stale_manifest,
            action_scripts,
            workflow,
        )
