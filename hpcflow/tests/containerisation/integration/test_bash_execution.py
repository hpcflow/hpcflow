"""Live Linux Bash bridge validation with Docker and an observable scheduler stub."""

from __future__ import annotations

import json
import os

import pytest

from hpcflow.tests.containerisation.unit.test_bash_wrapper import bash_host


@pytest.mark.integration
def test_bash_docker_queued_workflow(bash_host):
    """Run a real container-prepared queued job on Linux and reload its numerical output."""
    image = os.environ.get("HPCFLOW_TEST_CONTAINER_IMAGE")
    if not image:
        pytest.skip("Set HPCFLOW_TEST_CONTAINER_IMAGE to a locally built image")
    run, _, root, _, config, records, linux_path = bash_host
    runtime_env = {"TEST_REAL_DOCKER": "1"}
    exported = run(["--", "install", "--shell", "bash"], **runtime_env)
    assert exported.returncode == 0, exported.stderr
    assert exported.stdout.startswith("#!/usr/bin/env bash\n")
    (root / "wrapper.sh").write_text(exported.stdout, encoding="utf-8", newline="\n")
    scheduler = root.parent / "bin" / "scheduler"
    configured = run(
        [
            "--",
            "config",
            "add-scheduler",
            "slurm",
            "--defaults",
            json.dumps({"submit_cmd": linux_path(scheduler)}),
        ],
        **runtime_env,
    )
    assert configured.returncode == 0, configured.stderr
    template = {
        "tasks": [
            {
                "schema": "test_t1_conditional_OS",
                "inputs": {"p1": 1},
                "resources": {
                    "any": {"scheduler": "slurm", "shell": "bash", "os_name": "posix"}
                },
            }
        ]
    }
    (root / "template.json").write_text(
        json.dumps(template), encoding="utf-8", newline="\n"
    )
    prepared = run(
        [
            "--",
            "go",
            "template.json",
            "--format",
            "json",
            "--path",
            ".",
            "--name",
            "live",
            "--name-no-timestamp",
        ],
        **runtime_env,
    )
    assert prepared.returncode == 0, prepared.stderr
    # The real scheduler command contains just --parsable and the script path.
    script_path = records("scheduler")[0][-1]
    executed = run(
        [],
        host_command=[
            "bash",
            "-c",
            'cd -- "$1"; exec bash "$2"',
            "job",
            linux_path(root / "live"),
            script_path,
        ],
        **runtime_env,
    )
    assert executed.returncode == 0, executed.stderr
    result = run(
        [],
        host_command=[
            "docker",
            "run",
            "--rm",
            "--mount",
            f"type=bind,source={linux_path(root)},target=/work",
            "--mount",
            f"type=bind,source={linux_path(config)},target=/config/.hpcflow-container",
            "--env",
            "HPCFLOW_CONFIG_DIR=/config/.hpcflow-container",
            "--entrypoint",
            "python",
            image,
            "-c",
            "from hpcflow.app import app; "
            "assert app.Workflow('/work/live').tasks[0].elements[0].outputs.p2.value == '101'; "
            "print('101')",
        ],
        **runtime_env,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == "101"
