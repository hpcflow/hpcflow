"""Exercise the Linux bridge using executable fake runtimes and host schedulers."""

from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import shlex
import subprocess

import pytest
from click.testing import CliRunner
from types import SimpleNamespace

from hpcflow.sdk.cli import _make_install_CLI


@pytest.fixture
def bash_host(tmp_path):
    """Provide a Linux host, isolated mounts, and observable runtime/scheduler commands."""
    prefix = []
    distro = os.getenv("HPCFLOW_TEST_WSL_DISTRO")
    if os.name == "nt":
        if not distro or not shutil.which("wsl"):
            pytest.skip("Linux or HPCFLOW_TEST_WSL_DISTRO is required")
        prefix = ["wsl", "-d", distro, "--"]

    def linux_path(path):
        if os.name != "nt":
            return str(path)
        path = Path(path).absolute()
        return f"/mnt/{path.drive[0].lower()}/" + "/".join(path.parts[1:])

    probe = subprocess.run(
        [*prefix, "bash", "-c", "command -v jq && command -v flock"],
        capture_output=True,
    )
    if probe.returncode:
        pytest.skip("Linux jq and flock are required")
    root = tmp_path / "host mount"
    root.mkdir()
    workflow = root / "workflow"
    workflow.mkdir()
    config = tmp_path / "config"
    config.mkdir()
    binary = tmp_path / "bin"
    binary.mkdir()
    root_linux = linux_path(root)
    config_linux = linux_path(config)
    binary_linux = linux_path(binary)
    source = (
        Path(__file__).parents[3] / "sdk" / "containerisation" / "container_wrapper.sh"
    )
    wrapper = root / "wrapper.sh"
    wrapper.write_text(
        source.read_text()
        .replace("__APP_NAME_", "__HPCFLOW_")
        .replace("__APP_PACKAGE__", "hpcflow")
        .replace("__APP_CAPS__", "HPCFLOW")
        .replace("__APP_RUNTIME__", "docker")
        .replace("__HPCFLOW_CONTAINER_IMAGE__", "hpcflow:test"),
        encoding="utf-8",
        newline="\n",
    )
    runtime_script = """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\0' "$@" | jq -Rs 'split("\\u0000")[:-1]' -c >> "$TEST_ROOT/runtime.jsonl"
if [[ ${TEST_REAL_DOCKER:-0} == 1 ]]; then
    exec /usr/bin/docker "$@"
fi
words=("$@")
plan=''
for ((i=0;i<$#;i++)); do
    if [[ ${words[i]} == --env ]]; then
        value=${words[i+1]}
        case $value in
            HPCFLOW_CONTAINER_STARTED_FILE=*)
                signal=${value##*/}
                printf started > "$TEST_CONFIG/$signal" ;;
            HPCFLOW_CONTAINER_PLAN_FILE=*) plan="$TEST_CONFIG/${value##*/}" ;;
        esac
    fi
done
if [[ " $* " == *' record-host-submission '* ]]; then
    cat > "$TEST_ROOT/ack.json"
    [[ ${FAIL_ACK:-0} != 1 ]] || exit 12
    printf '{"recorded":true}'
elif [[ " $* " == *' execute-run '* ]]; then
    cat "$TEST_ROOT/run.json"
elif [[ " $* " == *' complete-host-run '* ]]; then
    [[ ${FAIL_COMPLETE:-0} != 1 ]] || exit 13
    printf '{"completed":true}'
elif [[ -n $plan ]]; then
    cp "$TEST_ROOT/plan.json" "$plan"
else
    printf 'application-output\\n'
    exit "${PASS_EXIT:-0}"
fi
"""
    for name in ("docker", "apptainer", "singularity"):
        (binary / name).write_text(runtime_script, encoding="utf-8", newline="\n")
    scheduler = binary / "scheduler"
    scheduler.write_text(
        """#!/usr/bin/env bash
set -euo pipefail
printf '%s\\0' "$@" | jq -Rs 'split("\\u0000")[:-1]' -c >> "$TEST_ROOT/scheduler.jsonl"
printf '%s\\n' "${HPCFLOW_CONTAINER_WRAPPER:?}" > "$TEST_ROOT/wrapper-path"
printf '%s\\n' "${HPCFLOW_CONTAINER_CONTEXT:?}" > "$TEST_ROOT/context-path"
[[ ${FAIL_SCHEDULER:-0} != 1 ]] || exit 7
[[ ${WARN_SCHEDULER:-0} != 1 ]] || printf warning >&2
case ${SCHEDULER_OUTPUT:-slurm} in
    slurm) printf '123;cluster\\n' ;;
    sge) printf '123.1-4:1\\n' ;;
    *) printf 'unexpected output\\n' ;;
esac
""",
        encoding="utf-8",
        newline="\n",
    )
    subprocess.run(
        [*prefix, "chmod", "+x", *[linux_path(path) for path in binary.iterdir()]],
        check=True,
    )
    for index in (0, 1):
        (workflow / f"job {index}.sh").write_text(
            "#!/bin/bash\n", encoding="utf-8", newline="\n"
        )
    jobs = [
        {
            "submission_index": 0,
            "jobscript_index": index,
            "path": f"job {index}.sh",
            "scheduler": "slurm",
            "shell": "bash",
            "is_array": False,
            "dependencies": (
                [
                    {
                        "submission_index": 0,
                        "jobscript_index": 0,
                        "is_array": False,
                        "reference": None,
                        "placeholder": "__HPCFLOW_JOB_0_0__",
                    }
                ]
                if index
                else []
            ),
            "submit_command": [
                linux_path(scheduler),
                "__HPCFLOW_JOBSCRIPT_PATH__",
                *(["afterany:__HPCFLOW_JOB_0_0__"] if index else []),
                "",
                "$(touch SHOULD_NOT_EXIST)",
                "quote'and space\nnext line",
            ],
        }
        for index in (0, 1)
    ]
    plan = {
        "schema_version": 1,
        "workflow_id": "test-workflow",
        "workflow_path": "/work/workflow",
        "jobscripts": jobs,
    }

    def run(
        args, runtime="docker", host_command=None, embedded_defaults=False, **environment
    ):
        (root / "plan.json").write_text(json.dumps(plan), encoding="utf-8", newline="\n")
        sif = root / "image.sif"
        sif.touch()
        selected_image = (
            os.environ.get("HPCFLOW_TEST_CONTAINER_IMAGE", "hpcflow:test")
            if runtime == "docker" and environment.get("TEST_REAL_DOCKER") == "1"
            else ("hpcflow:test" if runtime == "docker" else linux_path(sif))
        )
        if embedded_defaults:
            app = SimpleNamespace(
                package_name="hpcflow",
                run_time_info=SimpleNamespace(container_image=None),
            )
            exported = CliRunner().invoke(
                _make_install_CLI(app),
                ["--shell", "bash", "--runtime", runtime, "--image", selected_image],
            )
            assert exported.exit_code == 0, exported.output
            wrapper.write_text(exported.stdout, encoding="utf-8", newline="\n")
        runtime_options = (
            [] if embedded_defaults else ["--runtime", runtime, "--image", selected_image]
        )
        env = {
            "PATH": f"{binary_linux}:/usr/local/bin:/usr/bin:/bin",
            "TEST_ROOT": root_linux,
            "TEST_CONFIG": config_linux,
            **environment,
        }
        command = [
            "env",
            *[f"{key}={value}" for key, value in env.items()],
            "bash",
            "-c",
            'cd -- "$1"; shift; exec bash "$@"',
            "test",
            root_linux,
            linux_path(wrapper),
            *runtime_options,
            "--config-dir",
            config_linux,
            *args,
        ]
        if host_command is not None:
            command = [
                "env",
                *[f"{key}={value}" for key, value in env.items()],
                "bash",
                "-c",
                'cd -- "$1"; shift; exec "$@"',
                "host",
                linux_path(workflow),
                *host_command,
            ]
        result = subprocess.run(
            [*prefix, "bash", "-s"] if prefix else command,
            input=("exec " + shlex.join(command) + "\n").encode() if prefix else None,
            capture_output=True,
            timeout=180 if environment.get("TEST_REAL_DOCKER") == "1" else 40,
        )
        return subprocess.CompletedProcess(
            result.args,
            result.returncode,
            result.stdout.decode("utf-8"),
            result.stderr.decode("utf-8"),
        )

    def records(name):
        path = root / f"{name}.jsonl"
        return (
            [json.loads(line) for line in path.read_text().splitlines()]
            if path.exists()
            else []
        )

    return run, plan, root, workflow, config, records, linux_path


@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
@pytest.mark.parametrize("package", ["hpcflow", "matflow"])
def test_bash_jobscript_environment(bash_host, runtime, package):
    """Forward jobscript state literally, mapping paths without overriding bridge settings."""
    run, _, root, workflow, config, records, linux_path = bash_host
    if package == "matflow":
        wrapper = root / "wrapper.sh"
        wrapper.write_text(
            wrapper.read_text()
            .replace("HPCFLOW", "MATFLOW")
            .replace("hpcflow", "matflow"),
            encoding="utf-8",
            newline="\n",
        )
    caps = package.upper()
    path_suffixes = [
        "WK_PATH",
        "WK_PATH_ARG",
        "SUB_SCRIPTS_DIR",
        "SUB_TMP_DIR",
        "SUB_LOG_DIR",
        "SUB_STD_DIR",
        "LOG_PATH",
        "JS_FUNCS_PATH",
        "RUN_ID_FILE",
    ]
    environment = {
        f"{caps}_{suffix}": linux_path(workflow / f"{suffix} with spaces")
        for suffix in path_suffixes
    }
    values = {
        "SUB_IDX": "0",
        "JS_IDX": "2",
        "RUN_ID": "7",
        "BLOCK_IDX": "1",
        "BLOCK_ACT_IDX": "3",
        "JS_ELEM_IDX": "4",
        "BLOCK_ELEM_IDX": "5",
        "APP_LAUNCH_START": "2026-10-11T19:17:20.123456Z",
        "RUN_LOG_PATH": " ",
        "CUSTOM": "quote' $HOME; $(touch SHOULD_NOT_EXIST)\nnext line",
        "EMPTY": "",
    }
    environment.update({f"{caps}_{key}": value for key, value in values.items()})
    environment.update(
        {
            f"{caps}_CONFIG_DIR": "/wrong-config",
            f"{caps}_CONTAINER": "wrong-image",
            f"{caps}_CONTAINER_HOST_HOSTNAME": "wrong-host",
            f"{caps}_RUN_PORT": "12345",
            "UNRELATED_SECRET": "do-not-forward",
        }
    )
    result = run(["--", "validate", "template.yaml"], runtime, **environment)
    assert result.returncode == 0, result.stderr
    argv = records("runtime")[0]
    entries = [argv[index + 1] for index, word in enumerate(argv) if word == "--env"]
    forwarded = dict(entry.split("=", 1) for entry in entries)
    for suffix in path_suffixes:
        assert forwarded[f"{caps}_{suffix}"] == f"/work/workflow/{suffix} with spaces"
    for suffix, value in values.items():
        assert forwarded[f"{caps}_{suffix}"] == value
    assert forwarded[f"{caps}_CONFIG_DIR"] == f"/config/.{package}-container"
    assert forwarded[f"{caps}_CONTAINER_HOST_CONFIG_DIR"] == linux_path(config)
    assert forwarded[f"{caps}_CONTAINER_HOST_WORK_DIR"] == linux_path(root)
    assert forwarded[f"{caps}_CONTAINER"] != "wrong-image"
    assert forwarded[f"{caps}_CONTAINER_HOST_HOSTNAME"] != "wrong-host"
    assert f"{caps}_RUN_PORT" not in forwarded
    assert "UNRELATED_SECRET" not in forwarded
    assert len(entries) == len(forwarded)
    assert not (root / "SHOULD_NOT_EXIST").exists()


@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
def test_bash_exported_defaults(bash_host, runtime):
    """Submit using exported defaults without runtime/image flags or a separator."""
    run, _, _, _, _, records, _ = bash_host
    result = run(["go", "template.yaml"], runtime, embedded_defaults=True)
    assert result.returncode == 0, result.stderr
    assert len(records("scheduler")) == 2


@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
@pytest.mark.parametrize("scheduler", ["slurm", "sge"])
def test_bash_scheduler_submission(bash_host, runtime, scheduler):
    """Submit on the host, preserve literal argv, resolve dependencies and acknowledge IDs."""
    run, plan, root, workflow, config, records, linux_path = bash_host
    for job in plan["jobscripts"]:
        job["scheduler"] = scheduler
    result = run(["--", "go", "template.yaml"], runtime, SCHEDULER_OUTPUT=scheduler)
    assert result.returncode == 0, result.stderr
    launches = records("scheduler")
    assert len(launches) == 2
    assert launches[0][0] == linux_path(workflow / "job 0.sh")
    assert launches[0][1:] == [
        "",
        "$(touch SHOULD_NOT_EXIST)",
        "quote'and space\nnext line",
    ]
    assert launches[1][1] == "afterany:123"
    assert not (root / "SHOULD_NOT_EXIST").exists()
    assert not (workflow / ".hpcflow-host-submission.json").exists()
    assert json.loads((root / "ack.json").read_text())["scheduler_job_ID"] == "123"
    assert list((config / "host-mounts").glob("*.json"))
    assert not list(config.glob(".host-started-*"))
    assert not list(config.glob(".host-plan-*"))


@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
@pytest.mark.parametrize("exit_code", ["0", "17"])
def test_bash_passthrough(bash_host, runtime, exit_code):
    """Map host arguments, retain runtime exit status and avoid scheduler launches."""
    run, _, root, _, _, records, linux_path = bash_host
    result = run(
        ["--", "validate", linux_path(root / "file with spaces")],
        runtime,
        PASS_EXIT=exit_code,
    )
    assert result.returncode == int(exit_code), result.stderr
    assert result.stdout == "application-output\n"
    call = records("runtime")[0]
    assert call[-1] == "/work/file with spaces"
    if runtime == "docker":
        assert "--mount" in call
    else:
        assert "--bind" in call and "--no-eval" in call
        assert "-t" not in call and "--publish" not in call
    assert not records("scheduler")


@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
def test_bash_notebook(bash_host, runtime):
    """Launch authenticated notebooks with runtime-specific networking and entrypoints."""
    run, _, _, _, _, records, _ = bash_host
    result = run(["--notebook-port", "8889", "--", "notebook"], runtime)
    assert result.returncode == 0, result.stderr
    call = records("runtime")[0]
    assert "--ServerApp.port=8889" in call
    if runtime == "docker":
        assert "127.0.0.1:8889:8889" in call
        assert "--ServerApp.ip=0.0.0.0" in call
    else:
        assert call[0] == "exec"
        assert "hpcflow-container-entrypoint" in call
        assert "--ServerApp.ip=127.0.0.1" in call
    assert not records("scheduler")


@pytest.mark.parametrize(
    "invalid", ["direct", "path", "dependency", "placeholder", "nul", "duplicate"]
)
def test_bash_rejects_invalid_plan_before_launch(bash_host, invalid):
    """Reject unsupported or malformed plans before launching even their first job."""
    run, plan, _, _, _, records, _ = bash_host
    job = plan["jobscripts"][1]
    if invalid == "direct":
        job["scheduler"] = "direct"
    elif invalid == "path":
        job["path"] = "../escape.sh"
    elif invalid == "dependency":
        job["dependencies"][0]["jobscript_index"] = 5
        job["dependencies"][0]["placeholder"] = "__HPCFLOW_JOB_0_5__"
    elif invalid == "placeholder":
        job["submit_command"].append("__HPCFLOW_JOB_9_9__")
    elif invalid == "nul":
        job["submit_command"].append("bad\0argument")
    else:
        job["jobscript_index"] = 0
    result = run(["--", "go", "template.yaml"])
    assert result.returncode != 0
    assert not records("scheduler")


@pytest.mark.parametrize(
    "failure", ["FAIL_SCHEDULER", "WARN_SCHEDULER", "SCHEDULER_OUTPUT"]
)
def test_bash_uncertain_launch_blocks_recovery(bash_host, failure):
    """Keep uncertain launch results and refuse blind relaunch or acknowledgement."""
    run, _, _, workflow, _, records, linux_path = bash_host
    result = run(["--", "go", "template.yaml"], **{failure: "1"})
    assert result.returncode != 0
    journal = workflow / ".hpcflow-host-submission.json"
    assert journal.exists()
    recovery = run(["--resume-result", linux_path(journal)])
    assert recovery.returncode != 0
    assert len(records("scheduler")) == 1


@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
def test_bash_acknowledgement_recovery(bash_host, runtime):
    """Retry a failed metadata acknowledgement without submitting the successful job again."""
    run, _, _, workflow, _, records, linux_path = bash_host
    result = run(["--", "go", "template.yaml"], runtime, FAIL_ACK="1")
    assert result.returncode != 0
    journal = workflow / ".hpcflow-host-submission.json"
    assert json.loads(journal.read_text())["state"] == "submitted"
    recovery = run(["--resume-result", linux_path(journal)], runtime)
    assert recovery.returncode == 0, recovery.stderr
    assert len(records("scheduler")) == 1
    assert not journal.exists()


@pytest.mark.parametrize(
    "command", [["show"], ["workflow", "0", "wait"], ["submission", "get-known"]]
)
def test_bash_deferred_host_operations(bash_host, command):
    """Fail explicitly rather than querying host processes or schedulers from a container."""
    run, _, _, _, _, records, _ = bash_host
    result = run(["--", *command])
    assert result.returncode != 0
    assert "not supported" in result.stderr
    assert not records("runtime")


@pytest.mark.parametrize(
    "reference",
    [
        ["workflow", "--ref-type", "id", "0", "submit"],
        ["workflow", "0", "--ref-type", "id", "submit"],
    ],
)
def test_bash_workflow_reference_order(bash_host, reference):
    """Normalise workflow reference options and recognise local-ID submissions."""
    run, _, _, _, _, records, _ = bash_host
    result = run(["--", *reference])
    assert result.returncode == 0, result.stderr
    assert len(records("scheduler")) == 2
    call = records("runtime")[0]
    start = call.index("workflow")
    assert call[start : start + 6] == [
        "workflow",
        "--ref-type",
        "id",
        "0",
        "submit",
        "--containerised",
    ]


@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
@pytest.mark.parametrize("failure", [False, True])
def test_bash_host_command_execution(bash_host, runtime, failure):
    """Execute queued-job commands on the host and retain completion failures without rerunning."""
    run, _, root, workflow, config, records, linux_path = bash_host
    (root / "run.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "workflow_id": "test-workflow",
                "working_directory": "/work/workflow",
                "command": [
                    "bash",
                    "-c",
                    'printf "%s" "$HOST_VALUE" > result; '
                    'printf "%s" "$HPCFLOW_CONFIG_DIR" > config-path; exit 5',
                ],
                "environment": {"HOST_VALUE": "/work/workflow"},
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        [
            "--",
            "internal",
            "workflow",
            "/work/workflow",
            "execute-run",
            "0",
            "0",
            "0",
            "0",
            "0",
        ],
        runtime,
        FAIL_COMPLETE="1" if failure else "0",
    )
    assert (result.returncode != 0) == failure, result.stderr
    assert (workflow / "result").read_text() == linux_path(workflow)
    assert (workflow / "config-path").read_text() == linux_path(config)
    completion = records("runtime")[1]
    assert completion[-2:] == ["--", "5"]
    journal = workflow / ".hpcflow-run-0-0-0-0-0.json"
    assert journal.exists() == failure


@pytest.mark.parametrize("port", ["0", "65536", "8889;echo bad"])
def test_bash_invalid_notebook_port(bash_host, port):
    """Reject invalid notebook ports before launching a runtime."""
    run, _, _, _, _, records, _ = bash_host
    result = run(["--notebook-port", port, "--", "notebook"])
    assert result.returncode != 0
    assert not records("runtime")


def test_bash_invalid_run_index(bash_host):
    """Reject run indices that could escape the workflow journal directory."""
    run, _, _, _, _, records, _ = bash_host
    result = run(
        [
            "--",
            "internal",
            "workflow",
            "/work/workflow",
            "execute-run",
            "../escape",
            "0",
            "0",
            "0",
            "0",
        ]
    )
    assert result.returncode != 0
    assert not records("runtime")
