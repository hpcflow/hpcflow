from __future__ import annotations

import os
import json
import re
from pathlib import Path
import shutil
import socket
import subprocess
import time

import pytest

from hpcflow.app import app as hf
from hpcflow.sdk.submission.enums import JobscriptElementState
from hpcflow.sdk.submission.submission import Submission


@pytest.mark.integration
def test_container_entrypoint_startup_signal(tmp_path):
    """Signal through the host mount before executing the application, without extra output."""
    image = os.environ.get("HPCFLOW_TEST_CONTAINER_IMAGE")
    if not image:
        pytest.skip("Set HPCFLOW_TEST_CONTAINER_IMAGE")
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--mount",
            f"type=bind,source={tmp_path},target=/signal",
            "--env",
            "HPCFLOW_CONTAINER_STARTED_FILE=/signal/started",
            "--entrypoint",
            "hpcflow-container-entrypoint",
            image,
            "sh",
            "-c",
            'test "$(cat /signal/started)" = started && printf application-output; exit 7',
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 7, result.stderr
    assert result.stdout == "application-output"
    assert result.stderr == ""
    assert (tmp_path / "started").read_text() == "started\n"


@pytest.mark.integration
@pytest.mark.skipif(os.name != "nt", reason="Windows host with Linux container")
def test_container_local_id_submit_with_default_tty(tmp_path, monkeypatch):
    """Submit remaining tasks by local ID from another mount with live TTY progress."""
    image = os.environ.get("HPCFLOW_TEST_CONTAINER_IMAGE")
    pwsh = shutil.which("pwsh")
    if not image or not pwsh:
        pytest.skip("Set HPCFLOW_TEST_CONTAINER_IMAGE and install PowerShell 7")
    config = tmp_path / "config"
    monkeypatch.setenv("HPCFLOW_CONFIG_DIR", str(config))
    monkeypatch.setenv("NO_COLOR", "")
    root = tmp_path / "workflow mount"
    observer = tmp_path / "observer mount"
    root.mkdir()
    observer.mkdir()
    source = (
        Path(__file__).parents[3] / "sdk" / "containerisation" / "container_wrapper.ps1"
    )
    wrapper = observer / "hpcflow.ps1"
    wrapper.write_text(
        source.read_text().replace("__APP_NAME_", "__HPCFLOW_"),
        encoding="utf-8",
        newline="\n",
    )
    template = {
        "tasks": [
            {"schema": "test_t1_conditional_OS", "inputs": {"p1": value}}
            for value in (101, 102)
        ]
    }
    (root / "template.json").write_text(
        json.dumps(template), encoding="utf-8", newline="\n"
    )

    def invoke(cwd, args):
        def quote(value):
            return "'" + str(value).replace("'", "''") + "'"

        command = (
            "$PSStyle.OutputRendering='ANSI'; "
            f"& {quote(wrapper)} -Image {quote(image)} -Machine 'local-id-test' "
            "-HpcflowArgs @("
            + ", ".join(quote(arg) for arg in args)
            + "); exit $LASTEXITCODE"
        )
        result = subprocess.run(
            [pwsh, "-NoProfile", "-Command", command],
            cwd=cwd,
            capture_output=True,
            timeout=120,
        )
        assert result.returncode == 0, (result.stdout + result.stderr).decode("utf-8")
        return result.stdout.decode("utf-8")

    prepared = invoke(
        root,
        [
            "go",
            "template.json",
            "--format",
            "json",
            "--tasks",
            "0",
            "--path",
            ".",
            "--name",
            "demo",
            "--name-no-timestamp",
        ],
    )
    assert "\x1b[?25l" in prepared and "\x1b[?25h" in prepared
    workflow_path = root / "demo"
    registry = (
        config.with_name("config-container")
        / "data"
        / "hpcflow"
        / "local-id-test"
        / "known_submissions.txt"
    )
    local_id = registry.read_text(encoding="utf-8").split("::", 1)[0]
    assert local_id == "0"
    status = invoke(
        observer, ["workflow", local_id, "--ref-type", "id", "show-all-status"]
    )
    assert status
    submitted = invoke(observer, ["workflow", local_id, "submit", "--tasks", "1"])
    assert "\x1b[?25l" in submitted and "\x1b[?25h" in submitted
    deadline = time.monotonic() + 120
    while True:
        workflow = hf.Workflow(workflow_path)
        runs = [
            run
            for sub in workflow.submissions
            for job in sub.jobscripts
            for run in job.all_EARs
        ]
        if runs and all(run.end_time is not None for run in runs):
            break
        if time.monotonic() >= deadline:
            pytest.fail("Local-ID submitted workflow did not finish")
        time.sleep(0.5)
    assert workflow.tasks[0].elements[0].outputs.p2.value == "201"
    assert workflow.tasks[1].elements[0].outputs.p2.value == "202"
    assert not list(config.with_name("config-container").glob(".host-plan-*.json"))


@pytest.mark.integration
@pytest.mark.skipif(os.name != "nt", reason="Windows host with Linux container")
@pytest.mark.parametrize("case", ["success", "failure", "dependencies"])
def test_linux_container_windows_direct_workflow(tmp_path, case):
    """Execute Linux-prepared workflows on Windows and check success, failure and dependencies."""
    image = os.environ.get("HPCFLOW_TEST_CONTAINER_IMAGE")
    if not image:
        pytest.skip("Set HPCFLOW_TEST_CONTAINER_IMAGE to a local image with hpcflow")
    pwsh = shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell 7 is required")
    root = tmp_path / "host mount with spaces"
    root.mkdir()
    source = (
        Path(__file__).parents[3] / "sdk" / "containerisation" / "container_wrapper.ps1"
    )
    wrapper = root / "hpcflow.ps1"
    wrapper.write_text(
        source.read_text().replace("__APP_NAME_", "__HPCFLOW_"),
        encoding="utf-8",
        newline="\n",
    )

    def quote(value):
        return "'" + str(value).replace("'", "''") + "'"

    args = ["demo-workflow", "go", "workflow_1"]
    if case != "success":
        if case == "failure":
            template = {
                "template_components": {
                    "task_schemas": [
                        {
                            "objective": "host_failure",
                            "actions": [
                                {
                                    "environments": {"any": "null_env"},
                                    "commands": [{"command": "exit 7"}],
                                }
                            ],
                        }
                    ],
                },
                "tasks": [{"schema": "host_failure"}],
            }
        else:
            template = {
                "tasks": [
                    {
                        "schema": "test_t1_conditional_OS",
                        "inputs": {"p1": 101 + index},
                        "resources": {"any": {"num_cores": index + 1}},
                    }
                    for index in (0, 1)
                ],
            }
        (root / "template.json").write_text(
            json.dumps(template), encoding="utf-8", newline="\n"
        )
        args = ["go", "template.json", "--format", "json", "--js-parallelism", "False"]
    args += ["--path", ".", "--name", "direct", "--name-no-timestamp"]
    invocation = (
        f"& {quote(wrapper)} -Image {quote(image)} -Machine 'windows-container-test' "
        "-HpcflowArgs @(" + ", ".join(quote(arg) for arg in args) + ")"
    )
    launched = subprocess.run(
        [pwsh, "-NoProfile", "-Command", invocation + "; exit $LASTEXITCODE"],
        cwd=root,
        capture_output=True,
        encoding="utf-8",
        timeout=120,
    )
    assert launched.returncode == 0, launched.stdout + launched.stderr
    workflow_path = root / "direct"
    deadline = time.monotonic() + 120
    while True:
        workflow = hf.Workflow(workflow_path)
        runs = [
            run
            for jobscript in workflow.submissions[0].jobscripts
            for run in jobscript.all_EARs
        ]
        if all(run.end_time is not None for run in runs):
            break
        if time.monotonic() >= deadline:
            logs = "\n".join(
                file.read_text(errors="replace")
                for file in (workflow_path / "artifacts").rglob("*.log")
            )
            pytest.fail("Host run did not complete:\n" + logs)
        time.sleep(0.5)
    assert all(run.exit_code == (7 if case == "failure" else 0) for run in runs)
    assert all(run.run_hostname == socket.gethostname() for run in runs)
    if case != "failure":
        assert workflow.tasks[0].elements[0].outputs.p2.value == "201"
    if case == "dependencies":
        assert len(workflow.submissions[0].jobscripts) == 2
        assert workflow.tasks[1].elements[0].outputs.p2.value == "202"
        assert runs[1].start_time >= runs[0].end_time
    assert all(
        jobscript.process_ID is not None
        for jobscript in workflow.submissions[0].jobscripts
    )


@pytest.mark.integration
@pytest.mark.skipif(os.name != "nt", reason="Windows host with Linux container")
def test_container_show_across_mounts(tmp_path, monkeypatch):
    """Monitor distinct workflows across mounts with host PID checks, TTY rendering and JSON."""
    image = os.environ.get("HPCFLOW_TEST_CONTAINER_IMAGE")
    pwsh = shutil.which("pwsh")
    if not image or not pwsh:
        pytest.skip("Set HPCFLOW_TEST_CONTAINER_IMAGE and install PowerShell 7")
    config = tmp_path / "config"
    monkeypatch.setenv("HPCFLOW_CONFIG_DIR", str(config))
    monkeypatch.setenv("NO_COLOR", "")
    source = (
        Path(__file__).parents[3] / "sdk" / "containerisation" / "container_wrapper.ps1"
    )
    roots = [tmp_path / name for name in ("first", "second", "observer")]
    for root in roots:
        root.mkdir()
        (root / "hpcflow.ps1").write_text(
            source.read_text().replace("__APP_NAME_", "__HPCFLOW_"),
            encoding="utf-8",
            newline="\n",
        )
    template = {
        "template_components": {
            "task_schemas": [
                {
                    "objective": "monitor",
                    "actions": [
                        {
                            "environments": {"any": "null_env"},
                            "commands": [
                                {
                                    "command": "while (-not (Test-Path -LiteralPath 'finish')) { "
                                    "Start-Sleep -Milliseconds 100 }"
                                }
                            ],
                        }
                    ],
                }
            ]
        },
        "tasks": [{"schema": "monitor"}],
    }

    def invoke(root, args, tty=False):
        def quote(value):
            return "'" + str(value).replace("'", "''") + "'"

        command = (
            ("$PSStyle.OutputRendering = 'ANSI'; " if tty else "")
            + f"& {quote(root / 'hpcflow.ps1')} -Image {quote(image)} "
            + ("" if tty else "-Tty:$false ")
            + "-Machine 'monitor-test' -HpcflowArgs @("
            + ", ".join(quote(arg) for arg in args)
            + "); exit $LASTEXITCODE"
        )
        result = subprocess.run(
            [pwsh, "-NoProfile", "-Command", command],
            cwd=root,
            capture_output=True,
            timeout=120,
        )
        stdout = result.stdout.decode("utf-8")
        stderr = result.stderr.decode("utf-8")
        assert result.returncode == 0, stdout + stderr
        return subprocess.CompletedProcess(result.args, result.returncode, stdout, stderr)

    workflow_paths = []
    try:
        for root in roots[:2]:
            (root / "template.json").write_text(
                json.dumps(template),
                encoding="utf-8",
                newline="\n",
            )
            workflow_paths.append(root / "demo")
            invoke(
                root,
                [
                    "go",
                    "template.json",
                    "--format",
                    "json",
                    "--path",
                    ".",
                    "--name",
                    "demo",
                    "--name-no-timestamp",
                ],
            )
        registry = (
            config.with_name("config-container")
            / "data"
            / "hpcflow"
            / "monitor-test"
            / "known_submissions.txt"
        )
        before = registry.read_bytes()
        shown = invoke(roots[2], ["show", "--no-update", "--full"])
        assert shown.stdout.count("demo") == 2
        assert [
            line.split("\u2502")[3].strip()
            for line in shown.stdout.splitlines()
            if "demo" in line
        ] == [JobscriptElementState.running.symbol] * 2
        assert "deleted" not in shown.stdout
        assert "unloadable" not in shown.stdout
        assert registry.read_bytes() == before
        assert before.count(b"/host-workflows/") == 2
        shown_tty = invoke(roots[2], ["show", "--no-update", "--full"], tty=True)
        assert "\x1b[?25l" in shown_tty.stdout
        assert "\x1b[?25h" in shown_tty.stdout
        assert "\r" in shown_tty.stdout
        assert "\x1b[2K" in shown_tty.stdout
        assert re.search(r"\x1b\[(?:[0-9]+;)*[1-9][0-9]*m", shown_tty.stdout)
        assert "\u250c" in shown_tty.stdout
        assert registry.read_bytes() == before
        assert not list(config.with_name("config-container").glob(".host-show-*.json"))
        known = json.loads(invoke(roots[2], ["submission", "get-known", "--json"]).stdout)
        assert len(known) == 2
        assert len({item["workflow_path"] for item in known}) == 2
        assert all(item["active_jobscripts"] for item in known)
    finally:
        for path in workflow_paths:
            if not path.exists():
                continue
            workflow = hf.Workflow(path)
            for job in workflow.submissions[0].jobscripts:
                for run in job.all_EARs:
                    Path(
                        run.get_directory()
                        or Submission.get_tmp_path(workflow.submissions_path, 0)
                    ).joinpath("finish").write_text("", encoding="utf-8")
            context = path / ".HPCFLOW-host-0-0.json"
            gate = path / ".HPCFLOW-host-0-0.ack"
            if context.exists() and not gate.exists():
                record = json.loads(context.read_text())
                subprocess.run(
                    [
                        pwsh,
                        "-NoProfile",
                        "-Command",
                        f"$p = Get-Process -Id {record['process_id']} -ErrorAction SilentlyContinue; "
                        f"if ($p -and $p.StartTime.ToUniversalTime().Ticks -eq {record['start_ticks']}) "
                        "{ Stop-Process -Id $p.Id }",
                    ],
                    check=True,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
    deadline = time.monotonic() + 120
    while not all(
        run.end_time is not None
        for path in workflow_paths
        for job in hf.Workflow(path).submissions[0].jobscripts
        for run in job.all_EARs
    ):
        if time.monotonic() >= deadline:
            pytest.fail("Monitored workflows did not finish")
        time.sleep(0.5)
    shown = invoke(roots[2], ["show"])
    assert shown.stdout.count("demo") == 2
    assert "inactive" in shown.stdout
    assert all(
        line.split("::", 3)[2] == "0" for line in registry.read_text().splitlines()
    )
