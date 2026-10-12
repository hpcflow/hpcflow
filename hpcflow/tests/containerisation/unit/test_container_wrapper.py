from __future__ import annotations

import json
import os
from importlib.resources import files
from pathlib import Path
import shutil
import socket
import subprocess
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from hpcflow.sdk.cli import _make_internal_CLI

WRAPPER = files("hpcflow.sdk.containerisation").joinpath("container_wrapper.ps1")


def ps_quote(value: str | Path) -> str:
    """Quote a literal host argument for the PowerShell test invocation."""
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture
def wrapper_host(tmp_path):
    """Provide fake container and scheduler commands with observable host journals."""
    pwsh = shutil.which("pwsh")
    if not pwsh:
        pytest.skip("PowerShell 7 is required for wrapper tests")
    root = tmp_path / "host mount with spaces"
    root.mkdir()
    workflow = root / "workflow with spaces"
    workflow.mkdir()
    rendered_wrapper = root / "wrapper.ps1"
    rendered_wrapper.write_text(
        WRAPPER.read_text().replace("__APP_NAME_", "__HPCFLOW_"),
        encoding="utf-8",
        newline="\n",
    )
    container = root / "fake_container.ps1"
    container.write_text(
        """
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
$words = @($args)
if ($env:WRAPPER_STARTUP_DELAY -eq '1') {
    Start-Sleep -Milliseconds 500
}
if ($env:WRAPPER_SIGNAL_STARTED -eq '1') {
    $signal = @($words | Where-Object { $_.StartsWith('HPCFLOW_CONTAINER_STARTED_FILE=') })[0]
    $signalPath = Join-Path (
        Join-Path (Split-Path $env:WRAPPER_PLAN) '.hpcflow-container'
    ) $signal.Split('/')[-1]
    [IO.File]::WriteAllText($signalPath, 'started')
    Start-Sleep -Milliseconds 500
    if (-not (Test-Path -LiteralPath $env:WRAPPER_CLEARED_FILE)) {
        throw 'Startup indicator still visible during application initialisation'
    }
}
if ($env:WRAPPER_EARLY_STDERR -eq '1') {
    [Console]::Error.WriteLine('early-container-output')
    Start-Sleep -Milliseconds 500
}
Add-Content -LiteralPath $env:WRAPPER_CONTAINER_LOG -Value (
    ConvertTo-Json -InputObject $words -Compress)
if ($words -contains 'execute-run') {
    Get-Content -Raw -LiteralPath $env:WRAPPER_RUN_PLAN
} elseif ($words -contains 'prepare-host-show') {
    Get-Content -Raw -LiteralPath $env:WRAPPER_MONITOR_PLAN
} elseif ($words -contains 'show-host') {
    if ($env:WRAPPER_FAIL_SHOW -eq '1') { exit 12 }
    if ($env:WRAPPER_LARGE_STDERR -eq '1') {
        [Console]::Error.Write('e' * 262144)
        [Console]::Error.Flush()
    }
    if ($words -contains '--snapshot-file') {
        $snapshotIndex = [Array]::IndexOf($words, '--snapshot-file') + 1
        $snapshotName = $words[$snapshotIndex].Split('/')[-1]
        $snapshotPath = Join-Path (
            Join-Path (Split-Path $env:WRAPPER_PLAN) '.hpcflow-container'
        ) $snapshotName
        $snapshot = Get-Content -Raw -Encoding utf8 -LiteralPath $snapshotPath
    } else {
        $snapshot = [IO.StreamReader]::new(
            [Console]::OpenStandardInput(), [Text.UTF8Encoding]::new($false)
        ).ReadToEnd().Trim()
    }
    Add-Content -LiteralPath $env:WRAPPER_MONITOR_LOG -Value $snapshot
    if ($words -contains '-t') {
        [Console]::Write(
            'first-frame' + [char]13 + [char]27 + '[2Ksecond-frame' +
            [char]13 + [char]27 + '[2K' + [char]0x250c + [char]0x25cf
        )
        exit 0
    }
    'host-show-table'
    if ($words -contains 'FORCE_COLOR=1') {
        [Console]::WriteLine(([char]27).ToString() + '[32mcoloured-status' + [char]27 + '[0m')
        if ($words -notcontains 'TTY_INTERACTIVE=0') {
            'spinner-frame'
        }
    }
    if ($env:WRAPPER_UNICODE -eq '1') {
        [Console]::OutputEncoding = [Text.UTF8Encoding]::new($false)
        [Console]::WriteLine(([char]0x250c).ToString() + [char]0x2500 + [char]0x25cf)
    }
} elseif ($words -contains 'complete-host-run') {
    Add-Content -LiteralPath $env:WRAPPER_COMPLETE_LOG -Value (
        ConvertTo-Json -InputObject $words -Compress)
    if ($env:WRAPPER_FAIL_COMPLETE -eq '1') { exit 11 }
    '{"completed":true}'
} elseif ($words -contains 'record-host-submission') {
    $result = [Console]::In.ReadToEnd() | ConvertFrom-Json -AsHashtable
    Add-Content -LiteralPath $env:WRAPPER_ACK_LOG -Value (
        ConvertTo-Json -InputObject $result -Compress -Depth 30)
    if ($env:WRAPPER_FAIL_ACK -eq '1') { exit 8 }
    if ($env:WRAPPER_BAD_ACK -eq '1') { '{"recorded":"true"}'; exit 0 }
    '{"recorded":true}'
} elseif ($words -contains '--containerised') {
    if ($env:WRAPPER_FAIL_PREPARE -eq '1') { exit 9 }
    $planOutput = @($words | Where-Object { $_.StartsWith('HPCFLOW_CONTAINER_PLAN_FILE=') })
    if ($planOutput.Count) {
        $planPath = Join-Path (
            Join-Path (Split-Path $env:WRAPPER_PLAN) '.hpcflow-container'
        ) $planOutput[0].Split('/')[-1]
        if ($env:WRAPPER_MISSING_PLAN -ne '1') {
            Copy-Item -LiteralPath $env:WRAPPER_PLAN -Destination $planPath
        }
        [Console]::Write('prepare-frame' + [char]13 + [char]27 + '[2K')
    } else {
        Get-Content -Raw -LiteralPath $env:WRAPPER_PLAN
    }
} else {
    'verbatim-output'
    exit ([int]$env:WRAPPER_PASS_EXIT)
}
""",
        encoding="utf-8",
        newline="\n",
    )
    scheduler = root / "fake_scheduler.ps1"
    scheduler.write_text(
        """
$words = @($args)
Add-Content -LiteralPath $env:WRAPPER_LAUNCH_LOG -Value (
    ConvertTo-Json -InputObject @{
        arguments = $words
        cwd = (Get-Location).ProviderPath
    } -Compress)
if ($env:WRAPPER_FAIL_LAUNCH -eq '1') { exit 7 }
if ($env:WRAPPER_FAIL_SECOND -eq '1' -and @(Get-Content $env:WRAPPER_LAUNCH_LOG).Count -eq 2) {
    exit 7
}
if ($env:WRAPPER_STDERR -eq '1') { [Console]::Error.WriteLine('scheduler warning') }
$count = @(Get-Content -LiteralPath $env:WRAPPER_LAUNCH_LOG).Count
if ($env:WRAPPER_BAD_OUTPUT -eq '1') { 'unexpected output' }
elseif ($env:WRAPPER_SCHEDULER -eq 'sge') { "$($count + 100).1-4:1" }
else { "$($count + 100);cluster" }
""",
        encoding="utf-8",
        newline="\n",
    )
    for index in (0, 1):
        (workflow / f"job {index}.ps1").write_text(
            "# prepared script\n", encoding="utf-8", newline="\n"
        )
    jobs = [
        {
            "submission_index": 0,
            "jobscript_index": index,
            "path": f"job {index}.ps1",
            "scheduler": "slurm",
            "shell": "powershell",
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
                pwsh,
                "-NoProfile",
                "-File",
                str(scheduler),
                *(["afterany:__HPCFLOW_JOB_0_0__"] if index else []),
                "__HPCFLOW_JOBSCRIPT_PATH__",
                "argument with spaces",
                "",
                'quote"and;literal',
            ],
        }
        for index in (0, 1)
    ]
    plan = {
        "schema_version": 1,
        "workflow_id": "workflow-id",
        "workflow_path": "/work/workflow with spaces",
        "jobscripts": jobs,
    }
    plan_file = root / "plan.json"
    env = {
        **os.environ,
        "HPCFLOW_CONFIG_DIR": str(root / ".hpcflow"),
        "MATFLOW_CONFIG_DIR": str(root / ".matflow"),
        "WRAPPER_PLAN": str(plan_file),
        "WRAPPER_CONTAINER_LOG": str(root / "container.jsonl"),
        "WRAPPER_ACK_LOG": str(root / "ack.jsonl"),
        "WRAPPER_LAUNCH_LOG": str(root / "launch.jsonl"),
        "WRAPPER_PASS_EXIT": "0",
        "WRAPPER_COMPLETE_LOG": str(root / "complete.jsonl"),
        "WRAPPER_MONITOR_LOG": str(root / "monitor.jsonl"),
    }

    def run(
        args=None,
        resume=False,
        wrapper=WRAPPER,
        use_image=True,
        positional=False,
        machine="host-machine",
        legacy_encoding=False,
        output_rendering="PlainText",
        plain_output_pipeline=False,
        tty=False,
        raw_output=False,
        notebook_port=None,
        **overrides,
    ):
        if wrapper == WRAPPER:
            wrapper = rendered_wrapper
        plan_file.write_text(json.dumps(plan), encoding="utf-8", newline="\n")
        invocation = f"& {ps_quote(wrapper)} " + (
            "-Image 'hpcflow:test' " if use_image else ""
        ) + (
            f"-Machine {ps_quote(machine)} " if machine is not None else ""
        ) + f"-ContainerCommand @({ps_quote(pwsh)}, '-NoProfile', '-File', " f"{ps_quote(container)}) " + (
            "" if tty is None else ("-Tty " if tty else "-Tty:$false ")
        )
        if notebook_port is not None:
            invocation += f"-NotebookPort {notebook_port} "
        if resume:
            invocation += "-ResumeResult " + ps_quote(
                workflow / ".hpcflow-host-submission.json"
            )
        elif positional:
            invocation += " ".join(
                ps_quote(arg) for arg in (args or ["go", "template.yaml"])
            )
        else:
            invocation += (
                "-HpcflowArgs @("
                + ", ".join(ps_quote(arg) for arg in (args or ["go", "template.yaml"]))
                + ")"
            )
        command = invocation + "; exit $LASTEXITCODE"
        if plain_output_pipeline:
            command = (
                "$out = " + invocation + " | Out-String; $code = $LASTEXITCODE; "
                "$PSStyle.OutputRendering = 'PlainText'; $out; exit $code"
            )
        if legacy_encoding:
            command = (
                "[Console]::OutputEncoding = [Text.Encoding]::GetEncoding(437); "
                "$OutputEncoding = [Text.Encoding]::GetEncoding(437); "
                "$out = " + invocation + "; $code = $LASTEXITCODE; "
                "if ([Console]::OutputEncoding.CodePage -ne 437 -or "
                "$OutputEncoding.CodePage -ne 437) { throw 'Encoding not restored' }; "
                "[Console]::OutputEncoding = [Text.UTF8Encoding]::new($false); "
                "$out; exit $code"
            )
        command = f"$PSStyle.OutputRendering = {ps_quote(output_rendering)}; " + command
        return subprocess.run(
            [pwsh, "-NoProfile", "-Command", command],
            cwd=root,
            env={**env, **overrides},
            capture_output=True,
            encoding=None if raw_output else "utf-8",
            timeout=30,
        )

    def records(name):
        path = root / f"{name}.jsonl"
        return (
            [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
            if (path.exists())
            else []
        )

    return run, plan, workflow, records


def test_wrapper_show_utf8_with_legacy_console_encoding(wrapper_host):
    """Preserve Unicode display and snapshot text under a legacy host console code page."""
    run, _, workflow, records = wrapper_host
    plan_file = workflow.parent / "unicode-monitor.json"
    plan_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "jobs": [
                    {
                        "workflow_path": "/host-workflows/test/caf\u00e9",
                        "workflow_id": "unicode",
                        "submission_index": 0,
                        "jobscript_index": 0,
                        "scheduler": "slurm",
                        "query": [
                            shutil.which("pwsh"),
                            "-NoProfile",
                            "-Command",
                            "'123 RUNNING'",
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        ["show"],
        legacy_encoding=True,
        WRAPPER_UNICODE="1",
        WRAPPER_MONITOR_PLAN=str(plan_file),
    )
    assert result.returncode == 0, result.stderr
    assert "\u250c\u2500\u25cf" in result.stdout
    assert records("monitor")[0]["jobs"][0]["workflow_path"].endswith("caf\u00e9")


@pytest.mark.parametrize(
    ("rendering", "no_color", "colour"),
    [("ANSI", "", True), ("PlainText", "", False), ("ANSI", "1", False)],
)
def test_wrapper_show_colours(wrapper_host, rendering, no_color, colour):
    """Enable terminal colours only when output rendering and NO_COLOR permit them."""
    run, _, workflow, records = wrapper_host
    plan_file = workflow.parent / "empty-monitor.json"
    plan_file.write_text('{"schema_version":1,"jobs":[]}', encoding="utf-8", newline="\n")
    result = run(
        ["show"],
        output_rendering=rendering,
        NO_COLOR=no_color,
        WRAPPER_MONITOR_PLAN=str(plan_file),
    )
    assert result.returncode == 0, result.stderr
    assert ("\x1b[32mcoloured-status\x1b[0m" in result.stdout) == colour
    calls = records("container")
    assert "FORCE_COLOR=1" not in calls[0]
    assert ("FORCE_COLOR=1" in calls[1]) == colour
    assert "spinner-frame" not in result.stdout


def test_wrapper_display_bypasses_plaintext_pipeline(wrapper_host):
    """Keep forced ANSI display intact when PowerShell would otherwise strip escape codes."""
    run, _, workflow, _ = wrapper_host
    plan_file = workflow.parent / "empty-monitor.json"
    plan_file.write_text('{"schema_version":1,"jobs":[]}', encoding="utf-8", newline="\n")
    result = run(
        ["show"],
        output_rendering="ANSI",
        plain_output_pipeline=True,
        NO_COLOR="",
        WRAPPER_MONITOR_PLAN=str(plan_file),
    )
    assert result.returncode == 0, result.stderr
    assert "\x1b[32mcoloured-status\x1b[0m" in result.stdout
    assert "spinner-frame" not in result.stdout


@pytest.mark.parametrize("fail", [False, True])
@pytest.mark.parametrize("tty", [True, None])
def test_wrapper_tty_preserves_frames_and_cleans_snapshot(wrapper_host, fail, tty):
    """Stream TTY bytes unchanged and remove temporary snapshots after success or failure."""
    run, _, workflow, records = wrapper_host
    plan_file = workflow.parent / "empty-monitor.json"
    plan_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "jobs": [
                    {
                        "workflow_path": "/host-workflows/test/caf\u00e9",
                        "workflow_id": "unicode",
                        "submission_index": 0,
                        "jobscript_index": 0,
                        "scheduler": "slurm",
                        "query": [
                            shutil.which("pwsh"),
                            "-NoProfile",
                            "-Command",
                            "'123 RUNNING'",
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        ["show"],
        tty=tty,
        raw_output=True,
        WRAPPER_MONITOR_PLAN=str(plan_file),
        WRAPPER_FAIL_SHOW="1" if fail else "",
    )
    assert result.returncode == (1 if fail else 0), result.stderr
    calls = records("container")
    assert "-t" not in calls[0]
    assert "-t" in calls[1]
    assert "-i" not in calls[1]
    assert "--snapshot-file" in calls[1]
    assert "TTY_INTERACTIVE=1" in calls[1]
    assert not list((workflow.parent / ".hpcflow-container").glob(".host-show-*.json"))
    if not fail:
        assert result.stdout == (
            "first-frame\r\x1b[2Ksecond-frame\r\x1b[2K\u250c\u25cf"
        ).encode("utf-8")
        assert records("monitor")[0]["jobs"][0]["workflow_path"].endswith("caf\u00e9")


def test_wrapper_tty_does_not_apply_to_json(wrapper_host):
    """Keep JSON passthrough non-TTY even when TTY display is explicitly requested."""
    run, _, _, records = wrapper_host
    result = run(["config", "get", "--json"], tty=True)
    assert result.returncode == 0, result.stderr
    assert "-t" not in records("container")[0]


@pytest.mark.parametrize(
    "args",
    [
        ["go", "template.yaml"],
        ["demo-workflow", "go", "workflow_1"],
        ["workflow", "workflow with spaces", "submit"],
    ],
)
def test_wrapper_submission_defaults_to_tty(wrapper_host, args):
    """Use TTY progress and a separate plan file for all default submission entry points."""
    run, _, workflow, records = wrapper_host
    result = run(args, tty=None)
    assert result.returncode == 0, result.stderr
    calls = records("container")
    assert "-t" in calls[0]
    assert any(word.startswith("HPCFLOW_CONTAINER_PLAN_FILE=") for word in calls[0])
    assert "\x1b[2K" in result.stdout
    assert len(records("ack")) == 2
    assert all("-t" not in call for call in calls[1:])
    assert not list((workflow.parent / ".hpcflow-container").glob(".host-plan-*.json"))


def test_wrapper_submission_tty_failure_does_not_launch(wrapper_host):
    """Stop before host launch when TTY container preparation fails."""
    run, _, workflow, records = wrapper_host
    result = run(tty=None, WRAPPER_FAIL_PREPARE="1")
    assert result.returncode == 1
    assert not records("launch")
    assert not list((workflow.parent / ".hpcflow-container").glob(".host-plan-*.json"))


def test_wrapper_submission_tty_missing_plan_does_not_launch(wrapper_host):
    """Reject successful-looking TTY preparation that did not produce a submission plan."""
    run, _, _, records = wrapper_host
    result = run(tty=None, WRAPPER_MISSING_PLAN="1")
    assert result.returncode == 1
    assert "did not write a host submission plan" in result.stderr
    assert not records("launch")


@pytest.mark.parametrize(
    "args",
    [
        ["workflow", "0", "--ref-type", "id", "submit"],
        ["workflow", "--ref-type", "id", "0", "submit"],
        ["workflow", "--ref-type=id", "0", "submit"],
    ],
)
def test_wrapper_workflow_reference_option_order(wrapper_host, args):
    """Normalise workflow reference options before dispatching to Click."""
    run, _, _, records = wrapper_host
    result = run(args)
    assert result.returncode == 0, result.stderr
    call = records("container")[0]
    index = call.index("workflow")
    expected = (
        ["workflow", "--ref-type=id", "0", "submit"]
        if "--ref-type=id" in args
        else ["workflow", "--ref-type", "id", "0", "submit"]
    )
    assert call[index : index + len(expected)] == expected


def test_wrapper_registered_mounts_available_to_workflow_commands(wrapper_host):
    """Mount previously registered roots when accessing workflows by local ID."""
    run, _, workflow, records = wrapper_host
    assert run().returncode == 0
    result = run(["workflow", "0", "show-all-status"])
    assert result.returncode == 0, result.stderr
    mounts = list((workflow.parent / ".hpcflow-container" / "host-mounts").glob("*.json"))
    assert len(mounts) == 1
    assert any(
        word.endswith(f"target=/host-workflows/{mounts[0].stem}")
        for word in records("container")[-1]
    )


def test_wrapper_submits_from_registered_workflow_alias(wrapper_host):
    """Map a persistent workflow alias back to host files during submission."""
    run, plan, workflow, records = wrapper_host
    assert run().returncode == 0
    mount = next((workflow.parent / ".hpcflow-container" / "host-mounts").glob("*.json"))
    plan["workflow_path"] = f"/host-workflows/{mount.stem}/{workflow.name}"
    result = run(["workflow", "0", "--ref-type=id", "submit"])
    assert result.returncode == 0, result.stderr
    assert len(records("ack")) == 4


def test_wrapper_json_passthrough_disables_colours(wrapper_host):
    """Exclude colour controls from machine-readable passthrough output."""
    run, _, _, records = wrapper_host
    result = run(["config", "get", "--json"], output_rendering="ANSI")
    assert result.returncode == 0, result.stderr
    assert "FORCE_COLOR=1" not in records("container")[0]
    assert "NO_COLOR=1" in records("container")[0]


@pytest.mark.parametrize("exit_code", [0, 7])
def test_wrapper_restores_encoding_after_passthrough(wrapper_host, exit_code):
    """Restore the caller's console encoding after successful or failed passthrough commands."""
    run, _, _, _ = wrapper_host
    result = run(["--version"], legacy_encoding=True, WRAPPER_PASS_EXIT=str(exit_code))
    assert result.returncode == exit_code, result.stderr
    assert "verbatim-output" in result.stdout


def test_wrapper_restores_encoding_after_container_failure(wrapper_host):
    """Restore console encoding even when container snapshot rendering fails."""
    run, _, workflow, _ = wrapper_host
    plan_file = workflow.parent / "empty-monitor.json"
    plan_file.write_text('{"schema_version":1,"jobs":[]}', encoding="utf-8", newline="\n")
    result = run(
        ["show"],
        legacy_encoding=True,
        WRAPPER_MONITOR_PLAN=str(plan_file),
        WRAPPER_FAIL_SHOW="1",
    )
    assert result.returncode == 1
    assert "Container command failed with exit code 12" in result.stderr
    assert "Encoding not restored" not in result.stderr


@pytest.mark.skipif(os.name != "nt", reason="Windows host process identity")
@pytest.mark.parametrize("reused_pid", [False, True])
def test_wrapper_show_host_process_identity(wrapper_host, reused_pid):
    """Distinguish a live host launcher from a reused PID using process creation time."""
    run, _, workflow, records = wrapper_host
    launched = run()
    assert launched.returncode == 0, launched.stderr
    root = workflow.parent
    mount_files = list((root / ".hpcflow-container" / "host-mounts").glob("*.json"))
    assert len(mount_files) == 1
    mount_id = mount_files[0].stem
    ticks = subprocess.check_output(
        [
            shutil.which("pwsh"),
            "-NoProfile",
            "-Command",
            f"(Get-Process -Id {os.getpid()}).StartTime.ToUniversalTime().Ticks",
        ],
        text=True,
    ).strip()
    context = workflow / ".HPCFLOW-host-0-0.json"
    context.write_text(
        json.dumps(
            {
                "workflow_id": "workflow-id",
                "process_id": os.getpid(),
                "start_ticks": int(ticks) + int(reused_pid),
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    plan_file = root / "monitor-plan.json"
    plan_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "jobs": [
                    {
                        "workflow_path": f"/host-workflows/{mount_id}/workflow with spaces",
                        "workflow_id": "workflow-id",
                        "submission_index": 0,
                        "jobscript_index": 0,
                        "scheduler": "direct",
                        "process_id": os.getpid(),
                    }
                ],
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        ["show", "--no-update", "--full", "-r", "7"], WRAPPER_MONITOR_PLAN=str(plan_file)
    )
    assert result.returncode == 0, result.stderr
    assert "host-show-table" in result.stdout
    assert records("monitor")[0]["jobs"][0]["active"] is (not reused_pid)
    args = records("container")[-1]
    assert args[-5:] == ["show-host", "--no-update", "--full", "-r", "7"]
    assert f"type=bind,source={root},target=/host-workflows/{mount_id}" in args
    assert ["--with-config", "machine", "host-machine"] == args[
        args.index("hpcflow:test") + 1 : args.index("hpcflow:test") + 4
    ]


@pytest.mark.parametrize("failed", [False, True])
def test_wrapper_show_scheduler_failure_is_not_inactivity(wrapper_host, failed):
    """Report scheduler query errors rather than treating them as inactive jobs."""
    run, _, workflow, records = wrapper_host
    root = workflow.parent
    plan_file = root / "monitor-plan.json"
    query = root / "query.ps1"
    query.write_text(
        "'123 RUNNING'\n" + ("exit 7\n" if failed else ""),
        encoding="utf-8",
        newline="\n",
    )
    plan_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "jobs": [
                    {
                        "workflow_path": "/host-workflows/test/workflow",
                        "workflow_id": "workflow-id",
                        "submission_index": 0,
                        "jobscript_index": index,
                        "scheduler": "slurm",
                        "query": [
                            shutil.which("pwsh"),
                            "-NoProfile",
                            "-File",
                            str(query),
                        ],
                    }
                    for index in (0, 1)
                ],
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(["show"], WRAPPER_MONITOR_PLAN=str(plan_file))
    assert (result.returncode != 0) is failed
    if failed:
        assert "monitoring failed" in result.stderr
        assert not records("monitor")
    else:
        assert [job["stdout"] for job in records("monitor")[0]["jobs"]] == [
            "123 RUNNING"
        ] * 2


@pytest.mark.parametrize("animate", [False, True])
@pytest.mark.parametrize("early_stderr", [False, True])
def test_wrapper_startup_indicator(wrapper_host, animate, early_stderr):
    """Show startup on stderr, clear animation before output, and preserve stdout."""
    run, _, workflow, _ = wrapper_host
    wrapper = workflow.parent / "startup-wrapper.ps1"
    # Exercise interactive rendering under subprocess capture without requiring
    # a real terminal; production uses terminal capability and redirection checks.
    wrapper.write_text(
        WRAPPER.read_text()
        .replace("__APP_NAME_", "__HPCFLOW_")
        .replace(
            "$animate = $Host.UI.SupportsVirtualTerminal -and -not [Console]::IsErrorRedirected",
            "$animate = $" + str(animate).lower(),
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        ["--version"],
        wrapper=wrapper,
        WRAPPER_STARTUP_DELAY="1",
        WRAPPER_EARLY_STDERR="1" if early_stderr else "0",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "verbatim-output\n"
    assert "Starting container..." in result.stderr
    if animate:
        assert "| Starting container..." in result.stderr
        assert "/ Starting container..." in result.stderr
        assert "\x1b[2K" in result.stderr
        if early_stderr:
            assert result.stderr.index("\x1b[2K") < result.stderr.index(
                "early-container-output"
            )
            assert (
                "Starting container..."
                not in result.stderr.split("early-container-output")[1]
            )
    else:
        assert result.stderr.count("Starting container...") == 1
        assert "\x1b" not in result.stderr


@pytest.mark.parametrize("command", [["--version"], ["notebook"]])
@pytest.mark.parametrize("exit_code", ["0", "9"])
def test_wrapper_startup_signal_clears_before_output(wrapper_host, command, exit_code):
    """Clear startup during silent application initialisation and clean the signal file."""
    run, _, workflow, records = wrapper_host
    wrapper = workflow.parent / "signal-wrapper.ps1"
    cleared = workflow.parent / "indicator-cleared"
    wrapper.write_text(
        WRAPPER.read_text()
        .replace("__APP_NAME_", "__HPCFLOW_")
        .replace(
            "$animate = $Host.UI.SupportsVirtualTerminal -and -not [Console]::IsErrorRedirected",
            "$animate = $true",
        )
        .replace(
            "[Console]::Error.Write(\"`r\" + [char]27 + '[2K')",
            "[Console]::Error.Write(\"`r\" + [char]27 + '[2K'); "
            "[IO.File]::WriteAllText($env:WRAPPER_CLEARED_FILE, 'cleared')",
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        command,
        wrapper=wrapper,
        WRAPPER_STARTUP_DELAY="1",
        WRAPPER_SIGNAL_STARTED="1",
        WRAPPER_CLEARED_FILE=str(cleared),
        WRAPPER_PASS_EXIT=exit_code,
    )
    assert result.returncode == int(exit_code), result.stderr
    assert cleared.exists()
    assert "\x1b[2K" in result.stderr
    assert not list((workflow.parent / ".hpcflow-container").glob(".host-started-*"))
    assert any(
        arg.startswith("HPCFLOW_CONTAINER_STARTED_FILE=")
        for arg in records("container")[0]
    )


def test_wrapper_large_snapshot_with_early_stderr(wrapper_host):
    """Drain stderr while writing a snapshot larger than the subprocess pipe buffers."""
    run, _, workflow, records = wrapper_host
    plan_file = workflow.parent / "large-monitor.json"
    plan_file.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "jobs": [
                    {
                        "workflow_path": "/work/workflow with spaces",
                        "workflow_id": "large-output",
                        "submission_index": 0,
                        "jobscript_index": 0,
                        "scheduler": "slurm",
                        "query": [
                            shutil.which("pwsh"),
                            "-NoProfile",
                            "-Command",
                            "'x' * 262144",
                        ],
                    }
                ],
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        ["show"],
        WRAPPER_MONITOR_PLAN=str(plan_file),
        WRAPPER_LARGE_STDERR="1",
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "host-show-table\n"
    assert result.stderr.count("e") >= 262144
    assert records("monitor")[0]["jobs"][0]["stdout"].strip() == "x" * 262144


@pytest.mark.parametrize("port", [8888, 8889])
def test_wrapper_notebook_server(wrapper_host, port):
    """Start an authenticated localhost notebook server with persistent workflow mounts."""
    run, _, _, records = wrapper_host
    result = run(["notebook"], notebook_port=port)
    assert result.returncode == 0, result.stderr
    command = records("container")[0]
    assert command[command.index("--publish") + 1] == f"127.0.0.1:{port}:8888"
    assert command[command.index("--entrypoint") + 1] == "hpcflow-container-entrypoint"
    image_index = command.index("hpcflow:test")
    assert command[image_index + 1 :] == [
        "python",
        "-m",
        "jupyterlab",
        "--ServerApp.ip=0.0.0.0",
        "--ServerApp.port=8888",
        "--ServerApp.port_retries=0",
        "--ServerApp.open_browser=False",
        "--ServerApp.allow_root=True",
        "--ServerApp.root_dir=/work",
        "--ServerApp.custom_display_url",
        f"http://127.0.0.1:{port}",
    ]
    assert any("target=/work" in arg for arg in command)
    assert any("target=/config/.hpcflow-container" in arg for arg in command)
    assert not records("launch")
    assert not records("ack")


@pytest.mark.parametrize("port", [0, 65536])
def test_wrapper_notebook_invalid_port(wrapper_host, port):
    """Reject invalid host ports before starting a container."""
    run, _, _, records = wrapper_host
    result = run(["notebook"], notebook_port=port)
    assert "Cannot validate argument on parameter 'NotebookPort'" in result.stderr
    assert not records("container")


def test_wrapper_notebook_rejects_server_arguments(wrapper_host):
    """Prevent forwarded server flags from disabling notebook authentication."""
    run, _, _, records = wrapper_host
    result = run(["notebook", "--IdentityProvider.token="])
    assert result.returncode != 0
    assert "Use notebook without" in result.stderr
    assert not records("container")


def test_wrapper_notebook_failure(wrapper_host):
    """Propagate startup failures such as an unavailable host port or missing JupyterLab."""
    run, _, _, _ = wrapper_host
    result = run(["notebook"], WRAPPER_PASS_EXIT="9")
    assert result.returncode == 9


def test_wrapper_defaults_machine_to_host_hostname(wrapper_host):
    """Use the host hostname when no machine override is supplied."""
    run, _, _, records = wrapper_host
    result = run(["demo-workflow", "go", "workflow_1"], positional=True, machine=None)
    assert result.returncode == 0, result.stderr
    hostname = socket.gethostname()
    assert all(ack["submit_machine"] == hostname for ack in records("ack"))
    preparation = records("container")[0]
    command_index = preparation.index("demo-workflow")
    machine_index = preparation.index("machine")
    assert preparation[machine_index - 1 : machine_index + 2] == [
        "--with-config",
        "machine",
        hostname,
    ]
    assert machine_index < command_index


def test_wrapper_rejects_explicit_empty_machine(wrapper_host):
    """Reject an explicitly empty machine instead of silently selecting a default."""
    run, _, _, records = wrapper_host
    result = run(machine="")
    assert result.returncode != 0
    assert "Machine must not be empty" in result.stderr
    assert not records("container")
    assert not records("launch")


@pytest.mark.skipif(os.name != "nt", reason="Windows host configuration paths")
@pytest.mark.parametrize("app_name", ["hpcflow", "matflow"])
@pytest.mark.parametrize("location", ["unset", "outside", "inside", "container"])
def test_wrapper_container_config_directory(wrapper_host, app_name, location):
    """Resolve application-specific persistent config, cache and data mounts from host settings."""
    run, _, workflow, records = wrapper_host
    root = workflow.parent
    wrapper = root / f"{app_name}.ps1"
    wrapper.write_text(
        WRAPPER.read_text().replace("__APP_NAME_", f"__{app_name.upper()}_"),
        encoding="utf-8",
        newline="\n",
    )
    config_dir = {
        "unset": "",
        "outside": str(root.parent / f".{app_name}"),
        "inside": str(root / "custom config"),
        "container": "/work/custom config",
    }[location]
    result = run(
        ["show-config"],
        wrapper=wrapper,
        **{f"{app_name.upper()}_CONFIG_DIR": config_dir},
        USERPROFILE=str(root),
        HOME=str(root),
    )
    assert result.returncode == 0, result.stderr
    args = records("container")[0]
    env_values = [args[index + 1] for index, arg in enumerate(args) if arg == "--env"]
    expected = (
        "/work/custom config"
        if location == "container"
        else (f"/config/.{app_name}-container")
    )
    assert [
        value
        for value in env_values
        if value.startswith(f"{app_name.upper()}_CONFIG_DIR=")
    ] == [f"{app_name.upper()}_CONFIG_DIR={expected}"]
    assert f"XDG_CACHE_HOME={expected}/cache" in env_values
    assert f"XDG_DATA_HOME={expected}/data" in env_values
    mounts = [args[index + 1] for index, arg in enumerate(args) if arg == "--mount"]
    if location == "container":
        assert len(mounts) == 1
    else:
        host_config = (
            root / f".{app_name}-container"
            if location == "unset"
            else Path(config_dir + "-container")
        )
        assert host_config.is_dir()
        assert mounts[1] == f"type=bind,source={host_config},target={expected}"
    assert args.count("shells.powershell.defaults.executable") == 1


@pytest.mark.parametrize(
    "args",
    [
        [
            "demo-workflow",
            "go",
            "workflow_1",
            "--path",
            ".",
            "--name",
            "name with spaces",
        ],
        ["go", "template.yaml", "--modify-js"],
        ["workflow", "workflow with spaces", "submit", "--name-no-timestamp"],
    ],
)
def test_wrapper_positional_submission(wrapper_host, args):
    """Recognise documented command-first positional submission forms."""
    run, _, _, records = wrapper_host
    result = run(args, positional=True)
    assert result.returncode == 0, result.stderr
    prepared_args = records("container")[0]
    command_index = prepared_args.index(args[0])
    assert prepared_args[command_index:] == [*args, "--containerised"]
    assert len(records("ack")) == 2


def test_wrapper_positional_passthrough(wrapper_host):
    """Preserve positional arguments for non-submission commands."""
    run, _, _, records = wrapper_host
    args = ["workflow", "workflow with spaces", "get-param", "1"]
    result = run(args, positional=True, WRAPPER_PASS_EXIT="6")
    assert result.returncode == 6, result.stderr
    assert records("container")[0][-len(args) :] == args
    assert not records("launch")


@pytest.mark.skipif(os.name != "nt", reason="Windows direct host launcher")
@pytest.mark.parametrize("fail_ack", [False, True])
def test_wrapper_direct_launch_and_dependencies(wrapper_host, fail_ack):
    """Launch gated direct host jobs and preserve dependency identities through acknowledgement."""
    import psutil

    run, plan, workflow, records = wrapper_host
    for index, job in enumerate(plan["jobscripts"]):
        job["scheduler"] = "direct"
        job["stdout_path"] = f"stdout {index}.log"
        job["stderr_path"] = f"stderr {index}.log"
        job["submit_command"] = [
            shutil.which("pwsh"),
            "-NoProfile",
            "-File",
            "__HPCFLOW_JOBSCRIPT_PATH__",
        ]
        (workflow / job["path"]).write_text(
            "Start-Sleep -Milliseconds 1500\n"
            + (
                "if (-not (Test-Path -LiteralPath 'done 0')) { throw 'Dependency ran early' }\n"
                if index
                else ""
            )
            + f"Set-Content -LiteralPath 'done {index}' -Value 'done'\n",
            encoding="utf-8",
            newline="\n",
        )
    result = run(WRAPPER_FAIL_ACK="1" if fail_ack else "0")
    if fail_ack:
        assert result.returncode != 0
        first_pid = records("ack")[0]["process_ID"]
        try:
            assert not (workflow / "done 0").exists()
            recovered = run(resume=True)
            assert recovered.returncode == 0, recovered.stderr
            plan["jobscripts"] = plan["jobscripts"][1:]
            plan["jobscripts"][0]["dependencies"][0]["reference"] = str(first_pid)
            result = run()
        finally:
            if result.returncode != 0:
                process = psutil.Process(first_pid)
                for child in process.children(recursive=True):
                    child.kill()
                process.kill()
    assert result.returncode == 0, result.stderr
    acknowledgements = list(
        {item["process_ID"]: item for item in records("ack")}.values()
    )
    assert len(acknowledgements) == 2
    assert all(item["process_ID"] > 0 for item in acknowledgements)
    assert all("scheduler_job_ID" not in item for item in acknowledgements)
    for item in acknowledgements:
        try:
            psutil.Process(item["process_ID"]).wait(timeout=15)
        except psutil.NoSuchProcess:
            pass
    assert (workflow / "done 0").exists()
    assert (workflow / "done 1").exists()
    assert (workflow / ".hpcflow.ps1").is_file()
    assert not (workflow / ".hpcflow-host-submission.json").exists()
    assert all(
        not (workflow / f"stderr {index}.log").read_text().strip() for index in (0, 1)
    )


@pytest.mark.parametrize("exit_code,fail_complete", [(0, False), (7, False), (0, True)])
@pytest.mark.parametrize("cached", [False, True])
def test_wrapper_host_command_execution(wrapper_host, exit_code, fail_complete, cached):
    """Execute host run plans with mapped paths and acknowledge their actual exit codes."""
    run, _, workflow, records = wrapper_host
    cache = workflow.parent / ".hpcflow-container" / "cache" / "hpcflow"
    if cached:
        cache.mkdir(parents=True)
    command_file = (cache if cached else workflow) / "command with spaces.ps1"
    container_file = (
        "/config/.hpcflow-container/cache/hpcflow/command with spaces.ps1"
        if cached
        else "/work/workflow with spaces/command with spaces.ps1"
    )
    command_file.write_text(
        "$env:HPCFLOW_RUN_ID | Set-Content -LiteralPath 'host-output'\n"
        "$env:HPCFLOW_TEST_CACHED_FILE | Set-Content -LiteralPath 'mapped-file'\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
        newline="\n",
    )
    run_plan = workflow.parent / "run-plan.json"
    run_plan.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "workflow_id": "workflow-id",
                "command": [
                    shutil.which("pwsh"),
                    "-NoProfile",
                    "-File",
                    container_file,
                ],
                "working_directory": "/work/workflow with spaces",
                "environment": {
                    "HPCFLOW_RUN_ID": "42",
                    "HPCFLOW_TEST_CACHED_FILE": container_file,
                },
            }
        ),
        encoding="utf-8",
        newline="\n",
    )
    result = run(
        [
            "internal",
            "workflow",
            "/work/workflow with spaces",
            "execute-run",
            "0",
            "0",
            "0",
            "0",
            "42",
        ],
        WRAPPER_RUN_PLAN=str(run_plan),
        WRAPPER_FAIL_COMPLETE="1" if fail_complete else "0",
    )
    assert (result.returncode == 0) == (not fail_complete), result.stderr
    assert (workflow / "host-output").read_text().strip() == "42"
    assert (workflow / "mapped-file").read_text().strip() == str(command_file)
    assert records("complete")[0][-2:] == ["--", str(exit_code)]
    assert not records("launch")
    journals = list(workflow.glob(".HPCFLOW-run-*.json"))
    if fail_complete:
        assert len(journals) == 1
        journal = json.loads(journals[0].read_text())
        assert journal["state"] == "executed"
        assert journal["exit_code"] == exit_code
        assert "environment" not in journal
        retry = run(journal["complete_args"])
        assert retry.returncode == 0, retry.stderr
    else:
        assert not journals


@pytest.mark.parametrize(
    "args",
    [
        ["go", "template.yaml"],
        ["demo-workflow", "go", "workflow_1"],
        ["workflow", "workflow with spaces", "submit"],
        ["workflow", "workflow with spaces", "--ref-type", "path", "submit"],
    ],
)
@pytest.mark.parametrize("scheduler", ["slurm", "sge"])
def test_wrapper_submission(wrapper_host, args, scheduler):
    """Submit prepared scheduler scripts with substituted paths and dependency references."""
    run, plan, workflow, records = wrapper_host
    for job in plan["jobscripts"]:
        job["scheduler"] = scheduler
    result = run(args, WRAPPER_SCHEDULER=scheduler)
    assert result.returncode == 0, result.stderr
    launches = records("launch")
    assert len(launches) == 2
    assert all(launch["cwd"] == str(workflow) for launch in launches)
    assert launches[1]["arguments"][0] == "afterany:101"
    assert launches[0]["arguments"] == [
        str(workflow / "job 0.ps1"),
        "argument with spaces",
        "",
        'quote"and;literal',
    ]
    acknowledgements = records("ack")
    assert [ack["scheduler_job_ID"] for ack in acknowledgements] == ["101", "102"]
    assert all(ack["submit_machine"] == "host-machine" for ack in acknowledgements)
    assert len(records("container")) == 3
    assert records("container")[0][-1] == "--containerised"
    assert not (workflow / ".hpcflow-host-submission.json").exists()


def test_wrapper_passthrough(wrapper_host):
    """Preserve passthrough arguments, output and native exit status."""
    run, _, _, records = wrapper_host
    result = run(["--version"], WRAPPER_PASS_EXIT="3")
    assert result.returncode == 3
    assert result.stdout.strip() == "verbatim-output"
    assert not records("launch")
    assert records("container")[0][-1] == "--version"


def test_wrapper_ack_failure_and_recovery(wrapper_host):
    """Recover a failed acknowledgement without launching an already submitted job again."""
    run, plan, workflow, records = wrapper_host
    result = run(WRAPPER_FAIL_ACK="1")
    assert result.returncode != 0
    journal = workflow / ".hpcflow-host-submission.json"
    assert json.loads(journal.read_text())["state"] == "submitted"
    assert len(records("launch")) == 1
    blocked = run()
    assert blocked.returncode != 0
    assert len(records("launch")) == 1
    recovered = run(resume=True)
    assert recovered.returncode == 0, recovered.stderr
    assert len(records("launch")) == 1
    assert not journal.exists()
    plan["jobscripts"] = plan["jobscripts"][1:]
    plan["jobscripts"][0]["dependencies"][0]["reference"] = "101"
    continued = run()
    assert continued.returncode == 0, continued.stderr
    assert len(records("launch")) == 2
    assert records("launch")[1]["arguments"][0] == "afterany:101"
    assert records("ack")[-1]["scheduler_job_ID"] == "102"


@pytest.mark.parametrize(
    "overrides",
    [
        {"WRAPPER_FAIL_LAUNCH": "1"},
        {"WRAPPER_BAD_OUTPUT": "1"},
        {"WRAPPER_STDERR": "1"},
    ],
)
def test_wrapper_uncertain_launch_blocks_recovery(wrapper_host, overrides):
    """Keep uncertain scheduler launch outcomes blocked until manually reconciled."""
    run, _, workflow, records = wrapper_host
    result = run(**overrides)
    assert result.returncode != 0
    journal = json.loads((workflow / ".hpcflow-host-submission.json").read_text())
    assert journal["state"] == "launching"
    assert len(records("launch")) == 1
    assert not records("ack")
    recovered = run(resume=True)
    assert recovered.returncode != 0
    assert "uncertain" in recovered.stderr
    assert len(records("launch")) == 1


@pytest.mark.parametrize(
    "invalid",
    [
        "outside",
        "traversal",
        "dependency",
        "scheduler",
        "direct",
        "placeholder",
        "duplicate",
        "version",
        "boolean-version",
    ],
)
def test_wrapper_invalid_plan_never_launches(wrapper_host, invalid):
    """Validate the whole submission plan before executing its first host command."""
    run, plan, _, records = wrapper_host
    if invalid == "outside":
        plan["workflow_path"] = "/outside/workflow"
    elif invalid == "traversal":
        plan["jobscripts"][1]["path"] = "../bad"
    elif invalid == "dependency":
        plan["jobscripts"][0]["dependencies"] = plan["jobscripts"][1]["dependencies"]
    elif invalid in ("scheduler", "direct"):
        plan["jobscripts"][1]["scheduler"] = (
            "direct" if invalid == "direct" else "unsupported"
        )
    elif invalid == "placeholder":
        plan["jobscripts"][1]["submit_command"].append("__HPCFLOW_JOB_99_99__")
    elif invalid == "duplicate":
        plan["jobscripts"][1]["jobscript_index"] = 0
    elif invalid == "version":
        plan["schema_version"] = 2
    elif invalid == "boolean-version":
        plan["schema_version"] = True
    else:
        plan["jobscripts"][1]["scheduler"] = "unsupported"
    result = run()
    assert result.returncode != 0
    assert not records("launch")


def test_wrapper_existing_dependency(wrapper_host):
    """Reuse scheduler references for dependencies that were submitted previously."""
    run, plan, _, records = wrapper_host
    plan["jobscripts"] = plan["jobscripts"][1:]
    plan["jobscripts"][0]["dependencies"][0]["reference"] = "500"
    result = run()
    assert result.returncode == 0, result.stderr
    assert records("launch")[0]["arguments"][0] == "afterany:500"


def test_wrapper_invalid_ack_retains_result(wrapper_host):
    """Retain the durable launch result when the container returns an invalid acknowledgement."""
    run, _, workflow, records = wrapper_host
    result = run(WRAPPER_BAD_ACK="1")
    assert result.returncode != 0
    assert len(records("launch")) == 1, result.stderr
    assert (
        json.loads((workflow / ".hpcflow-host-submission.json").read_text())["state"]
        == "submitted"
    )


@pytest.mark.parametrize("option", ["--wait", "--cancel"])
def test_wrapper_rejects_wait_and_cancel(wrapper_host, option):
    """Reject unsupported synchronous waiting and cancellation in host submission mode."""
    run, _, _, records = wrapper_host
    result = run(["go", "template.yaml", option])
    assert result.returncode != 0
    assert not records("container")
    assert not records("launch")


def test_wrapper_rejects_abort(wrapper_host):
    run, _, _, records = wrapper_host
    result = run(["workflow", "0", "abort-run"])
    assert result.returncode != 0
    assert "Aborting container host runs is not supported" in result.stderr
    assert not records("container")
    assert not records("launch")


def test_wrapper_no_jobs(wrapper_host):
    """Handle an empty prepared plan without executing host submission commands."""
    run, plan, workflow, records = wrapper_host
    plan["jobscripts"] = []
    result = run()
    assert result.returncode == 0, result.stderr
    assert not records("launch")
    assert not (workflow / ".hpcflow-host-submission.json").exists()


def test_wrapper_failed_preparation_never_launches(wrapper_host):
    """Prevent host launch when container preparation exits unsuccessfully."""
    run, _, _, records = wrapper_host
    result = run(WRAPPER_FAIL_PREPARE="1")
    assert result.returncode != 0
    assert not records("launch")


def test_wrapper_global_prefix_never_passes_submission_through(wrapper_host):
    """Reject ambiguous global-prefix syntax instead of submitting inside the container."""
    run, _, _, records = wrapper_host
    result = run(["--config-key", "key", "go", "template.yaml"])
    assert result.returncode != 0
    assert "ConfigArgs" in result.stderr
    assert not records("container")


def test_wrapper_exported_default_image(wrapper_host, cli_runner, monkeypatch, tmp_path):
    """Execute the exported wrapper using its substituted default image."""
    from hpcflow.app import app as hf

    monkeypatch.setattr(hf.run_time_info, "container_image", "hpcflow:test")
    exported = cli_runner(["internal", "get-container-wrapper"])
    assert exported.exit_code == 0, exported.output
    script = tmp_path / "exported.ps1"
    script.write_text(exported.stdout, encoding="utf-8", newline="\n")
    run, _, _, records = wrapper_host
    result = run(wrapper=script, use_image=False)
    assert result.returncode == 0, result.stderr
    assert len(records("launch")) == 2


def test_wrapper_downstream_tokens(wrapper_host, tmp_path):
    """Use downstream application markers consistently in plans and wrapper commands."""
    run, plan, _, records = wrapper_host
    app = SimpleNamespace(
        package_name="matflow",
        run_time_info=SimpleNamespace(container_image="matflow:test"),
    )
    command = _make_internal_CLI(app).commands["get-container-wrapper"]
    exported = CliRunner().invoke(command)
    assert exported.exit_code == 0, exported.output
    assert "__HPCFLOW_" not in exported.stdout
    script = tmp_path / "matflow-wrapper.ps1"
    script.write_text(exported.stdout, encoding="utf-8", newline="\n")
    downstream_plan = json.loads(json.dumps(plan).replace("__HPCFLOW_", "__MATFLOW_"))
    plan.update(downstream_plan)
    result = run(wrapper=script, use_image=False)
    assert result.returncode == 0, result.stderr
    assert records("launch")[1]["arguments"][0] == "afterany:101"
    assert len(records("ack")) == 2


def test_wrapper_partial_failure_preserves_success(wrapper_host):
    """Retain earlier successful acknowledgements when a later host submission fails."""
    run, _, workflow, records = wrapper_host
    result = run(WRAPPER_FAIL_SECOND="1")
    assert result.returncode != 0
    assert len(records("launch")) == 2, result.stderr
    assert len(records("ack")) == 1
    assert records("ack")[0]["scheduler_job_ID"] == "101"
    journal = json.loads((workflow / ".hpcflow-host-submission.json").read_text())
    assert journal["result"]["jobscript_index"] == 1
    assert journal["state"] == "launching"


def test_wrapper_multiple_submissions_and_array_dependency(wrapper_host):
    """Substitute dependencies across submissions while retaining scheduler array syntax."""
    run, plan, _, records = wrapper_host
    plan["jobscripts"][1]["submission_index"] = 1
    plan["jobscripts"][0]["is_array"] = True
    plan["jobscripts"][1]["is_array"] = True
    plan["jobscripts"][1]["dependencies"][0]["is_array"] = True
    plan["jobscripts"][1]["submit_command"][4] = "aftercorr:__HPCFLOW_JOB_0_0__"
    result = run()
    assert result.returncode == 0, result.stderr
    assert records("launch")[1]["arguments"][0] == "aftercorr:101"
    assert [record["submission_index"] for record in records("ack")] == [0, 1]
