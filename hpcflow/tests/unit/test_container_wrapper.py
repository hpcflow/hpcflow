from __future__ import annotations

import json
import os
from pathlib import Path
import shutil
import subprocess
from types import SimpleNamespace

import pytest
from click.testing import CliRunner

from hpcflow.sdk.cli import _make_internal_CLI

WRAPPER = Path(__file__).parents[2] / "sdk" / "submission" / "container_wrapper.ps1"


def ps_quote(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


@pytest.fixture
def wrapper_host(tmp_path):
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
$words = @($args)
Add-Content -LiteralPath $env:WRAPPER_CONTAINER_LOG -Value (
    ConvertTo-Json -InputObject $words -Compress)
if ($words -contains 'record-host-submission') {
    $result = [Console]::In.ReadToEnd() | ConvertFrom-Json -AsHashtable
    Add-Content -LiteralPath $env:WRAPPER_ACK_LOG -Value (
        ConvertTo-Json -InputObject $result -Compress -Depth 30)
    if ($env:WRAPPER_FAIL_ACK -eq '1') { exit 8 }
    if ($env:WRAPPER_BAD_ACK -eq '1') { '{"recorded":"true"}'; exit 0 }
    '{"recorded":true}'
} elseif ($words -contains '--containerised') {
    if ($env:WRAPPER_FAIL_PREPARE -eq '1') { exit 9 }
    Get-Content -Raw -LiteralPath $env:WRAPPER_PLAN
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
        "WRAPPER_PLAN": str(plan_file),
        "WRAPPER_CONTAINER_LOG": str(root / "container.jsonl"),
        "WRAPPER_ACK_LOG": str(root / "ack.jsonl"),
        "WRAPPER_LAUNCH_LOG": str(root / "launch.jsonl"),
        "WRAPPER_PASS_EXIT": "0",
    }

    def run(args=None, resume=False, wrapper=WRAPPER, use_image=True, **overrides):
        if wrapper == WRAPPER:
            wrapper = rendered_wrapper
        plan_file.write_text(json.dumps(plan), encoding="utf-8", newline="\n")
        invocation = (
            f"& {ps_quote(wrapper)} "
            + ("-Image 'hpcflow:test' " if use_image else "")
            + "-Machine 'host-machine' "
            f"-ContainerCommand @({ps_quote(pwsh)}, '-NoProfile', '-File', "
            f"{ps_quote(container)}) "
        )
        if resume:
            invocation += "-ResumeResult " + ps_quote(
                workflow / ".hpcflow-host-submission.json"
            )
        else:
            invocation += (
                "-HpcflowArgs @("
                + ", ".join(ps_quote(arg) for arg in (args or ["go", "template.yaml"]))
                + ")"
            )
        return subprocess.run(
            [pwsh, "-NoProfile", "-Command", invocation + "; exit $LASTEXITCODE"],
            cwd=root,
            env={**env, **overrides},
            capture_output=True,
            text=True,
            timeout=30,
        )

    def records(name):
        path = root / f"{name}.jsonl"
        return (
            [json.loads(line) for line in path.read_text().splitlines()]
            if (path.exists())
            else []
        )

    return run, plan, workflow, records


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
    run, _, _, records = wrapper_host
    result = run(["--version"], WRAPPER_PASS_EXIT="3")
    assert result.returncode == 3
    assert result.stdout.strip() == "verbatim-output"
    assert not records("launch")
    assert records("container")[0][-1] == "--version"


def test_wrapper_ack_failure_and_recovery(wrapper_host):
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
    run, plan, _, records = wrapper_host
    plan["jobscripts"] = plan["jobscripts"][1:]
    plan["jobscripts"][0]["dependencies"][0]["reference"] = "500"
    result = run()
    assert result.returncode == 0, result.stderr
    assert records("launch")[0]["arguments"][0] == "afterany:500"


def test_wrapper_invalid_ack_retains_result(wrapper_host):
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
    run, _, _, records = wrapper_host
    result = run(["go", "template.yaml", option])
    assert result.returncode != 0
    assert not records("container")
    assert not records("launch")


def test_wrapper_no_jobs(wrapper_host):
    run, plan, workflow, records = wrapper_host
    plan["jobscripts"] = []
    result = run()
    assert result.returncode == 0, result.stderr
    assert not records("launch")
    assert not (workflow / ".hpcflow-host-submission.json").exists()


def test_wrapper_failed_preparation_never_launches(wrapper_host):
    run, _, _, records = wrapper_host
    result = run(WRAPPER_FAIL_PREPARE="1")
    assert result.returncode != 0
    assert not records("launch")


def test_wrapper_global_prefix_never_passes_submission_through(wrapper_host):
    run, _, _, records = wrapper_host
    result = run(["--config-key", "key", "go", "template.yaml"])
    assert result.returncode != 0
    assert "ConfigArgs" in result.stderr
    assert not records("container")


def test_wrapper_exported_default_image(wrapper_host, cli_runner, monkeypatch, tmp_path):
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
