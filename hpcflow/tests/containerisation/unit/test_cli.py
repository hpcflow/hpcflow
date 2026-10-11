import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from hpcflow.app import app as hf
from hpcflow.sdk.cli import (
    _make_install_CLI,
    _make_internal_CLI,
)


@pytest.fixture(params=["go", "demo-go", "submit"])
def submission_command(request):
    """Provide each CLI entry point that prepares a host submission plan."""
    if request.param == "go":
        return hf.cli.commands["go"], ["template.yaml"]
    if request.param == "demo-go":
        return hf.cli.commands["demo-workflow"].commands["go"], ["workflow_1"]
    return hf.cli.commands["workflow"].commands["submit"], []


def test_containerised_help(submission_command):
    """Advertise container preparation and JSON host plans on all submission commands."""
    command, _ = submission_command
    result = CliRunner().invoke(command, ["--help"])
    assert result.exit_code == 0
    assert "--containerised" in result.output
    assert "JSON host submission plan" in result.output


def test_containerised_plan_file(submission_command, monkeypatch, tmp_path):
    """Write submission plans to the configured exchange file without emitting JSON on stdout."""
    command, args = submission_command
    plan = {"schema_version": 1, "workflow_path": "caf\u00e9", "jobscripts": []}
    submit = Mock(return_value=plan if command.name == "submit" else (None, plan))
    workflow = hf.Workflow.__new__(hf.Workflow)
    if command.name == "submit":
        monkeypatch.setattr(hf.Workflow, "submit", submit)
    else:
        api_name = (
            "make_and_submit_workflow"
            if args == ["template.yaml"]
            else "make_and_submit_demo_workflow"
        )
        monkeypatch.setattr(type(hf), api_name, property(lambda self: submit))
    output = tmp_path / "plan with spaces.json"
    monkeypatch.setenv("HPCFLOW_CONTAINER_PLAN_FILE", str(output))
    result = CliRunner().invoke(command, [*args, "--containerised"], obj=workflow)
    assert result.exit_code == 0, result.output
    assert result.stdout == ""
    assert json.loads(output.read_text(encoding="utf-8")) == plan


@pytest.mark.parametrize(
    ("containerised", "modify_js"),
    [(False, False), (False, True), (True, False), (True, True)],
)
def test_containerised_passed_to_submission_api(
    submission_command, monkeypatch, containerised, modify_js
):
    """Forward container and jobscript-editing flags from each CLI submission command."""
    command, args = submission_command
    plan = {
        "schema_version": 1,
        "workflow_id": "workflow-id",
        "workflow_path": "workflow",
        "jobscripts": [],
    }
    submit = Mock(return_value=plan if command.name == "submit" else (None, plan))
    workflow = hf.Workflow.__new__(hf.Workflow)
    if command.name == "submit":
        monkeypatch.setattr(hf.Workflow, "submit", submit)
    else:
        api_name = (
            "make_and_submit_workflow"
            if args == ["template.yaml"]
            else "make_and_submit_demo_workflow"
        )
        monkeypatch.setattr(type(hf), api_name, property(lambda self: submit))
    result = CliRunner().invoke(
        command,
        [
            *args,
            *(["--containerised"] if containerised else []),
            *(["--modify-js"] if modify_js else []),
        ],
        obj=workflow,
    )
    assert result.exit_code == 0
    submit.assert_called_once()
    assert submit.call_args.kwargs["containerised"] is containerised
    assert submit.call_args.kwargs["modify_js"] is modify_js
    if containerised:
        assert json.loads(result.stdout) == plan


@pytest.mark.parametrize("command", ["go", "demo-go", "submit"])
@pytest.mark.parametrize("modify_js", [False, True])
def test_containerised_json_plan(tmp_path, cli_runner, monkeypatch, command, modify_js):
    """Prepare real jobscripts and emit valid JSON without launching them inside the container."""
    launch = Mock(side_effect=AssertionError("Container must not launch jobscripts"))
    monkeypatch.setattr(hf.Jobscript, "submit", launch)
    if command == "submit":
        workflow = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
        assert isinstance(workflow, hf.Workflow)
        args = ["workflow", str(workflow.path), "submit"]
        hf.unload_config()
    elif command == "demo-go":
        args = ["demo-workflow", "go", "workflow_1", "--path", str(tmp_path)]
    else:
        args = [
            "go",
            json.dumps(
                {
                    "name": "plan",
                    "tasks": [{"schema": "test_t1_conditional_OS", "inputs": {"p1": 1}}],
                }
            ),
            "--string",
            "--format",
            "json",
            "--path",
            str(tmp_path),
        ]
    result = cli_runner(
        [*args, "--containerised", *(["--modify-js"] if modify_js else [])],
        input="n\ny\n" if modify_js else None,
    )
    assert result.exit_code == 0, result.output
    plan = json.loads(result.stdout)
    assert plan["schema_version"] == 1
    assert plan["workflow_id"] == hf.Workflow(plan["workflow_path"]).id_
    assert len(plan["jobscripts"]) == 1
    workflow_path = Path(plan["workflow_path"])
    assert (workflow_path / plan["jobscripts"][0]["path"]).is_file()
    assert not hf.Workflow(workflow_path).submissions[0].submitted_jobscripts
    launch.assert_not_called()
    assert ("Ready to return the host submission plan?" in result.stderr) is modify_js


@pytest.mark.parametrize("option", ["--wait", "--cancel"])
def test_containerised_rejects_host_only_options(tmp_path, cli_runner, option):
    """Reject waiting or cancellation during container preparation before creating a workflow."""
    result = cli_runner(
        [
            "demo-workflow",
            "go",
            "workflow_1",
            "--path",
            str(tmp_path),
            "--containerised",
            option,
        ]
    )
    assert result.exit_code != 0
    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize("stdin", [False, True])
def test_record_host_submission_cli(tmp_path, cli_runner, stdin):
    """Record host acknowledgements from files or stdin and make identical retries idempotent."""
    workflow = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
    plan = workflow.submit(containerised=True, status=False)
    payload = json.dumps(
        {
            "schema_version": 1,
            "workflow_id": plan["workflow_id"],
            "submission_index": 0,
            "jobscript_index": 0,
            "process_ID": 12345,
            "submit_command": ["host-shell", "/host/workflow/script"],
            "submit_time": "2026-10-09T13:00:00+00:00",
            "submit_hostname": "host-login",
            "submit_machine": "host-cluster",
        }
    )
    result_file = tmp_path / "host-result.json"
    result_file.write_text(payload, encoding="utf-8", newline="\n")
    args = [
        "internal",
        "workflow",
        str(workflow.path),
        "record-host-submission",
        "-" if stdin else str(result_file),
        "--no-add-to-known",
    ]
    hf.unload_config()
    result = cli_runner(args, input=payload if stdin else None)
    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == {"recorded": True}
    assert hf.Workflow(workflow.path).submissions[0].submitted_jobscripts == (0,)
    hf.unload_config()
    retry = cli_runner(args, input=payload if stdin else None)
    assert retry.exit_code == 0, retry.output
    assert json.loads(retry.stdout) == {"recorded": False}


@pytest.mark.parametrize("payload", ["not-json", "[]", "{}"])
def test_record_host_submission_cli_invalid(tmp_path, cli_runner, payload):
    """Reject malformed acknowledgement payloads without marking jobscripts submitted."""
    workflow = hf.make_demo_workflow("workflow_1", path=tmp_path, status=False)
    workflow.submit(containerised=True, status=False)
    hf.unload_config()
    result = cli_runner(
        ["internal", "workflow", str(workflow.path), "record-host-submission", "-"],
        input=payload,
    )
    assert result.exit_code != 0
    assert not result.stdout
    assert "Error:" in result.stderr
    assert not hf.Workflow(workflow.path).submissions[0].submitted_jobscripts


def test_record_host_submission_is_internal():
    """Expose acknowledgement only through the private CLI and workflow API."""
    assert "record-host-submission" not in hf.cli.commands["workflow"].commands
    internal_workflow = hf.cli.commands["internal"].commands["workflow"]
    assert "record-host-submission" in internal_workflow.commands
    assert not hasattr(hf.Workflow, "record_host_submission")
    assert hasattr(hf.Workflow, "_record_host_submission")


@pytest.mark.parametrize("command", [["install"], ["internal", "get-container-wrapper"]])
@pytest.mark.parametrize("image", [None, "hpcflow:test"])
def test_get_container_wrapper(cli_runner, monkeypatch, image, command):
    """Export a wrapper with its image substituted or fail explicitly when no image is set."""
    monkeypatch.setattr(hf.run_time_info, "container_image", image)
    result = cli_runner(command)
    if image is None:
        assert result.exit_code != 0
        assert not result.stdout
        assert "HPCFLOW_CONTAINER" in result.stderr
    else:
        assert result.exit_code == 0, result.output
        assert "[string]$Image = 'hpcflow:test'" in result.stdout
        assert "__HPCFLOW_CONTAINER_IMAGE__" not in result.stdout
        assert "\r\n" not in result.stdout


@pytest.mark.parametrize("command", [["install"], ["internal", "get-container-wrapper"]])
def test_get_container_wrapper_image_override(cli_runner, monkeypatch, command):
    """Prefer an explicit export image over the container's default image marker."""
    monkeypatch.setattr(hf.run_time_info, "container_image", "default:test")
    result = cli_runner([*command, "--image", "override:test"])
    assert result.exit_code == 0, result.output
    assert "[string]$Image = 'override:test'" in result.stdout


def test_install_help(cli_runner):
    """Document the short install command, wrapper filename and image override."""
    result = cli_runner(["install", "--help"])
    assert result.exit_code == 0, result.output
    assert "hpcflow.ps1" in result.stdout
    assert "--image" in result.stdout


@pytest.mark.parametrize("image", ["bad\nimage", "bad\rimage", "bad\0image"])
def test_install_invalid_image(cli_runner, image):
    """Reject image strings containing line breaks or NUL bytes before wrapper export."""
    result = cli_runner(["install", "--image", image])
    assert result.exit_code != 0
    assert not result.stdout
    assert "Error:" in result.stderr


@pytest.mark.parametrize("package", ["hpcflow", "matflow"])
@pytest.mark.parametrize("runtime", ["docker", "apptainer", "singularity"])
def test_install_bash_wrapper(package, runtime):
    """Export a namespaced Bash wrapper with literal shell-safe image quoting."""
    app = SimpleNamespace(
        package_name=package,
        run_time_info=SimpleNamespace(container_image=None),
    )
    result = CliRunner().invoke(
        _make_install_CLI(app),
        ["--shell", "bash", "--runtime", runtime, "--image", "image'$(touch bad)"],
    )
    assert result.exit_code == 0, result.output
    assert result.stdout.startswith("#!/usr/bin/env bash\n")
    assert f"app='{package}'" in result.stdout
    assert "image='image'\"'\"'$(touch bad)'" in result.stdout
    assert f"runtime='{runtime}'" in result.stdout
    assert "__APP_RUNTIME__" not in result.stdout
    assert f"__{package.upper()}_JOBSCRIPT_PATH__" in result.stdout
    assert "__APP_NAME_" not in result.stdout
    assert "\r\n" not in result.stdout


@pytest.mark.parametrize("runtime", ["apptainer", "singularity"])
def test_install_non_docker_requires_bash(cli_runner, runtime):
    """Reject unsupported PowerShell runtimes instead of exporting a misleading wrapper."""
    result = cli_runner(["install", "--runtime", runtime, "--image", "/shared/image.sif"])
    assert result.exit_code != 0
    assert not result.stdout
    assert "--shell bash" in result.stderr


@pytest.mark.parametrize("public", [False, True])
def test_downstream_container_wrapper_environment_help(public):
    """Namespace exported wrapper tokens and help for downstream applications such as MatFlow."""
    app = SimpleNamespace(
        package_name="matflow",
        run_time_info=SimpleNamespace(container_image=None),
    )
    command = (
        _make_install_CLI(app)
        if public
        else _make_internal_CLI(app).commands["get-container-wrapper"]
    )
    runner = CliRunner()
    help_result = runner.invoke(command, ["--help"])
    assert help_result.exit_code == 0
    assert "MATFLOW_CONTAINER" in help_result.output
    assert "HPCFLOW_CONTAINER" not in help_result.output
    assert "matflow.ps1" in help_result.output
    missing_image = runner.invoke(command)
    assert missing_image.exit_code != 0
    assert "MATFLOW_CONTAINER" in missing_image.stderr
    assert "HPCFLOW_CONTAINER" not in missing_image.stderr
    exported = runner.invoke(command, ["--image", "matflow:test"])
    assert exported.exit_code == 0, exported.output
    assert "__MATFLOW_JOBSCRIPT_PATH__" in exported.stdout
    assert "__MATFLOW_JOB_" in exported.stdout
    assert "__HPCFLOW_" not in exported.stdout
    assert "__APP_NAME_" not in exported.stdout
    assert "__MATFLOW_CONTAINER_IMAGE__" not in exported.stdout
